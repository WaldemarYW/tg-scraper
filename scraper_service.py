# scraper_service.py
import asyncio
import csv
import logging
import os
import sqlite3
import uuid
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Set

from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
from pydantic import BaseModel
from telethon import TelegramClient
from telethon.errors import FloodWaitError, RPCError

load_dotenv()

try:
    API_ID = int(os.getenv("API_ID"))
except (TypeError, ValueError) as exc:
    raise RuntimeError("API_ID is required and must be an integer") from exc
API_HASH = os.getenv("API_HASH")
if not API_HASH:
    raise RuntimeError("API_HASH is required")
SESSION_NAME = os.getenv("SESSION", "scraper_session")
TARGET_DEFAULT = os.getenv("TARGET")
DATABASE_PATH = os.getenv("DATABASE_PATH", "members.db")

CHUNK_SIZE = 100
PAUSE_BETWEEN_CHUNKS = 1.0
REQUEST_INTERVAL_SECONDS = 0.0
JOB_RETENTION_SECONDS = 3600
CSV_OUTPUT_DIR = os.getenv("CSV_OUTPUT_DIR", "exports")

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("scraper")

client = TelegramClient(SESSION_NAME, API_ID, API_HASH)

db_conn: Optional[sqlite3.Connection] = None
db_lock = asyncio.Lock()
scrape_lock = asyncio.Lock()
SCRAPE_JOBS: Dict[str, Dict[str, Any]] = {}
jobs_lock = asyncio.Lock()

os.makedirs(CSV_OUTPUT_DIR, exist_ok=True)


app = FastAPI(title="TG Scraper API")


class ScrapeRequest(BaseModel):
    chat: Optional[str]


class Member(BaseModel):
    id: int
    username: Optional[str]
    first_name: Optional[str]
    last_name: Optional[str]
    phone: Optional[str]
    added_at: str


class JobResponse(BaseModel):
    job_id: str
    status: str


class JobStatusResponse(BaseModel):
    job_id: str
    status: str
    total: int
    processed: int
    started_at: Optional[str] = None
    finished_at: Optional[str] = None
    error: Optional[str] = None
    csv_path: Optional[str] = None


def _current_iso() -> str:
    return datetime.utcnow().isoformat()


def _parse_iso(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value)
    except ValueError:
        return None


async def _update_job(job_id: str, **kwargs: Any) -> None:
    async with jobs_lock:
        job = SCRAPE_JOBS.get(job_id)
        if job is None:
            return
        for key, value in kwargs.items():
            job[key] = value


async def cleanup_finished_jobs() -> None:
    cutoff = datetime.utcnow() - timedelta(seconds=JOB_RETENTION_SECONDS)
    async with jobs_lock:
        stale_ids = []
        for job_id, job in SCRAPE_JOBS.items():
            if job.get("status") not in {"done", "error"}:
                continue
            finished_iso = job.get("finished_at")
            finished_dt = _parse_iso(finished_iso)
            if finished_dt and finished_dt < cutoff:
                stale_ids.append(job_id)
        for job_id in stale_ids:
            SCRAPE_JOBS.pop(job_id, None)


def _write_members_csv(members: List[Member], csv_path: str) -> None:
    with open(csv_path, "w", newline="", encoding="utf-8") as csv_file:
        writer = csv.writer(csv_file)
        writer.writerow(["id", "username", "first_name", "last_name", "phone", "added_at"])
        for member in members:
            writer.writerow(
                [
                    member.id,
                    member.username or "",
                    member.first_name or "",
                    member.last_name or "",
                    member.phone or "",
                    member.added_at,
                ]
            )


async def scrape_users(job_id: str, chat_value: str) -> None:
    processed_total = 0
    newly_saved = 0
    csv_path = os.path.join(CSV_OUTPUT_DIR, f"members_{job_id}.csv")

    try:
        async with scrape_lock:
            if db_conn is None:
                raise RuntimeError("Database is not initialised.")

            async with db_lock:
                existing_ids = await asyncio.to_thread(_fetch_existing_ids_sync, db_conn)

            await _update_job(job_id, total=len(existing_ids), processed=0)

            logger.info(
                "Starting scrape job %s for %s. Already have %d members.",
                job_id,
                chat_value,
                len(existing_ids),
            )

            iterator = client.iter_participants(chat_value, limit=None, aggressive=False)
            processed_in_chunk = 0

            while True:
                try:
                    user = await iterator.__anext__()
                except StopAsyncIteration:
                    break
                except FloodWaitError as e:
                    logger.warning("Flood wait for %s seconds; sleeping.", e.seconds)
                    await asyncio.sleep(e.seconds)
                    continue
                except RPCError as e:
                    raise RuntimeError(f"Telegram RPC error: {e}") from e
                except Exception as e:
                    raise RuntimeError(f"Error: {e}") from e

                processed_total += 1

                if user.id in existing_ids:
                    if REQUEST_INTERVAL_SECONDS > 0:
                        await asyncio.sleep(REQUEST_INTERVAL_SECONDS)
                    if processed_total % CHUNK_SIZE == 0:
                        await _update_job(
                            job_id,
                            processed=processed_total,
                            total=len(existing_ids),
                        )
                    continue

                member = Member(
                    id=user.id,
                    username=user.username,
                    first_name=user.first_name,
                    last_name=user.last_name,
                    phone=user.phone,
                    added_at=datetime.utcnow().isoformat(),
                )

                async with db_lock:
                    await asyncio.to_thread(_insert_member_sync, db_conn, member)

                existing_ids.add(user.id)
                newly_saved += 1
                processed_in_chunk += 1

                if REQUEST_INTERVAL_SECONDS > 0:
                    await asyncio.sleep(REQUEST_INTERVAL_SECONDS)

                if processed_total % CHUNK_SIZE == 0:
                    await _update_job(
                        job_id,
                        processed=processed_total,
                        total=len(existing_ids),
                    )

                if processed_in_chunk >= CHUNK_SIZE:
                    logger.info(
                        "Collected %d new members so far (%d total stored).",
                        newly_saved,
                        len(existing_ids),
                    )
                    await _update_job(
                        job_id,
                        processed=processed_total,
                        total=len(existing_ids),
                    )
                    await asyncio.sleep(PAUSE_BETWEEN_CHUNKS)
                    processed_in_chunk = 0

            if processed_in_chunk:
                logger.info(
                    "Collected %d new members so far (%d total stored).",
                    newly_saved,
                    len(existing_ids),
                )

            async with db_lock:
                members = await asyncio.to_thread(_fetch_all_members_sync, db_conn)

            await asyncio.to_thread(_write_members_csv, members, csv_path)

            logger.info(
                "Scrape finished for %s. Total unique members stored: %d (newly added %d).",
                chat_value,
                len(members),
                newly_saved,
            )

            await _update_job(
                job_id,
                status="done",
                total=len(members),
                processed=processed_total,
                finished_at=_current_iso(),
                error=None,
                csv_path=csv_path,
            )
    except Exception as exc:
        total_snapshot = len(existing_ids) if "existing_ids" in locals() else 0
        await _update_job(
            job_id,
            status="error",
            error=str(exc),
            finished_at=_current_iso(),
            processed=processed_total,
            total=total_snapshot,
            csv_path=csv_path if os.path.exists(csv_path) else None,
        )
        logger.exception("Scrape job %s failed: %s", job_id, exc)


def init_db() -> sqlite3.Connection:
    conn = sqlite3.connect(DATABASE_PATH, check_same_thread=False)
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS members (
            id INTEGER PRIMARY KEY,
            username TEXT,
            first_name TEXT,
            last_name TEXT,
            phone TEXT,
            added_at TEXT NOT NULL
        )
        """
    )
    conn.commit()
    return conn


def _fetch_existing_ids_sync(conn: sqlite3.Connection) -> Set[int]:
    cursor = conn.execute("SELECT id FROM members")
    return {row[0] for row in cursor.fetchall()}


def _insert_member_sync(conn: sqlite3.Connection, member: Member) -> None:
    conn.execute(
        """
        INSERT INTO members (id, username, first_name, last_name, phone, added_at)
        VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT(id) DO UPDATE SET
            username = excluded.username,
            first_name = excluded.first_name,
            last_name = excluded.last_name,
            phone = excluded.phone
        """,
        (
            member.id,
            member.username,
            member.first_name,
            member.last_name,
            member.phone,
            member.added_at,
        ),
    )
    conn.commit()


def _fetch_all_members_sync(conn: sqlite3.Connection) -> List[Member]:
    cursor = conn.execute(
        """
        SELECT id, username, first_name, last_name, phone, added_at
        FROM members
        ORDER BY added_at ASC
        """
    )
    rows = cursor.fetchall()
    return [
        Member(
            id=row[0],
            username=row[1],
            first_name=row[2],
            last_name=row[3],
            phone=row[4],
            added_at=row[5],
        )
        for row in rows
    ]


@app.on_event("startup")
async def on_startup():
    global db_conn
    db_conn = init_db()
    await client.connect()
    await cleanup_finished_jobs()
    if not await client.is_user_authorized():
        raise RuntimeError("Userbot не авторизован. Сначала запусти scraper_login.py")


@app.on_event("shutdown")
async def on_shutdown():
    await client.disconnect()
    global db_conn
    if db_conn:
        db_conn.close()
        db_conn = None


@app.post("/scrape", response_model=JobResponse, status_code=202)
async def scrape(req: ScrapeRequest):
    if db_conn is None:
        raise HTTPException(status_code=500, detail="Database is not initialised.")

    chat_value = (req.chat or TARGET_DEFAULT or "").strip()
    if not chat_value:
        raise HTTPException(status_code=400, detail="Chat reference is required.")

    job_id = str(uuid.uuid4())

    await cleanup_finished_jobs()

    async with jobs_lock:
        SCRAPE_JOBS[job_id] = {
            "status": "running",
            "total": 0,
            "processed": 0,
            "started_at": _current_iso(),
            "finished_at": None,
            "error": None,
            "csv_path": None,
        }

    asyncio.create_task(scrape_users(job_id, chat_value))
    logger.info("Queued scrape job %s for %s", job_id, chat_value)

    return JobResponse(job_id=job_id, status="started")


@app.get("/scrape_status", response_model=JobStatusResponse)
async def scrape_status(job_id: str):
    await cleanup_finished_jobs()

    async with jobs_lock:
        job = SCRAPE_JOBS.get(job_id)

    if job is None:
        raise HTTPException(status_code=404, detail="Job not found")

    return JobStatusResponse(job_id=job_id, **job)


@app.get("/scrape_result")
async def scrape_result(job_id: str):
    async with jobs_lock:
        job = SCRAPE_JOBS.get(job_id)

    if job is None:
        raise HTTPException(status_code=404, detail="Job not found")

    if job.get("status") != "done":
        raise HTTPException(status_code=409, detail="Job not finished")

    csv_path = job.get("csv_path")
    if not csv_path or not os.path.exists(csv_path):
        raise HTTPException(status_code=500, detail="CSV file is not available")

    filename = os.path.basename(csv_path)
    return FileResponse(csv_path, media_type="text/csv", filename=filename)
