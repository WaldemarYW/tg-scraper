# scraper_service.py
import asyncio
import csv
import logging
import os
import re
import sqlite3
import uuid
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Set
import re
import tempfile

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
FILENAME_SANITIZE_RE = re.compile(r"[^A-Za-z0-9._-]+")
FULL_EXPORT_NAME = "members_full.csv"

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("scraper")

client = TelegramClient(SESSION_NAME, API_ID, API_HASH)

db_conn: Optional[sqlite3.Connection] = None
db_lock = asyncio.Lock()
scrape_lock = asyncio.Lock()
SCRAPE_JOBS: Dict[str, Dict[str, Any]] = {}
jobs_lock = asyncio.Lock()
broadcast_lock = asyncio.Lock()
BROADCAST_JOBS: Dict[str, Dict[str, Any]] = {}
current_broadcast_job_id: Optional[str] = None

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
    last_broadcast_at: Optional[str] = None
    last_broadcast_status: Optional[str] = None
    is_hr: bool = False
    source_chat: Optional[str] = None


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
    chat_title: Optional[str] = None


class CSVExport(BaseModel):
    filename: str
    job_id: Optional[str]
    created_at: Optional[str]
    url: str


class BroadcastRequest(BaseModel):
    text: str
    limit: Optional[int] = None
    interval_seconds: float = 0.0
    source_chat: Optional[str] = None
    chat_title: Optional[str] = None


class BroadcastJobResponse(BaseModel):
    job_id: str
    status: str
    source_chat: Optional[str] = None


class BroadcastStatusResponse(BaseModel):
    job_id: str
    status: str
    total: int
    processed: int
    sent_success: int
    sent_failed: int
    started_at: Optional[str] = None
    finished_at: Optional[str] = None
    cancel_requested: bool
    message: Optional[str] = None
    source_chat: Optional[str] = None
    chat_title: Optional[str] = None


class BroadcastLogEntry(BaseModel):
    member_id: int
    username: Optional[str]
    status: str
    timestamp: str


class BroadcastLogResponse(BaseModel):
    job_id: str
    entries: List[BroadcastLogEntry]
    total: int
    next_offset: Optional[int]
    has_more: bool
    source_chat: Optional[str] = None


class BroadcastStatsEntry(BaseModel):
    date: str
    processed: int


def _current_iso() -> str:
    return datetime.utcnow().isoformat()


def _parse_iso(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value)
    except ValueError:
        return None


def _ensure_member_columns(conn: sqlite3.Connection) -> None:
    cursor = conn.execute("PRAGMA table_info(members)")
    existing = {row[1] for row in cursor.fetchall()}
    if "last_broadcast_at" not in existing:
        conn.execute("ALTER TABLE members ADD COLUMN last_broadcast_at TEXT")
    if "last_broadcast_status" not in existing:
        conn.execute("ALTER TABLE members ADD COLUMN last_broadcast_status TEXT")
    if "is_hr" not in existing:
        conn.execute("ALTER TABLE members ADD COLUMN is_hr INTEGER DEFAULT 0")
        conn.execute(
            """
            UPDATE members
            SET is_hr = CASE
                WHEN LOWER(IFNULL(username, '')) LIKE '%hr%'
                  OR LOWER(IFNULL(first_name, '')) LIKE '%hr%'
                  OR LOWER(IFNULL(last_name, '')) LIKE '%hr%'
                THEN 1 ELSE 0 END
            """
        )
    if "source_chat" not in existing:
        conn.execute("ALTER TABLE members ADD COLUMN source_chat TEXT")
    conn.commit()


def _ensure_broadcast_history_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS broadcast_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            job_id TEXT NOT NULL,
            member_id INTEGER NOT NULL,
            username TEXT,
            status TEXT NOT NULL,
            timestamp TEXT NOT NULL
        )
        """
    )
    conn.commit()


async def _update_job(job_id: str, **kwargs: Any) -> None:
    async with jobs_lock:
        job = SCRAPE_JOBS.get(job_id)
        if job is None:
            return
        for key, value in kwargs.items():
            job[key] = value


async def _update_broadcast_job(job_id: str, **kwargs: Any) -> None:
    async with broadcast_lock:
        job = BROADCAST_JOBS.get(job_id)
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


def _safe_filename_component(value: str) -> str:
    cleaned = FILENAME_SANITIZE_RE.sub("_", value.strip())
    cleaned = cleaned.strip("._-")
    return cleaned[:80]


def _derive_chat_title(entity: Any, fallback: str) -> str:
    for attr in ("title", "username", "first_name"):
        val = getattr(entity, attr, None)
        if isinstance(val, str) and val.strip():
            return val.strip()
    if isinstance(entity, dict):
        for key in ("title", "username", "first_name"):
            val = entity.get(key)
            if isinstance(val, str) and val.strip():
                return val.strip()
    return fallback


def _fallback_from_chat_value(value: str) -> str:
    if not value:
        return ""
    stripped = value.strip()
    if stripped.startswith("@"):
        stripped = stripped[1:]
    if "/" in stripped:
        stripped = stripped.rstrip("/").rsplit("/", 1)[-1]
    return stripped


def _is_hr_candidate(*values: Optional[str]) -> bool:
    for value in values:
        if value and "hr" in value.lower():
            return True
    return False


def _list_csv_exports() -> List[CSVExport]:
    exports: List[CSVExport] = []
    base_path = os.path.abspath(CSV_OUTPUT_DIR)

    try:
        entries = list(os.scandir(base_path))
    except FileNotFoundError:
        return exports

    entries = [entry for entry in entries if entry.is_file() and entry.name.endswith(".csv")]
    entries.sort(key=lambda e: e.stat().st_mtime, reverse=True)

    job_map = {}
    for job_id, job in SCRAPE_JOBS.items():
        csv_path = job.get("csv_path")
        if csv_path:
            job_map[os.path.abspath(csv_path)] = job_id

    for entry in entries:
        file_path = os.path.abspath(entry.path)
        job_id = job_map.get(file_path)
        created_at = datetime.utcfromtimestamp(entry.stat().st_mtime).isoformat()
        exports.append(
            CSVExport(
                filename=entry.name,
                job_id=job_id,
                created_at=created_at,
                url=f"/scrape_export/{entry.name}",
            )
        )
    return exports


def _clear_csv_exports() -> int:
    if not os.path.isdir(CSV_OUTPUT_DIR):
        return 0
    removed = 0
    for entry in os.scandir(CSV_OUTPUT_DIR):
        if entry.is_file() and entry.name.endswith(".csv"):
            try:
                os.remove(entry.path)
                removed += 1
            except FileNotFoundError:
                continue
    return removed




def _fetch_pending_broadcast_members_sync(
    conn: sqlite3.Connection, limit: Optional[int], source_chat: Optional[str]
) -> List[Member]:
    query = """
        SELECT id, username, first_name, last_name, phone, added_at, last_broadcast_at, last_broadcast_status, IFNULL(is_hr, 0), source_chat
        FROM members
        WHERE last_broadcast_at IS NULL AND IFNULL(is_hr, 0) = 0
    """
    params: List[Any] = []
    if source_chat:
        query += " AND source_chat = ?"
        params.append(source_chat)

    query += " ORDER BY added_at ASC"
    if limit is not None and limit > 0:
        query += f" LIMIT {int(limit)}"
    cursor = conn.execute(query, tuple(params))
    rows = cursor.fetchall()
    return [
        Member(
            id=row[0],
            username=row[1],
            first_name=row[2],
            last_name=row[3],
            phone=row[4],
            added_at=row[5],
            last_broadcast_at=row[6],
            last_broadcast_status=row[7],
            is_hr=bool(row[8]),
            source_chat=row[9],
        )
        for row in rows
    ]


def _mark_member_broadcast_status_sync(
    conn: sqlite3.Connection, member_id: int, timestamp: str, status: str
) -> None:
    conn.execute(
        """
        UPDATE members
        SET last_broadcast_at = ?, last_broadcast_status = ?
        WHERE id = ?
        """,
        (timestamp, status, member_id),
    )
    conn.commit()


def _insert_broadcast_log_sync(
    conn: sqlite3.Connection,
    job_id: str,
    member: Member,
    status: str,
    timestamp: str,
) -> None:
    conn.execute(
        """
        INSERT INTO broadcast_history (job_id, member_id, username, status, timestamp)
        VALUES (?, ?, ?, ?, ?)
        """,
        (job_id, member.id, member.username, status, timestamp),
    )
    conn.commit()


def _fetch_broadcast_logs_sync(
    conn: sqlite3.Connection, job_id: str, offset: int, limit: int
) -> Dict[str, Any]:
    cursor = conn.execute(
        """
        SELECT id, member_id, username, status, timestamp
        FROM broadcast_history
        WHERE job_id = ?
        ORDER BY id DESC
        LIMIT ? OFFSET ?
        """,
        (job_id, limit, offset),
    )
    rows = cursor.fetchall()
    total_cursor = conn.execute(
        "SELECT COUNT(*) FROM broadcast_history WHERE job_id = ?", (job_id,)
    )
    total = total_cursor.fetchone()[0]
    entries = [
        {
            "member_id": row[1],
            "username": row[2],
            "status": row[3],
            "timestamp": row[4],
        }
        for row in rows
    ]
    return {"entries": entries, "total": total}


def _fetch_broadcast_stats_sync(conn: sqlite3.Connection, limit: Optional[int]) -> List[Dict[str, Any]]:
    query = """
        SELECT substr(timestamp, 1, 10) as day, COUNT(*) as processed
        FROM broadcast_history
        GROUP BY day
        ORDER BY day DESC
    """
    params: tuple = ()
    if limit and limit > 0:
        query += " LIMIT ?"
        params = (limit,)
    cursor = conn.execute(query, params)
    rows = cursor.fetchall()
    return [{"date": row[0], "processed": row[1]} for row in rows]


async def _write_full_export() -> str:
    if db_conn is None:
        raise RuntimeError("Database is not initialised.")
    async with db_lock:
        members = await asyncio.to_thread(_fetch_all_members_sync, db_conn)
    csv_path = os.path.join(CSV_OUTPUT_DIR, FULL_EXPORT_NAME)
    await asyncio.to_thread(_write_members_csv, members, csv_path)
    return csv_path


async def scrape_users(job_id: str, chat_value: str) -> None:
    processed_total = 0
    newly_saved = 0
    csv_path = ""
    chat_title = chat_value
    job_members: List[Member] = []

    try:
        async with scrape_lock:
            if db_conn is None:
                raise RuntimeError("Database is not initialised.")

            async with db_lock:
                existing_ids = await asyncio.to_thread(_fetch_existing_ids_sync, db_conn)

            await _update_job(job_id, total=0, processed=0)

            entity = await client.get_entity(chat_value)
            chat_title = _derive_chat_title(entity, chat_value)
            fallback_name = _fallback_from_chat_value(chat_value)
            safe_title = _safe_filename_component(chat_title) or _safe_filename_component(fallback_name) or f"job_{job_id}"
            csv_filename = f"members_{safe_title}.csv"
            csv_path = os.path.join(CSV_OUTPUT_DIR, csv_filename)
            await _update_job(job_id, chat_title=chat_title)

            logger.info(
                "Starting scrape job %s for %s. Already have %d members.",
                job_id,
                chat_title,
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
                is_new = user.id not in existing_ids
                member = Member(
                    id=user.id,
                    username=user.username,
                    first_name=user.first_name,
                    last_name=user.last_name,
                    phone=user.phone,
                    added_at=datetime.utcnow().isoformat(),
                    is_hr=_is_hr_candidate(user.username, user.first_name, user.last_name),
                )

                if is_new:
                    job_members.append(member)
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
                        total=len(job_members),
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
                        total=len(job_members),
                    )
                    await asyncio.sleep(PAUSE_BETWEEN_CHUNKS)
                    processed_in_chunk = 0

            if processed_in_chunk:
                logger.info(
                    "Collected %d new members so far (%d total stored).",
                    newly_saved,
                    len(existing_ids),
                )

            await asyncio.to_thread(_write_members_csv, job_members, csv_path)

            logger.info(
                "Scrape finished for %s. Added %d new members in this run (processed %d records).",
                chat_title,
                len(job_members),
                processed_total,
            )

            await _update_job(
                job_id,
                status="done",
                total=len(job_members),
                processed=processed_total,
                finished_at=_current_iso(),
                error=None,
                csv_path=csv_path,
            )
    except Exception as exc:
        await _update_job(
            job_id,
            status="error",
            error=str(exc),
            finished_at=_current_iso(),
            processed=processed_total,
            total=len(job_members),
            csv_path=csv_path if os.path.exists(csv_path) else None,
        )
        logger.exception("Scrape job %s failed: %s", job_id, exc)


async def broadcast_users(job_id: str, text: str, interval: float, recipients: List[Member]) -> None:
    global current_broadcast_job_id
    job = BROADCAST_JOBS[job_id]
    processed = 0
    sent_success = 0
    sent_failed = 0

    try:
        for member in recipients:
            if job.get("cancel_requested"):
                break

            if member.is_hr:
                processed += 1
                await _update_broadcast_job(
                    job_id,
                    processed=processed,
                    last_member_id=member.id,
                    last_member_status="skipped_hr",
                )
                continue

            target = member.username or member.id
            status = "skipped"

            while True:
                try:
                    await client.send_message(target, text)
                    sent_success += 1
                    status = "sent"
                    break
                except FloodWaitError as e:
                    await asyncio.sleep(e.seconds + 1)
                    continue
                except RPCError as e:
                    sent_failed += 1
                    status = f"rpc_error:{e.__class__.__name__}"
                    break
                except Exception as e:
                    sent_failed += 1
                    status = f"error:{e}"
                    break

            processed += 1
            timestamp = _current_iso()
            async with db_lock:
                await asyncio.to_thread(
                    _mark_member_broadcast_status_sync,
                    db_conn,
                    member.id,
                    timestamp,
                    status,
                )
                await asyncio.to_thread(
                    _insert_broadcast_log_sync,
                    db_conn,
                    job_id,
                    member,
                    status,
                    timestamp,
                )

            await _update_broadcast_job(
                job_id,
                processed=processed,
                sent_success=sent_success,
                sent_failed=sent_failed,
                last_member_id=member.id,
                last_member_status=status,
            )

            if job.get("cancel_requested"):
                break

            if interval > 0:
                await asyncio.sleep(interval)

        status_value = "cancelled" if job.get("cancel_requested") else "done"
        await _update_broadcast_job(
            job_id,
            status=status_value,
            finished_at=_current_iso(),
            processed=processed,
            sent_success=sent_success,
            sent_failed=sent_failed,
        )
    except Exception as exc:
        await _update_broadcast_job(
            job_id,
            status="error",
            finished_at=_current_iso(),
            message=str(exc),
        )
        logger.exception("Broadcast job %s failed: %s", job_id, exc)
    finally:
        async with broadcast_lock:
            if current_broadcast_job_id == job_id:
                current_broadcast_job_id = None


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
            added_at TEXT NOT NULL,
            last_broadcast_at TEXT,
            last_broadcast_status TEXT,
            is_hr INTEGER DEFAULT 0,
            source_chat TEXT
        )
        """
    )
    conn.commit()
    _ensure_member_columns(conn)
    _ensure_broadcast_history_table(conn)
    return conn


def _fetch_existing_ids_sync(conn: sqlite3.Connection) -> Set[int]:
    cursor = conn.execute("SELECT id FROM members")
    return {row[0] for row in cursor.fetchall()}


def _insert_member_sync(conn: sqlite3.Connection, member: Member) -> None:
    conn.execute(
        """
        INSERT INTO members (
            id,
            username,
            first_name,
            last_name,
            phone,
            added_at,
            last_broadcast_at,
            last_broadcast_status,
            is_hr,
            source_chat
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(id) DO UPDATE SET
            username = excluded.username,
            first_name = excluded.first_name,
            last_name = excluded.last_name,
            phone = excluded.phone,
            is_hr = excluded.is_hr,
            source_chat = COALESCE(excluded.source_chat, members.source_chat)
        """,
        (
            member.id,
            member.username,
            member.first_name,
            member.last_name,
            member.phone,
            member.added_at,
            member.last_broadcast_at,
            member.last_broadcast_status,
            int(member.is_hr),
            member.source_chat,
        ),
    )
    conn.commit()


def _fetch_all_members_sync(conn: sqlite3.Connection) -> List[Member]:
    cursor = conn.execute(
        """
        SELECT id, username, first_name, last_name, phone, added_at, last_broadcast_at, last_broadcast_status, IFNULL(is_hr, 0), source_chat
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
            last_broadcast_at=row[6],
            last_broadcast_status=row[7],
            is_hr=bool(row[8]),
            source_chat=row[9],
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
            "chat_title": None,
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


@app.post("/send_start", response_model=BroadcastJobResponse, status_code=202)
async def send_start(req: BroadcastRequest):
    global current_broadcast_job_id
    if db_conn is None:
        raise HTTPException(status_code=500, detail="Database is not initialised.")

    text = req.text.strip()
    if not text:
        raise HTTPException(status_code=400, detail="Broadcast text is required.")

    limit = req.limit if req.limit and req.limit > 0 else None
    interval = max(0.0, req.interval_seconds or 0.0)
    source_chat = (req.source_chat or "").strip() or None

    async with db_lock:
        recipients = await asyncio.to_thread(
            _fetch_pending_broadcast_members_sync,
            db_conn,
            limit,
            source_chat,
        )

    if not recipients:
        raise HTTPException(status_code=400, detail="No recipients available for broadcast.")

    job_id = str(uuid.uuid4())

    async with broadcast_lock:
        if current_broadcast_job_id:
            previous_job = BROADCAST_JOBS.get(current_broadcast_job_id)
            if previous_job and previous_job.get("status") == "running":
                previous_job["cancel_requested"] = True
                previous_job["message"] = "Superseded by a new broadcast."
        BROADCAST_JOBS[job_id] = {
            "status": "running",
            "text": text,
            "total": len(recipients),
            "processed": 0,
            "sent_success": 0,
            "sent_failed": 0,
            "limit": limit,
            "interval": interval,
            "source_chat": source_chat,
            "chat_title": req.chat_title,
            "started_at": _current_iso(),
            "finished_at": None,
            "cancel_requested": False,
            "message": None,
        }
        current_broadcast_job_id = job_id

    task = asyncio.create_task(broadcast_users(job_id, text, interval, recipients))
    async with broadcast_lock:
        BROADCAST_JOBS[job_id]["task"] = task

    return BroadcastJobResponse(job_id=job_id, status="started")


@app.post("/send_stop")
async def send_stop(job_id: str):
    async with broadcast_lock:
        job = BROADCAST_JOBS.get(job_id)
        if job is None:
            raise HTTPException(status_code=404, detail="Broadcast job not found.")
        if job.get("status") != "running":
            return {"status": job.get("status"), "message": "Job is not running."}
        job["cancel_requested"] = True
        job["message"] = "Cancellation requested by user."
    return {"status": "cancelling"}


@app.get("/send_status", response_model=BroadcastStatusResponse)
async def send_status(job_id: str):
    async with broadcast_lock:
        job = BROADCAST_JOBS.get(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail="Broadcast job not found.")
    return BroadcastStatusResponse(
        job_id=job_id,
        status=job.get("status"),
        total=job.get("total", 0),
        processed=job.get("processed", 0),
        sent_success=job.get("sent_success", 0),
        sent_failed=job.get("sent_failed", 0),
        started_at=job.get("started_at"),
        finished_at=job.get("finished_at"),
        cancel_requested=job.get("cancel_requested", False),
        message=job.get("message"),
        source_chat=job.get("source_chat"),
        chat_title=job.get("chat_title"),
    )


@app.get("/send_log", response_model=BroadcastLogResponse)
async def send_log(job_id: str, offset: int = 0, limit: int = 10):
    if db_conn is None:
        raise HTTPException(status_code=500, detail="Database is not initialised.")

    if limit <= 0 or limit > 100:
        raise HTTPException(status_code=400, detail="Limit must be between 1 and 100.")

    async with db_lock:
        result = await asyncio.to_thread(
            _fetch_broadcast_logs_sync,
            db_conn,
            job_id,
            offset,
            limit,
        )

    entries = [
        BroadcastLogEntry(
            member_id=entry["member_id"],
            username=entry["username"],
            status=entry["status"],
            timestamp=entry["timestamp"],
        )
        for entry in result["entries"]
    ]
    total = result["total"]
    next_offset = offset + len(entries) if entries else None
    has_more = next_offset is not None and next_offset < total

    return BroadcastLogResponse(
        job_id=job_id,
        entries=entries,
        total=total,
        next_offset=next_offset if has_more else None,
        has_more=has_more,
        source_chat=BROADCAST_JOBS.get(job_id, {}).get("source_chat"),
    )


@app.get("/broadcast_stats", response_model=List[BroadcastStatsEntry])
async def broadcast_stats(limit: int = 30):
    if db_conn is None:
        raise HTTPException(status_code=500, detail="Database is not initialised.")
    if limit <= 0:
        limit = 30

    async with db_lock:
        rows = await asyncio.to_thread(
            _fetch_broadcast_stats_sync,
            db_conn,
            limit,
        )

    return [
        BroadcastStatsEntry(date=row["date"], processed=row["processed"])
        for row in rows
    ]


@app.get("/scrape_exports", response_model=List[CSVExport])
async def scrape_exports():
    return _list_csv_exports()


def _resolve_csv_path(filename: str) -> str:
    if "/" in filename or "\\" in filename or ".." in filename:
        raise HTTPException(status_code=400, detail="Invalid filename")
    abs_dir = os.path.abspath(CSV_OUTPUT_DIR)
    abs_path = os.path.abspath(os.path.join(CSV_OUTPUT_DIR, filename))
    if not abs_path.startswith(abs_dir + os.sep) and abs_path != abs_dir:
        raise HTTPException(status_code=400, detail="Invalid filename")
    return abs_path


@app.post("/scrape_exports/clear")
async def scrape_exports_clear():
    deleted = await asyncio.to_thread(_clear_csv_exports)

    async with jobs_lock:
        for job in SCRAPE_JOBS.values():
            csv_path = job.get("csv_path")
            if csv_path and not os.path.exists(csv_path):
                job["csv_path"] = None

    return {"deleted": deleted}


@app.get("/scrape_export/full")
async def scrape_export_full():
    path = await _write_full_export()
    filename = os.path.basename(path)
    return FileResponse(path, media_type="text/csv", filename=filename)


@app.get("/scrape_export/{filename}")
async def scrape_export(filename: str):
    path = _resolve_csv_path(filename)
    if not os.path.exists(path):
        raise HTTPException(status_code=404, detail="File not found")
    return FileResponse(path, media_type="text/csv", filename=filename)


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
