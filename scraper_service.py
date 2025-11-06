# scraper_service.py
import asyncio
import logging
import os
import sqlite3
from datetime import datetime
from typing import List, Optional, Set

from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException
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
PAUSE_BETWEEN_CHUNKS = 2
REQUEST_INTERVAL_SECONDS = 1.0

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("scraper")

client = TelegramClient(SESSION_NAME, API_ID, API_HASH)

db_conn: Optional[sqlite3.Connection] = None
db_lock = asyncio.Lock()
scrape_lock = asyncio.Lock()


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


class ScrapeResponse(BaseModel):
    total: int
    members: List[Member]


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
    if not await client.is_user_authorized():
        raise RuntimeError("Userbot не авторизован. Сначала запусти scraper_login.py")


@app.on_event("shutdown")
async def on_shutdown():
    await client.disconnect()
    global db_conn
    if db_conn:
        db_conn.close()
        db_conn = None


@app.post("/scrape", response_model=ScrapeResponse)
async def scrape(req: ScrapeRequest):
    if db_conn is None:
        raise HTTPException(status_code=500, detail="Database is not initialised.")

    chat_value = (req.chat or TARGET_DEFAULT or "").strip()
    if not chat_value:
        raise HTTPException(status_code=400, detail="Chat reference is required.")

    async with scrape_lock:
        async with db_lock:
            existing_ids = await asyncio.to_thread(_fetch_existing_ids_sync, db_conn)

        logger.info(
            "Starting scrape for %s. Already have %d members.",
            chat_value,
            len(existing_ids),
        )

        iterator = client.iter_participants(chat_value, limit=None, aggressive=False)
        processed_in_chunk = 0
        newly_saved = 0

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
                raise HTTPException(status_code=400, detail=f"Telegram RPC error: {e}") from e
            except Exception as e:
                raise HTTPException(status_code=400, detail=f"Error: {e}") from e

            if user.id in existing_ids:
                await asyncio.sleep(REQUEST_INTERVAL_SECONDS)
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

            await asyncio.sleep(REQUEST_INTERVAL_SECONDS)

            if processed_in_chunk >= CHUNK_SIZE:
                logger.info(
                    "Collected %d new members so far (%d total stored).",
                    newly_saved,
                    len(existing_ids),
                )
                await asyncio.sleep(PAUSE_BETWEEN_CHUNKS)
                processed_in_chunk = 0

        if processed_in_chunk:
            logger.info("Collected %d new members so far (%d total stored).", newly_saved, len(existing_ids))

        async with db_lock:
            members = await asyncio.to_thread(_fetch_all_members_sync, db_conn)

        logger.info(
            "Scrape finished for %s. Total unique members stored: %d (newly added %d).",
            chat_value,
            len(members),
            newly_saved,
        )

    return ScrapeResponse(total=len(members), members=members)
