# scraper_service.py
import asyncio
import csv
import json
import logging
import os
import random
import re
import sqlite3
import uuid
from datetime import datetime, timedelta, timezone, time
from typing import Any, Dict, List, Optional, Set, Tuple
from zoneinfo import ZoneInfo

import requests

from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
from pydantic import BaseModel
from telethon import TelegramClient
from telethon.errors import FloodWaitError, RPCError
from telethon.tl import functions, types
from telethon.tl.types import InputPeerChannel, InputPeerChat

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

PROMO_SLOTS = ("morning", "noon", "evening")
PROMO_DEFAULT_SCHEDULE = {
    "morning": {"hour": 9, "minute": 0},
    "noon": {"hour": 13, "minute": 0},
    "evening": {"hour": 18, "minute": 0},
}
PROMO_MIN_DELAY_SECONDS = float(os.getenv("PROMO_MIN_DELAY", 6))
PROMO_MAX_DELAY_SECONDS = float(os.getenv("PROMO_MAX_DELAY", 18))
if PROMO_MIN_DELAY_SECONDS < 0:
    PROMO_MIN_DELAY_SECONDS = 0.0
if PROMO_MAX_DELAY_SECONDS < PROMO_MIN_DELAY_SECONDS:
    PROMO_MAX_DELAY_SECONDS = PROMO_MIN_DELAY_SECONDS + 1.0
PROMO_FOLDER_NAME = os.getenv("PROMO_FOLDER_NAME", "Бесплатно PR").strip()
PROMO_GROUP_SYNC_INTERVAL_SECONDS = int(os.getenv("PROMO_GROUP_SYNC_INTERVAL", 300))
KYIV_TZ = ZoneInfo("Europe/Kyiv")
UTC_TZ = timezone.utc
CHAT_DIALOG_PAGE_SIZE = int(os.getenv("CHAT_DIALOG_PAGE_SIZE", 20))
CHAT_MESSAGE_PAGE_SIZE = int(os.getenv("CHAT_MESSAGE_PAGE_SIZE", 20))
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
OPENAI_DIALOG_MODEL = os.getenv("OPENAI_DIALOG_MODEL", "gpt-4o-mini-2024-07-18")
DIALOG_AI_URL = os.getenv("DIALOG_AI_URL")
HR_ASSISTANT_PROMPT = (
    "Ты – віртуальний HR-асистент компанії Furioza, яка працює в сфері міжнародних онлайн-знайомств (дейтинг). "
    "Твоє завдання – вести кандидатів по воронці від першого запиту до заповнення анкети. Працюй українською мовою, "
    "пиши коротко, просто і по-людськи, ніби живий HR у Telegram. Дотримуйся наступного сценарію:\n\n"
    "1. Загальні правила: відповідай стисло, по суті, без води; кілька коротких повідомлень краще довгих; тон доброзичливий, "
    "але без сюсюкання; м’яко веди до наступного кроку (графік → навчання → анкета); якщо кандидат мовчить, "
    "роби м’які фоллоу-апи.\n"
    "2. Старт діалогу: привітайся, представся як Володимир HR Furioza, уточни, чи шукає роботу.\n"
    "3. Міні-кваліфікація: запитай вік і чи є ноутбук/ПК; якщо немає техніки або <18 – ввічливо відмов.\n"
    "4. Коротко поясни суть роботи оператора чату в сфері дейтингу.\n"
    "5. Опиши графік (денна 14–23, нічна 23–08) і запитай, що зручно.\n"
    "6. Поясни заробіток: перший місяць 450$+, далі 700–1000$+.\n"
    "7. Розкажи про навчання (онлайн, ~2 години, блоки з тестами, після чого оплачуване стажування).\n"
    "8. Запропонуй відео або пояснення в чаті; після відео попроси фідбек (+/-/питання).\n"
    "9. Відповідай на стандартні питання (що таке дейтинг, штрафи, документи, цікаві листи, платформа).\n"
    "10. Якщо кандидат готовий – попроси заповнити анкету з переліком даних (ПІБ, дата народження, телефон, посилання на Telegram, "
    "чи є діти до 3 років, вибір зміни, старт стажування, місто, email, скрін документа).\n"
    "11. Якщо замовк – роби ввічливі фоллоу-апи.\n"
    "12. Будь-який вільний текст кандидата спробуй віднести до етапу воронки і відповідай відповідним блоком, ведучи до наступного кроку."
)

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
current_scrape_job_id: Optional[str] = None
promo_scheduler_task: Optional[asyncio.Task] = None
promo_schedule_event = asyncio.Event()
promo_slot_last_day: Dict[str, str] = {}
promo_group_sync_lock = asyncio.Lock()
promo_last_sync_ts: float = 0.0
promo_paused: bool = True
promo_control_lock = asyncio.Lock()

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
    chat_title: Optional[str] = None
    source_chat: Optional[str] = None


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


class PromoGroupModel(BaseModel):
    id: int
    title: Optional[str]
    link: str
    enabled: bool
    added_at: str
    last_sent_at: Optional[str]
    last_status: Optional[str]


class PromoMessageModel(BaseModel):
    id: int
    text: str
    enabled: bool
    added_at: str


class PromoMessageCreate(BaseModel):
    text: str


class PromoScheduleEntry(BaseModel):
    slot: str
    hour: int
    minute: int


class PromoScheduleUpdate(BaseModel):
    slot: str
    hour: int
    minute: int


class PromoHistoryEntry(BaseModel):
    group_id: int
    group_title: Optional[str]
    link: str
    status: str
    sent_at: Optional[str]
    message_id: Optional[int]
    message_text: Optional[str]
    details: Optional[str]


class PromoSlotStatus(BaseModel):
    slot: str
    scheduled_for: str
    entries: List[PromoHistoryEntry]


class PromoGroupSummary(BaseModel):
    group_id: int
    title: Optional[str]
    link: str
    sent: int
    failed: int


class PromoStatusResponse(BaseModel):
    day: str
    slots: List[PromoSlotStatus]
    group_summary: List[PromoGroupSummary]
    total_sent: int
    total_failed: int
    is_paused: bool
    current_slot: Optional[str] = None


class DialogItem(BaseModel):
    peer_id: int
    name: str
    username: Optional[str]
    link: str
    unread: bool
    last_message: Optional[str]


class DialogListResponse(BaseModel):
    page: int
    has_more: bool
    items: List[DialogItem]


class DialogMessage(BaseModel):
    id: int
    text: Optional[str]
    is_outgoing: bool
    date: Optional[str]
    sender: Optional[str]


class DialogMessagesResponse(BaseModel):
    dialog: DialogItem
    messages: List[DialogMessage]
    next_offset: Optional[int]
    has_more: bool


class DialogSendRequest(BaseModel):
    text: str


class DialogSendResponse(BaseModel):
    id: int
    date: Optional[str]


class DialogSuggestRequest(BaseModel):
    draft: Optional[str] = None
    extra: Optional[str] = None


class DialogSuggestResponse(BaseModel):
    suggestions: List[str]


def _current_iso() -> str:
    return datetime.now(UTC_TZ).isoformat()


def _parse_iso(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    try:
        dt_value = datetime.fromisoformat(value)
    except ValueError:
        return None
    if dt_value.tzinfo is None:
        dt_value = dt_value.replace(tzinfo=UTC_TZ)
    return dt_value


def _to_kyiv_str(dt_value: Optional[datetime]) -> Optional[str]:
    if dt_value is None:
        return None
    local_dt = dt_value.astimezone(KYIV_TZ)
    return local_dt.strftime("%H:%M")


def _iso_to_kyiv_str(value: Optional[str]) -> Optional[str]:
    parsed = _parse_iso(value)
    return _to_kyiv_str(parsed)


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


def _ensure_promo_tables(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS promo_groups (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            title TEXT,
            link TEXT NOT NULL UNIQUE,
            enabled INTEGER NOT NULL DEFAULT 1,
            added_at TEXT NOT NULL,
            last_sent_at TEXT,
            last_status TEXT,
            peer_id INTEGER,
            peer_type TEXT,
            access_hash INTEGER,
            username TEXT
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS promo_messages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            text TEXT NOT NULL,
            enabled INTEGER NOT NULL DEFAULT 1,
            added_at TEXT NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS promo_schedule (
            slot TEXT PRIMARY KEY,
            hour INTEGER NOT NULL,
            minute INTEGER NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS promo_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            day_key TEXT NOT NULL,
            slot TEXT NOT NULL,
            group_id INTEGER NOT NULL,
            group_title TEXT,
            link TEXT NOT NULL,
            message_id INTEGER,
            message_text TEXT,
            planned_at TEXT NOT NULL,
            sent_at TEXT,
            status TEXT NOT NULL,
            details TEXT,
            FOREIGN KEY(group_id) REFERENCES promo_groups(id),
            FOREIGN KEY(message_id) REFERENCES promo_messages(id)
        )
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_promo_history_day_slot
        ON promo_history(day_key, slot)
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_promo_history_group
        ON promo_history(group_id, day_key)
        """
    )
    cursor = conn.execute("PRAGMA table_info(promo_groups)")
    columns = {row[1] for row in cursor.fetchall()}
    if "peer_id" not in columns:
        conn.execute("ALTER TABLE promo_groups ADD COLUMN peer_id INTEGER")
    if "peer_type" not in columns:
        conn.execute("ALTER TABLE promo_groups ADD COLUMN peer_type TEXT")
    if "access_hash" not in columns:
        conn.execute("ALTER TABLE promo_groups ADD COLUMN access_hash INTEGER")
    if "username" not in columns:
        conn.execute("ALTER TABLE promo_groups ADD COLUMN username TEXT")
    conn.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS idx_promo_groups_peer
        ON promo_groups(peer_id)
        WHERE peer_id IS NOT NULL
        """
    )
    conn.commit()


def _ensure_default_promo_schedule(conn: sqlite3.Connection) -> None:
    for slot in PROMO_SLOTS:
        defaults = PROMO_DEFAULT_SCHEDULE.get(slot, {"hour": 9, "minute": 0})
        conn.execute(
            """
            INSERT INTO promo_schedule(slot, hour, minute)
            VALUES(?, ?, ?)
            ON CONFLICT(slot) DO NOTHING
            """,
            (slot, defaults["hour"], defaults["minute"]),
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


def _write_export_metadata(csv_path: str, chat_title: str, source_chat: str) -> None:
    meta = {
        "chat_title": chat_title,
        "source_chat": source_chat,
        "generated_at": _current_iso(),
    }
    meta_path = f"{csv_path}.meta.json"
    with open(meta_path, "w", encoding="utf-8") as meta_file:
        json.dump(meta, meta_file, ensure_ascii=False, indent=2)


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
        chat_title = None
        source_chat = None

        meta_path = f"{file_path}.meta.json"
        if os.path.exists(meta_path):
            try:
                with open(meta_path, "r", encoding="utf-8") as meta_file:
                    meta = json.load(meta_file)
                    chat_title = meta.get("chat_title")
                    source_chat = meta.get("source_chat")
            except (json.JSONDecodeError, OSError):
                pass
        if not chat_title and job_id:
            job = SCRAPE_JOBS.get(job_id, {})
            chat_title = job.get("chat_title")
            source_chat = source_chat or job.get("source_chat")

        exports.append(
            CSVExport(
                filename=entry.name,
                job_id=job_id,
                created_at=created_at,
                url=f"/scrape_export/{entry.name}",
                chat_title=chat_title,
                source_chat=source_chat,
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


def _list_promo_groups_sync(conn: sqlite3.Connection) -> List[Dict[str, Any]]:
    cursor = conn.execute(
        """
        SELECT id, title, link, enabled, added_at, last_sent_at, last_status, peer_id, peer_type, access_hash, username
        FROM promo_groups
        ORDER BY id ASC
        """
    )
    rows = cursor.fetchall()
    return [
        {
            "id": row[0],
            "title": row[1],
            "link": row[2],
            "enabled": bool(row[3]),
            "added_at": row[4],
            "last_sent_at": row[5],
            "last_status": row[6],
            "peer_id": row[7],
            "peer_type": row[8],
            "access_hash": row[9],
            "username": row[10],
        }
        for row in rows
    ]


def _list_promo_messages_sync(conn: sqlite3.Connection) -> List[Dict[str, Any]]:
    cursor = conn.execute(
        """
        SELECT id, text, enabled, added_at
        FROM promo_messages
        ORDER BY id ASC
        """
    )
    rows = cursor.fetchall()
    return [
        {
            "id": row[0],
            "text": row[1],
            "enabled": bool(row[2]),
            "added_at": row[3],
        }
        for row in rows
    ]


def _create_promo_message_sync(conn: sqlite3.Connection, text: str) -> Dict[str, Any]:
    now = _current_iso()
    conn.execute(
        """
        INSERT INTO promo_messages(text, enabled, added_at)
        VALUES(?, 1, ?)
        """,
        (text, now),
    )
    conn.commit()
    cursor = conn.execute(
        """
        SELECT id, text, enabled, added_at
        FROM promo_messages
        WHERE rowid = last_insert_rowid()
        """
    )
    row = cursor.fetchone()
    return {
        "id": row[0],
        "text": row[1],
        "enabled": bool(row[2]),
        "added_at": row[3],
    }


def _delete_promo_message_sync(conn: sqlite3.Connection, message_id: int) -> bool:
    cursor = conn.execute("DELETE FROM promo_messages WHERE id = ?", (message_id,))
    conn.commit()
    return cursor.rowcount > 0


def _disable_group_ids(conn: sqlite3.Connection, group_ids: Set[int]) -> None:
    if not group_ids:
        return
    placeholders = ",".join("?" for _ in group_ids)
    conn.execute(
        f"UPDATE promo_groups SET enabled = 0 WHERE id IN ({placeholders})",
        tuple(group_ids),
    )


def _build_group_record_from_entity(entity: Any) -> Optional[Dict[str, Any]]:
    if isinstance(entity, types.Channel):
        peer_type = "channel"
        access_hash = getattr(entity, "access_hash", None)
    elif isinstance(entity, types.Chat):
        peer_type = "chat"
        access_hash = None
    else:
        return None

    peer_id = getattr(entity, "id", None)
    if peer_id is None:
        return None

    username = getattr(entity, "username", None)
    link_value = f"https://t.me/{username}" if username else f"id:{peer_id}"
    title = getattr(entity, "title", None) or username or f"id:{peer_id}"

    return {
        "peer_id": peer_id,
        "peer_type": peer_type,
        "access_hash": access_hash,
        "username": username,
        "title": title,
        "link": link_value,
    }


def _apply_promo_group_records_sync(conn: sqlite3.Connection, records: List[Dict[str, Any]]) -> None:
    cursor = conn.execute(
        "SELECT id, peer_id FROM promo_groups WHERE peer_id IS NOT NULL"
    )
    existing_map = {row[1]: row[0] for row in cursor.fetchall() if row[1] is not None}
    managed_ids = set(existing_map.values())
    seen_ids: Set[int] = set()
    now = _current_iso()

    for record in records:
        peer_id = record["peer_id"]
        row_id = existing_map.get(peer_id)
        if row_id:
            conn.execute(
                """
                UPDATE promo_groups
                SET title = ?, link = ?, username = ?, peer_type = ?, access_hash = ?, enabled = 1
                WHERE id = ?
                """,
                (
                    record["title"],
                    record["link"],
                    record["username"],
                    record["peer_type"],
                    record["access_hash"],
                    row_id,
                ),
            )
            seen_ids.add(row_id)
        else:
            conn.execute(
                """
                INSERT INTO promo_groups(
                    title,
                    link,
                    enabled,
                    added_at,
                    last_sent_at,
                    last_status,
                    peer_id,
                    peer_type,
                    access_hash,
                    username
                )
                VALUES(?, ?, 1, ?, NULL, NULL, ?, ?, ?, ?)
                """,
                (
                    record["title"],
                    record["link"],
                    now,
                    record["peer_id"],
                    record["peer_type"],
                    record["access_hash"],
                    record["username"],
                ),
            )
            new_row_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
            seen_ids.add(new_row_id)

    missing_ids = managed_ids - seen_ids
    if missing_ids:
        _disable_group_ids(conn, missing_ids)

    conn.commit()


def _group_to_input_peer(group: Dict[str, Any]) -> Optional[Any]:
    peer_id = group.get("peer_id")
    peer_type = group.get("peer_type")
    if not peer_id or not peer_type:
        return None
    if peer_type == "chat":
        return InputPeerChat(int(peer_id))
    access_hash = group.get("access_hash")
    if access_hash is None:
        return None
    return InputPeerChannel(int(peer_id), int(access_hash))


def _group_display_name(group: Dict[str, Any]) -> str:
    return (
        group.get("title")
        or (group.get("username") and f"@{group['username']}")
        or group.get("link")
        or f"id:{group.get('peer_id')}"
    )


async def _sync_promo_groups_from_folder() -> None:
    if db_conn is None or not PROMO_FOLDER_NAME:
        return

    try:
        filters_result = await client(functions.messages.GetDialogFiltersRequest())
    except RPCError as exc:
        logger.error("Не удалось получить папки диалогов: %s", exc)
        return
    except Exception as exc:
        logger.exception("Не удалось получить папки диалогов: %s", exc)
        return

    folder_title_lower = PROMO_FOLDER_NAME.lower()
    target_filter = None
    dialog_filters = getattr(filters_result, "filters", []) or []
    for dialog_filter in dialog_filters:
        if getattr(dialog_filter, "title", "").lower() == folder_title_lower:
            target_filter = dialog_filter
            break

    if target_filter is None:
        logger.warning(
            "Папка '%s' не найдена среди фильтров. Группы рекламы недоступны.",
            PROMO_FOLDER_NAME,
        )
        records: List[Dict[str, Any]] = []
    else:
        include_peers = getattr(target_filter, "include_peers", []) or []
        records = []
        for peer in include_peers:
            try:
                entity = await client.get_entity(peer)
            except Exception as exc:
                logger.warning(
                    "Не удалось получить сущность для папки '%s': %s",
                    PROMO_FOLDER_NAME,
                    exc,
                )
                continue
            record = _build_group_record_from_entity(entity)
            if record:
                records.append(record)

    async with db_lock:
        await asyncio.to_thread(_apply_promo_group_records_sync, db_conn, records)
    logger.info("Папка '%s': синхронизировано групп: %d", PROMO_FOLDER_NAME, len(records))


async def ensure_promo_groups_synced(force: bool = False) -> None:
    global promo_last_sync_ts
    if not PROMO_FOLDER_NAME:
        return
    now = asyncio.get_event_loop().time()
    interval = max(PROMO_GROUP_SYNC_INTERVAL_SECONDS, 5)
    if not force and now - promo_last_sync_ts < interval:
        return
    async with promo_group_sync_lock:
        now = asyncio.get_event_loop().time()
        if not force and now - promo_last_sync_ts < interval:
            return
        await _sync_promo_groups_from_folder()
        promo_last_sync_ts = now


def _get_promo_schedule_sync(conn: sqlite3.Connection) -> List[Dict[str, Any]]:
    cursor = conn.execute(
        """
        SELECT slot, hour, minute
        FROM promo_schedule
        ORDER BY CASE slot
            WHEN 'morning' THEN 0
            WHEN 'noon' THEN 1
            WHEN 'evening' THEN 2
            ELSE 3 END, slot
        """
    )
    rows = cursor.fetchall()
    return [{"slot": row[0], "hour": row[1], "minute": row[2]} for row in rows]


def _update_promo_schedule_entry_sync(conn: sqlite3.Connection, slot: str, hour: int, minute: int) -> None:
    conn.execute(
        """
        INSERT INTO promo_schedule(slot, hour, minute)
        VALUES(?, ?, ?)
        ON CONFLICT(slot) DO UPDATE SET hour = excluded.hour, minute = excluded.minute
        """,
        (slot, hour, minute),
    )
    conn.commit()


def _fetch_promo_schedule_map_sync(conn: sqlite3.Connection) -> Dict[str, Dict[str, int]]:
    rows = _get_promo_schedule_sync(conn)
    return {row["slot"]: {"hour": row["hour"], "minute": row["minute"]} for row in rows}


def _fetch_slot_done_groups_sync(conn: sqlite3.Connection, day_key: str, slot: str) -> Set[int]:
    cursor = conn.execute(
        """
        SELECT DISTINCT group_id FROM promo_history
        WHERE day_key = ? AND slot = ?
        """,
        (day_key, slot),
    )
    return {row[0] for row in cursor.fetchall()}


def _record_promo_history_sync(
    conn: sqlite3.Connection,
    *,
    day_key: str,
    slot: str,
    group: Dict[str, Any],
    message: Optional[Dict[str, Any]],
    planned_at: str,
    sent_at: Optional[str],
    status: str,
    details: Optional[str],
) -> None:
    conn.execute(
        """
        INSERT INTO promo_history(day_key, slot, group_id, group_title, link, message_id, message_text, planned_at, sent_at, status, details)
        VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            day_key,
            slot,
            group["id"],
            group.get("title"),
            group["link"],
            message.get("id") if message else None,
            message.get("text") if message else None,
            planned_at,
            sent_at,
            status,
            details,
        ),
    )
    conn.commit()


def _update_group_send_stats_sync(
    conn: sqlite3.Connection,
    group_id: int,
    sent_at: Optional[str],
    status: str,
) -> None:
    conn.execute(
        """
        UPDATE promo_groups
        SET last_sent_at = ?, last_status = ?
        WHERE id = ?
        """,
        (sent_at, status, group_id),
    )
    conn.commit()


def _fetch_promo_history_day_sync(conn: sqlite3.Connection, day_key: str) -> List[Dict[str, Any]]:
    cursor = conn.execute(
        """
        SELECT day_key, slot, group_id, group_title, link, message_id, message_text, sent_at, status, details
        FROM promo_history
        WHERE day_key = ?
        ORDER BY id ASC
        """,
        (day_key,),
    )
    rows = cursor.fetchall()
    return [
        {
            "slot": row[1],
            "group_id": row[2],
            "group_title": row[3],
            "link": row[4],
            "message_id": row[5],
            "message_text": row[6],
            "sent_at": row[7],
            "status": row[8],
            "details": row[9],
        }
        for row in rows
    ]


def _fetch_promo_group_summary_sync(conn: sqlite3.Connection, day_key: str) -> List[Dict[str, Any]]:
    cursor = conn.execute(
        """
        SELECT g.id, COALESCE(g.title, ''), g.link,
               SUM(CASE WHEN h.status = 'sent' THEN 1 ELSE 0 END) as sent_count,
               SUM(CASE WHEN h.status != 'sent' THEN 1 ELSE 0 END) as failed_count
        FROM promo_groups g
        LEFT JOIN promo_history h ON g.id = h.group_id AND h.day_key = ?
        GROUP BY g.id, g.title, g.link
        ORDER BY g.id
        """,
        (day_key,),
    )
    rows = cursor.fetchall()
    return [
        {
            "group_id": row[0],
            "title": row[1] or None,
            "link": row[2],
            "sent": row[3] or 0,
            "failed": row[4] or 0,
        }
        for row in rows
    ]


def _count_slot_totals(rows: List[Dict[str, Any]]) -> Dict[str, int]:
    sent = sum(1 for row in rows if row.get("status") == "sent")
    failed = sum(1 for row in rows if row.get("status") != "sent")
    return {"sent": sent, "failed": failed}


def _build_day_key(dt: Optional[datetime] = None) -> str:
    if dt is None:
        target = datetime.now(KYIV_TZ)
    else:
        if dt.tzinfo is None:
            target = dt.replace(tzinfo=UTC_TZ).astimezone(KYIV_TZ)
        else:
            target = dt.astimezone(KYIV_TZ)
    return target.date().isoformat()


def _build_schedule_plan_for_day(day_key: str, schedule_rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    try:
        base_date = datetime.fromisoformat(day_key).date()
    except ValueError:
        base_date = datetime.now(KYIV_TZ).date()

    plan: List[Dict[str, Any]] = []
    for row in schedule_rows:
        slot = row.get("slot")
        hour = int(row.get("hour", 0))
        minute = int(row.get("minute", 0))
        start_local = datetime.combine(base_date, time(hour, minute), tzinfo=KYIV_TZ)
        plan.append(
            {
                "slot": slot,
                "hour": hour,
                "minute": minute,
                "local_start": start_local,
                "next_start": None,
                "scheduled_display": start_local.strftime("%H:%M"),
            }
        )

    plan.sort(key=lambda e: (e["hour"], e["minute"], e["slot"]))
    if not plan:
        return plan

    for idx, entry in enumerate(plan):
        next_entry = plan[(idx + 1) % len(plan)]
        next_start = next_entry["local_start"]
        if next_start <= entry["local_start"]:
            next_start = next_start + timedelta(days=1)
        entry["next_start"] = next_start
    return plan


def _determine_current_slot(plan: List[Dict[str, Any]], *, now: Optional[datetime] = None) -> Optional[str]:
    if not plan:
        return None
    now_local = now or datetime.now(KYIV_TZ)
    for entry in plan:
        start = entry["local_start"]
        end = entry["next_start"]
        if start <= now_local < end:
            return entry["slot"]
        if now_local < start:
            return entry["slot"]
    return plan[0]["slot"]


def _dialog_display_name(user: types.User) -> str:
    first = user.first_name or ""
    last = user.last_name or ""
    name = f"{first} {last}".strip()
    if name:
        return name
    if user.username:
        return f"@{user.username}"
    return f"id:{user.id}"


def _dialog_link(user: types.User) -> str:
    if user.username:
        return f"https://t.me/{user.username}"
    return f"tg://user?id={user.id}"


def _truncate_preview(text: Optional[str], limit: int = 60) -> str:
    if not text:
        return ""
    text = text.strip().replace("\n", " ")
    if len(text) <= limit:
        return text
    return text[: limit - 1] + "…"


def _dialog_to_dict(dialog: Any) -> Dict[str, Any]:
    entity: types.User = dialog.entity
    name = _dialog_display_name(entity)
    username = entity.username
    return {
        "peer_id": entity.id,
        "name": name,
        "username": username,
        "link": _dialog_link(entity),
        "unread": bool(dialog.unread_count),
        "last_message": _truncate_preview(getattr(dialog.message, "message", "") if dialog.message else ""),
    }


def _message_to_dict(message: Any) -> Dict[str, Any]:
    text = message.message or ""
    sender_display = "Я" if message.out else "Кандидат"
    return {
        "id": message.id,
        "text": text,
        "is_outgoing": bool(message.out),
        "date": message.date.astimezone(KYIV_TZ).isoformat() if message.date else None,
        "sender": sender_display,
    }


def _entity_to_dialog_item(entity: types.User) -> Dict[str, Any]:
    return {
        "peer_id": entity.id,
        "name": _dialog_display_name(entity),
        "username": entity.username,
        "link": _dialog_link(entity),
        "unread": False,
        "last_message": None,
    }


async def _ensure_private_entity(peer_id: int) -> types.User:
    entity = await client.get_entity(int(peer_id))
    if not isinstance(entity, types.User) or entity.is_self or getattr(entity, "bot", False):
        raise HTTPException(status_code=400, detail="Dialog is not a private user chat")
    return entity


async def _list_private_dialogs_page(page: int, page_size: int) -> Tuple[List[Dict[str, Any]], bool]:
    if page < 0:
        page = 0
    start_index = page * page_size
    end_index = start_index + page_size
    target_count = end_index + 1
    collected: List[Any] = []
    async for dialog in client.iter_dialogs():
        if not isinstance(dialog.entity, types.User):
            continue
        entity: types.User = dialog.entity
        if entity.is_self or getattr(entity, "bot", False):
            continue
        collected.append(dialog)
        if len(collected) >= target_count:
            break
    items = collected[start_index:end_index]
    has_more = len(collected) > end_index
    return [_dialog_to_dict(dialog) for dialog in items], has_more


async def _fetch_dialog_messages(peer_id: int, limit: int, offset_id: Optional[int]) -> Tuple[List[Dict[str, Any]], Optional[int]]:
    entity = await _ensure_private_entity(peer_id)
    messages: List[Dict[str, Any]] = []
    next_offset = None
    async for message in client.iter_messages(entity, limit=limit, offset_id=offset_id or 0):
        messages.append(_message_to_dict(message))
    if messages and len(messages) == limit:
        next_offset = messages[-1]["id"]
    return messages, next_offset


async def _collect_recent_messages_for_context(peer_id: int, limit: int = 10) -> List[Dict[str, Any]]:
    entity = await _ensure_private_entity(peer_id)
    collected: List[Dict[str, Any]] = []
    async for message in client.iter_messages(entity, limit=limit):
        collected.append(_message_to_dict(message))
    collected.reverse()
    return collected


def _build_conversation_summary(messages: List[Dict[str, Any]]) -> str:
    lines = []
    for item in messages:
        sender = item.get("sender") or ("Я" if item.get("is_outgoing") else "Кандидат")
        text = item.get("text") or "[без тексту]"
        text = text.strip()
        if len(text) > 400:
            text = text[:400] + "…"
        lines.append(f"{sender}: {text}")
    return "\n".join(lines)


def _parse_gpt_suggestions(raw_text: str, limit: int = 3) -> List[str]:
    suggestions: List[str] = []
    for line in raw_text.splitlines():
        stripped = line.strip()
        if not stripped:
            continue
        match = re.match(r"^(\d+)[)\.-]\s*(.+)$", stripped)
        if match:
            suggestions.append(match.group(2).strip())
        else:
            suggestions.append(stripped)
        if len(suggestions) >= limit:
            break
    if not suggestions and raw_text.strip():
        suggestions = [raw_text.strip()]
    return suggestions[:limit]


async def _request_gpt_responses(prompt: str) -> str:
    if not OPENAI_API_KEY:
        raise RuntimeError("OPENAI_API_KEY is not configured")

    payload = {
        "model": OPENAI_DIALOG_MODEL,
        "input": [
            {
                "role": "system",
                "content": "Ти — HR-асистент Furioza. Відповідай коротко та по сценарию."
            },
            {"role": "user", "content": prompt},
        ],
    }

    def _post_request() -> Dict[str, Any]:
        response = requests.post(
            "https://api.openai.com/v1/responses",
            headers={
                "Authorization": f"Bearer {OPENAI_API_KEY}",
                "Content-Type": "application/json",
            },
            json=payload,
            timeout=60,
        )
        text = response.text
        if not response.ok:
            raise RuntimeError(f"OpenAI error {response.status_code}: {text[:200]}")
        try:
            data = response.json()
        except json.JSONDecodeError as exc:
            raise RuntimeError(f"OpenAI returned invalid JSON: {text[:200]}") from exc
        return data

    data = await asyncio.to_thread(_post_request)
    output = data.get("output") or []
    if output and isinstance(output, list):
        first = output[0]
        content = first.get("content") if isinstance(first, dict) else None
        if content and isinstance(content, list):
            text_item = content[0]
            if isinstance(text_item, dict):
                text_value = text_item.get("text")
                if text_value:
                    return text_value
    text_alt = data.get("output_text")
    if text_alt:
        return text_alt
    raise RuntimeError("OpenAI response missing text content")


async def _generate_dialog_suggestions(peer_id: int, draft: Optional[str], extra: Optional[str] = None) -> List[str]:
    recent_messages = await _collect_recent_messages_for_context(peer_id, limit=10)
    summary = _build_conversation_summary(recent_messages)
    history_payload = [
        {
            "sender": "me" if msg.get("is_outgoing") else "candidate",
            "text": msg.get("text") or "",
        }
        for msg in recent_messages
    ]
    if DIALOG_AI_URL:
        payload = {"history": history_payload, "draft": draft or ""}
        if extra:
            payload["extra"] = extra
        def _post_dialog_server() -> Dict[str, Any]:
            response = requests.post(DIALOG_AI_URL, json=payload, timeout=60)
            if response.status_code != 200:
                raise RuntimeError(f"Dialog AI server {response.status_code}: {response.text[:200]}")
            return response.json()
        data = await asyncio.to_thread(_post_dialog_server)
        suggestions = data.get("suggestions") if isinstance(data, dict) else None
        if suggestions and isinstance(suggestions, list):
            return [str(item).strip() for item in suggestions if str(item).strip()]
    prompt_parts = [HR_ASSISTANT_PROMPT, "", "Останні повідомлення (від старих до нових):", summary or "(історія пуста)"]
    if draft:
        prompt_parts.append("")
        prompt_parts.append(f"Чернетка HR (можна переформулювати): {draft}")
    if extra:
        prompt_parts.append("")
        prompt_parts.append(f"Додаткові побажання HR: {extra}")
    prompt_parts.append("")
    prompt_parts.append("Сформуй три можливі відповіді, дотримуйся формату 1) ..., 2) ..., 3) ...")
    final_prompt = "\n".join(part for part in prompt_parts if part is not None)
    raw_text = await _request_gpt_responses(final_prompt)
    return _parse_gpt_suggestions(raw_text)


def _trigger_promo_scheduler_check() -> None:
    if not promo_schedule_event.is_set():
        promo_schedule_event.set()


async def _get_active_promo_groups() -> List[Dict[str, Any]]:
    await ensure_promo_groups_synced()
    if db_conn is None:
        return []
    async with db_lock:
        groups = await asyncio.to_thread(_list_promo_groups_sync, db_conn)
    return [group for group in groups if group.get("enabled") and group.get("peer_id")]


async def _get_active_promo_messages() -> List[Dict[str, Any]]:
    if db_conn is None:
        return []
    async with db_lock:
        messages = await asyncio.to_thread(_list_promo_messages_sync, db_conn)
    return [message for message in messages if message.get("enabled")]


async def _get_promo_schedule_map() -> Dict[str, Dict[str, int]]:
    if db_conn is None:
        return {}
    async with db_lock:
        schedule = await asyncio.to_thread(_fetch_promo_schedule_map_sync, db_conn)
    return schedule


async def _get_pending_groups(slot: str, day_key: str, groups: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    if not groups or db_conn is None:
        return []
    async with db_lock:
        done_ids = await asyncio.to_thread(_fetch_slot_done_groups_sync, db_conn, day_key, slot)
    return [group for group in groups if group["id"] not in done_ids]


async def _record_promo_result(
    *,
    day_key: str,
    slot: str,
    group: Dict[str, Any],
    message: Optional[Dict[str, Any]],
    planned_at: str,
    sent_at: Optional[str],
    status: str,
    details: Optional[str],
) -> None:
    if db_conn is None:
        return
    async with db_lock:
        await asyncio.to_thread(
            _record_promo_history_sync,
            db_conn,
            day_key=day_key,
            slot=slot,
            group=group,
            message=message,
            planned_at=planned_at,
            sent_at=sent_at,
            status=status,
            details=details,
        )
        await asyncio.to_thread(
            _update_group_send_stats_sync,
            db_conn,
            group["id"],
            sent_at,
            status,
        )


async def _run_promo_slot(slot: str, schedule_entry: Dict[str, int], day_key: str) -> bool:
    if promo_paused:
        logger.info("Promo slot %s skipped — scheduler paused", slot)
        return False
    groups = await _get_active_promo_groups()
    if not groups:
        logger.info("Promo slot %s skipped — no groups configured", slot)
        return False

    messages = await _get_active_promo_messages()
    if not messages:
        logger.warning("Promo slot %s skipped — no messages configured", slot)
        return False

    pending_groups = await _get_pending_groups(slot, day_key, groups)
    if not pending_groups:
        return True

    planned_dt = datetime.now(KYIV_TZ).replace(
        hour=schedule_entry.get("hour", 9),
        minute=schedule_entry.get("minute", 0),
        second=0,
        microsecond=0,
    )
    planned_iso = planned_dt.astimezone(UTC_TZ).isoformat()

    logger.info("Starting promo slot %s for %d groups", slot, len(pending_groups))

    if not client.is_connected():
        await client.connect()

    for idx, group in enumerate(pending_groups):
        message = random.choice(messages)
        status = "sent"
        details = None
        sent_at = None
        target_peer = _group_to_input_peer(group)
        display_name = _group_display_name(group)
        if target_peer is None:
            status = "failed"
            details = "invalid_peer"
            logger.warning("Promo group %s не имеет корректного peer_id", display_name)
        else:
            try:
                await client.send_message(target_peer, message["text"])
                sent_at = _current_iso()
                status = "sent"
                logger.info("Promo message sent to %s", display_name)
            except FloodWaitError as exc:
                wait_seconds = int(getattr(exc, "seconds", 30))
                details = f"flood_wait:{wait_seconds}"
                status = "failed"
                logger.warning(
                    "Flood wait while sending promo to %s: %s",
                    display_name,
                    wait_seconds,
                )
                await asyncio.sleep(min(wait_seconds + 1, 120))
            except RPCError as exc:
                status = "failed"
                details = f"rpc_error:{exc.__class__.__name__}"
                logger.exception("RPC error sending promo to %s", display_name)
            except Exception as exc:
                status = "failed"
                details = f"error:{exc}"
                logger.exception("Unexpected error sending promo to %s", display_name)

        await _record_promo_result(
            day_key=day_key,
            slot=slot,
            group=group,
            message=message,
            planned_at=planned_iso,
            sent_at=sent_at,
            status=status,
            details=details,
        )

        if idx < len(pending_groups) - 1:
            delay = random.uniform(PROMO_MIN_DELAY_SECONDS, PROMO_MAX_DELAY_SECONDS)
            await asyncio.sleep(delay)

    return True


async def _promo_scheduler_iteration() -> None:
    await ensure_promo_groups_synced()
    schedule_map = await _get_promo_schedule_map()
    if not schedule_map:
        return
    now = datetime.now(KYIV_TZ)
    day_key = _build_day_key(now)
    for slot_name, recorded_day in list(promo_slot_last_day.items()):
        if recorded_day != day_key:
            promo_slot_last_day.pop(slot_name, None)
    schedule_rows = [
        {"slot": slot, "hour": times.get("hour"), "minute": times.get("minute")}
        for slot, times in schedule_map.items()
    ]
    plan = _build_schedule_plan_for_day(day_key, schedule_rows)
    for entry in plan:
        slot = entry["slot"]
        slot_time = {"hour": entry["hour"], "minute": entry["minute"]}
        start_local = entry["local_start"]
        next_start = entry["next_start"]
        if promo_slot_last_day.get(slot) == day_key:
            continue
        if now < start_local:
            continue
        if now >= next_start:
            promo_slot_last_day[slot] = day_key
            continue
        slot_completed = await _run_promo_slot(slot, slot_time, day_key)
        if slot_completed:
            promo_slot_last_day[slot] = day_key


async def promo_scheduler_loop() -> None:
    logger.info("Promo scheduler started")
    while True:
        try:
            await _promo_scheduler_iteration()
        except Exception as exc:
            logger.exception("Promo scheduler iteration failed: %s", exc)
        wait_time = 60.0
        try:
            await asyncio.wait_for(promo_schedule_event.wait(), timeout=wait_time)
            promo_schedule_event.clear()
        except asyncio.TimeoutError:
            continue



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
    global current_scrape_job_id
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

            await _update_job(job_id, total=0, processed=0, cancel_requested=False)

            entity = await client.get_entity(chat_value)
            chat_title = _derive_chat_title(entity, chat_value)
            fallback_name = _fallback_from_chat_value(chat_value)
            source_chat_identifier = fallback_name or chat_title or chat_value
            safe_title = _safe_filename_component(chat_title) or _safe_filename_component(fallback_name) or f"job_{job_id}"
            csv_filename = f"members_{safe_title}.csv"
            csv_path = os.path.join(CSV_OUTPUT_DIR, csv_filename)
            await _update_job(job_id, chat_title=chat_title, source_chat=source_chat_identifier)
            current_scrape_job_id = job_id

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
                current_job = SCRAPE_JOBS.get(job_id, {})
                if current_job.get("cancel_requested"):
                    logger.info("Scrape job %s cancellation requested. Stopping.", job_id)
                    break
                is_new = user.id not in existing_ids
                member = Member(
                    id=user.id,
                    username=user.username,
                    first_name=user.first_name,
                    last_name=user.last_name,
                    phone=user.phone,
                    added_at=datetime.utcnow().isoformat(),
                    is_hr=_is_hr_candidate(user.username, user.first_name, user.last_name),
                    source_chat=source_chat_identifier,
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
            await asyncio.to_thread(_write_export_metadata, csv_path, chat_title, source_chat_identifier)

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

        status_value = (
            "cancelled"
            if SCRAPE_JOBS.get(job_id, {}).get("cancel_requested")
            else "done"
        )
        await _update_job(
            job_id,
            status=status_value,
            finished_at=_current_iso(),
            total=len(job_members),
            processed=processed_total,
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
    _ensure_promo_tables(conn)
    _ensure_default_promo_schedule(conn)
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
    global promo_paused
    promo_paused = True
    await ensure_promo_groups_synced(force=True)
    global promo_scheduler_task
    if promo_scheduler_task is None or promo_scheduler_task.done():
        promo_scheduler_task = asyncio.create_task(promo_scheduler_loop())


@app.on_event("shutdown")
async def on_shutdown():
    await client.disconnect()
    global db_conn
    if db_conn:
        db_conn.close()
        db_conn = None
    global promo_scheduler_task
    if promo_scheduler_task:
        promo_scheduler_task.cancel()
        try:
            await promo_scheduler_task
        except asyncio.CancelledError:
            pass
        promo_scheduler_task = None


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


@app.post("/scrape_stop")
async def scrape_stop(job_id: str):
    async with jobs_lock:
        job = SCRAPE_JOBS.get(job_id)
        if job is None:
            raise HTTPException(status_code=404, detail="Job not found")
        if job.get("status") in {"done", "error", "cancelled"}:
            return {"status": job.get("status"), "message": "Job already finished."}
        job["cancel_requested"] = True
        job["message"] = "Cancellation requested by user."
    return {"status": "cancelling"}


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

    return BroadcastJobResponse(job_id=job_id, status="started", source_chat=source_chat)


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


@app.get("/promo/groups", response_model=List[PromoGroupModel])
async def get_promo_groups():
    if db_conn is None:
        raise HTTPException(status_code=500, detail="Database is not initialised.")
    await ensure_promo_groups_synced(force=True)
    async with db_lock:
        groups = await asyncio.to_thread(_list_promo_groups_sync, db_conn)
    groups = [group for group in groups if group.get("peer_id") and group.get("enabled")]
    return [
        PromoGroupModel(
            id=group["id"],
            title=group.get("title"),
            link=group["link"],
            enabled=group.get("enabled", True),
            added_at=group["added_at"],
            last_sent_at=group.get("last_sent_at"),
            last_status=group.get("last_status"),
        )
        for group in groups
    ]


@app.post("/promo/pause")
async def promo_pause():
    global promo_paused
    async with promo_control_lock:
        promo_paused = True
    return {"paused": True}


@app.post("/promo/resume")
async def promo_resume():
    global promo_paused
    async with promo_control_lock:
        promo_paused = False
    _trigger_promo_scheduler_check()
    return {"paused": False}


@app.get("/dialogs", response_model=DialogListResponse)
async def api_list_dialogs(page: int = 0):
    items, has_more = await _list_private_dialogs_page(page, CHAT_DIALOG_PAGE_SIZE)
    return DialogListResponse(
        page=page,
        has_more=has_more,
        items=[DialogItem(**item) for item in items],
    )


@app.get("/dialogs/{peer_id}/messages", response_model=DialogMessagesResponse)
async def api_dialog_messages(peer_id: int, offset_id: Optional[int] = None):
    entity = await _ensure_private_entity(peer_id)
    messages, next_offset = await _fetch_dialog_messages(peer_id, CHAT_MESSAGE_PAGE_SIZE, offset_id)
    dialog_item = _entity_to_dialog_item(entity)
    return DialogMessagesResponse(
        dialog=DialogItem(**dialog_item),
        messages=[DialogMessage(**msg) for msg in messages],
        next_offset=next_offset,
        has_more=bool(next_offset),
    )


@app.post("/dialogs/{peer_id}/send", response_model=DialogSendResponse)
async def api_dialog_send(peer_id: int, payload: DialogSendRequest):
    text = (payload.text or "").strip()
    if not text:
        raise HTTPException(status_code=400, detail="Text is required")
    entity = await _ensure_private_entity(peer_id)
    message = await client.send_message(entity, text)
    date_iso = message.date.astimezone(KYIV_TZ).isoformat() if message.date else None
    return DialogSendResponse(id=message.id, date=date_iso)


@app.post("/dialogs/{peer_id}/suggest", response_model=DialogSuggestResponse)
async def api_dialog_suggest(peer_id: int, payload: DialogSuggestRequest):
    try:
        await _ensure_private_entity(peer_id)
        suggestions = await _generate_dialog_suggestions(
            peer_id,
            (payload.draft or "").strip() or None,
            (payload.extra or "").strip() or None,
        )
    except HTTPException:
        raise
    except Exception as exc:
        logger.exception("Failed to generate suggestions: %s", exc)
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    return DialogSuggestResponse(suggestions=suggestions)


@app.get("/promo/messages", response_model=List[PromoMessageModel])
async def get_promo_messages():
    if db_conn is None:
        raise HTTPException(status_code=500, detail="Database is not initialised.")
    async with db_lock:
        messages = await asyncio.to_thread(_list_promo_messages_sync, db_conn)
    return [
        PromoMessageModel(
            id=message["id"],
            text=message["text"],
            enabled=message.get("enabled", True),
            added_at=message["added_at"],
        )
        for message in messages
    ]


@app.post("/promo/messages", response_model=PromoMessageModel)
async def create_promo_message(payload: PromoMessageCreate):
    if db_conn is None:
        raise HTTPException(status_code=500, detail="Database is not initialised.")
    text = (payload.text or "").strip()
    if not text:
        raise HTTPException(status_code=400, detail="Message text is required.")
    async with db_lock:
        message = await asyncio.to_thread(_create_promo_message_sync, db_conn, text)
    _trigger_promo_scheduler_check()
    return PromoMessageModel(
        id=message["id"],
        text=message["text"],
        enabled=message.get("enabled", True),
        added_at=message["added_at"],
    )


@app.delete("/promo/messages/{message_id}")
async def delete_promo_message(message_id: int):
    if db_conn is None:
        raise HTTPException(status_code=500, detail="Database is not initialised.")
    async with db_lock:
        deleted = await asyncio.to_thread(_delete_promo_message_sync, db_conn, message_id)
    if not deleted:
        raise HTTPException(status_code=404, detail="Message not found")
    _trigger_promo_scheduler_check()
    return {"deleted": True}


@app.get("/promo/schedule", response_model=List[PromoScheduleEntry])
async def get_promo_schedule():
    if db_conn is None:
        raise HTTPException(status_code=500, detail="Database is not initialised.")
    async with db_lock:
        schedule = await asyncio.to_thread(_get_promo_schedule_sync, db_conn)
    return [
        PromoScheduleEntry(slot=row["slot"], hour=row["hour"], minute=row["minute"])
        for row in schedule
    ]


@app.put("/promo/schedule", response_model=PromoScheduleEntry)
async def update_promo_schedule(entry: PromoScheduleUpdate):
    if entry.slot not in PROMO_SLOTS:
        raise HTTPException(status_code=400, detail="Unknown slot")
    if entry.hour < 0 or entry.hour > 23 or entry.minute < 0 or entry.minute > 59:
        raise HTTPException(status_code=400, detail="Invalid time")
    if db_conn is None:
        raise HTTPException(status_code=500, detail="Database is not initialised.")
    async with db_lock:
        await asyncio.to_thread(
            _update_promo_schedule_entry_sync,
            db_conn,
            entry.slot,
            entry.hour,
            entry.minute,
        )
    _trigger_promo_scheduler_check()
    return PromoScheduleEntry(slot=entry.slot, hour=entry.hour, minute=entry.minute)


@app.get("/promo/status", response_model=PromoStatusResponse)
async def promo_status(day: Optional[str] = None):
    if db_conn is None:
        raise HTTPException(status_code=500, detail="Database is not initialised.")
    target_day = day or _build_day_key()
    try:
        datetime.fromisoformat(target_day)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid day format")
    await ensure_promo_groups_synced()

    async with db_lock:
        history_rows = await asyncio.to_thread(_fetch_promo_history_day_sync, db_conn, target_day)
        schedule_rows = await asyncio.to_thread(_get_promo_schedule_sync, db_conn)
        summary_rows = await asyncio.to_thread(_fetch_promo_group_summary_sync, db_conn, target_day)

    slot_entries: Dict[str, List[PromoHistoryEntry]] = {}
    for row in history_rows:
        slot_entries.setdefault(row["slot"], []).append(
            PromoHistoryEntry(
                group_id=row["group_id"],
                group_title=row.get("group_title"),
                link=row["link"],
                status=row["status"],
                sent_at=_iso_to_kyiv_str(row.get("sent_at")),
                message_id=row.get("message_id"),
                message_text=row.get("message_text"),
                details=row.get("details"),
            )
        )

    plan = _build_schedule_plan_for_day(target_day, schedule_rows)
    current_slot = _determine_current_slot(plan)

    slots: List[PromoSlotStatus] = []
    total_sent = 0
    total_failed = 0

    for entry in plan:
        slot = entry["slot"]
        entries = slot_entries.get(slot, [])
        slots.append(
            PromoSlotStatus(
                slot=slot,
                scheduled_for=entry["scheduled_display"],
                entries=entries,
            )
        )
        total_sent += sum(1 for entry in entries if entry.status == "sent")
        total_failed += sum(1 for entry in entries if entry.status != "sent")

    group_summary = [
        PromoGroupSummary(
            group_id=row["group_id"],
            title=row.get("title"),
            link=row["link"],
            sent=row.get("sent", 0),
            failed=row.get("failed", 0),
        )
        for row in summary_rows
    ]

    return PromoStatusResponse(
        day=target_day,
        slots=slots,
        group_summary=group_summary,
        total_sent=total_sent,
        total_failed=total_failed,
        is_paused=promo_paused,
        current_slot=current_slot,
    )


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
