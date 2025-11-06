# scraper_service.py
import os
from typing import List, Optional

from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from telethon import TelegramClient
from telethon.errors import RPCError

load_dotenv()

API_ID = int(os.getenv("API_ID"))
API_HASH = os.getenv("API_HASH")

# используем ту же сессию, что и в scraper_login.py
client = TelegramClient("scraper_session", API_ID, API_HASH)

app = FastAPI(title="TG Scraper API")


class ScrapeRequest(BaseModel):
    chat: str          # @groupname или ссылка t.me/...
    limit: Optional[int] = 200  # максимум, сколько участников тянуть


class Member(BaseModel):
    id: int
    username: Optional[str]
    first_name: Optional[str]
    last_name: Optional[str]


class ScrapeResponse(BaseModel):
    total: int
    members: List[Member]


@app.on_event("startup")
async def on_startup():
    # подключаем telethon-клиента при старте сервиса
    await client.connect()
    if not await client.is_user_authorized():
        # если не авторизован, значит не запускали scraper_login.py
        raise RuntimeError("Userbot не авторизован. Сначала запусти scraper_login.py")


@app.on_event("shutdown")
async def on_shutdown():
    await client.disconnect()


@app.post("/scrape", response_model=ScrapeResponse)
async def scrape(req: ScrapeRequest):
    chat = req.chat.strip()
    limit = req.limit or 200

    members: List[Member] = []

    try:
        # Telethon сам поймёт: это @username или t.me/...
        async for user in client.iter_participants(chat, limit=limit):
            members.append(
                Member(
                    id=user.id,
                    username=user.username,
                    first_name=user.first_name,
                    last_name=user.last_name,
                )
            )
    except RPCError as e:
        raise HTTPException(status_code=400, detail=f"Telegram RPC error: {e}")
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Error: {e}")

    return ScrapeResponse(total=len(members), members=members)
