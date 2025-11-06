# scraper_login.py
import os
from dotenv import load_dotenv
from telethon import TelegramClient

load_dotenv()

API_ID = int(os.getenv("API_ID"))
API_HASH = os.getenv("API_HASH")
SCRAPER_PHONE = os.getenv("SCRAPER_PHONE")

# файл сессии будет храниться как scraper_session.session
client = TelegramClient("scraper_session", API_ID, API_HASH)


async def main():
    # просто стартуем клиента - он сам спросит код
    print("Запускаю авторизацию userbot'а...")
    await client.start(phone=SCRAPER_PHONE)
    me = await client.get_me()
    print("Готово! Авторизован как:", me.id, me.username)


if __name__ == "__main__":
    with client:
        client.loop.run_until_complete(main())
