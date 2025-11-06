# bot.py
import os
import io
import csv
import logging
from typing import Dict

import requests
from aiogram import Bot, Dispatcher, executor, types
from dotenv import load_dotenv

load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN")
SCRAPER_API_URL = os.getenv("SCRAPER_API_URL", "http://127.0.0.1:8000")

logging.basicConfig(level=logging.INFO)

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher(bot)

# Простое хранение "состояния" в памяти: кто сейчас вводит ссылку для скрапа
user_states: Dict[int, str] = {}  # user_id -> "waiting_for_chat"


@dp.message_handler(commands=["start"])
async def cmd_start(message: types.Message):
    text = (
        "Привет! 👋\n\n"
        "Я бот для скрапа участников из групп/каналов.\n\n"
        "Команды:\n"
        "/scrape – собрать участников из группы/канала и получить CSV.\n\n"
        "Когда нажмёшь /scrape, я попрошу ссылку или @юзернейм чата."
    )
    await message.answer(text)


@dp.message_handler(commands=["scrape"])
async def cmd_scrape(message: types.Message):
    user_id = message.from_user.id
    user_states[user_id] = "waiting_for_chat"

    text = (
        "Ок 👍\n\n"
        "Теперь пришли мне ссылку или @юзернейм группы/канала.\n"
        "Например:\n"
        "`https://t.me/testgroup`\n"
        "или\n"
        "`@testgroup`\n\n"
        "Я соберу участников (по умолчанию до 200) и пришлю CSV-файл."
    )
    await message.answer(text, parse_mode="Markdown")


@dp.message_handler(content_types=types.ContentTypes.TEXT)
async def handle_text(message: types.Message):
    user_id = message.from_user.id
    state = user_states.get(user_id)

    # если мы ждем от этого юзера ссылку для скрапа
    if state == "waiting_for_chat":
        chat_ref = message.text.strip()

        # сбрасываем состояние
        user_states[user_id] = ""

        await message.answer("Пробую скрапнуть участников, подожди немного... ⏳")

        # запрос к API скрапера
        try:
            resp = requests.post(
                f"{SCRAPER_API_URL}/scrape",
                json={"chat": chat_ref, "limit": 200},
                timeout=60,
            )
        except Exception as e:
            await message.answer(f"Не смог достучаться до API скрапера: {e}")
            return

        if resp.status_code != 200:
            await message.answer(
                f"Ошибка от скрапера ({resp.status_code}): {resp.text}"
            )
            return

        data = resp.json()
        total = data.get("total", 0)
        members = data.get("members", [])

        if total == 0:
            await message.answer("Никого не нашёл 😕 Проверь ссылку/юзернейм.")
            return

        # создаём CSV в памяти
        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerow(["id", "username", "first_name", "last_name"])

        for m in members:
            writer.writerow(
                [
                    m.get("id"),
                    m.get("username") or "",
                    m.get("first_name") or "",
                    m.get("last_name") or "",
                ]
            )

        csv_bytes = io.BytesIO(output.getvalue().encode("utf-8"))
        csv_bytes.name = "members.csv"

        text = (
            f"Готово ✅\n"
            f"Чат: `{chat_ref}`\n"
            f"Найдено участников: *{total}*.\n\n"
            f"Вот файл CSV:"
        )

        await message.answer_document(
            types.InputFile(csv_bytes),
            caption=text,
            parse_mode="Markdown",
        )

    else:
        # если не в режиме скрапа — просто подсказываем команды
        await message.answer("Если хочешь собрать участников – нажми /scrape 🙂")


if __name__ == "__main__":
    executor.start_polling(dp, skip_updates=True)
