# bot.py
import asyncio
import os
import io
import logging
import uuid
from typing import Dict, Any, List, Tuple

import requests
from aiogram import Bot, Dispatcher, executor, types
from aiogram.utils.exceptions import MessageNotModified
from dotenv import load_dotenv

load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN")
SCRAPER_API_URL = os.getenv("SCRAPER_API_URL", "http://127.0.0.1:8000").rstrip("/")

logging.basicConfig(level=logging.INFO)

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher(bot)

# Простое хранение "состояния" в памяти: кто сейчас вводит ссылку для скрапа
user_states: Dict[int, str] = {}  # user_id -> "waiting_for_chat"
broadcast_states: Dict[int, Dict[str, Any]] = {}


async def api_request(method: str, endpoint: str, **kwargs):
    timeout = kwargs.pop("timeout", 30)
    url = f"{SCRAPER_API_URL}{endpoint}"

    def _do_request():
        return requests.request(method=method, url=url, timeout=timeout, **kwargs)

    return await asyncio.to_thread(_do_request)


async def api_json(method: str, endpoint: str, **kwargs):
    response = await api_request(method, endpoint, **kwargs)
    try:
        data = response.json()
    except ValueError:
        data = None
    return response, data


CALLBACK_PREFIX = "download:"
CLEAR_EXPORTS_CALLBACK = "clear_exports"
FULL_EXPORT_CALLBACK = "download_full"
STOP_BROADCAST_PREFIX = "stop_broadcast:"
BROADCAST_INFO_PREFIX = "broadcast_info:"
export_tokens: Dict[str, str] = {}

MAIN_KEYBOARD = types.ReplyKeyboardMarkup(resize_keyboard=True)
MAIN_KEYBOARD.row(
    types.KeyboardButton("/scrape"),
    types.KeyboardButton("/exports"),
)
MAIN_KEYBOARD.row(
    types.KeyboardButton("/broadcast"),
    types.KeyboardButton("Статистика по дням"),
)


def _format_log_entries(entries):
    if not entries:
        return "Нет записей."
    lines = []
    for entry in entries:
        username = entry.get("username") or "-"
        user_display = f"@{username}" if username not in ("-", None) else f"id:{entry.get('member_id')}"
        status = entry.get("status", "unknown")
        timestamp = entry.get("timestamp", "")
        lines.append(f"{user_display} — {status} ({timestamp})")
    return "\n".join(lines)


async def send_broadcast_stats_message(message: types.Message):
    try:
        response, data = await api_json(
            "get",
            "/broadcast_stats",
            params={"limit": 30},
            timeout=20,
        )
    except Exception as exc:
        await message.answer(
            f"Не удалось получить статистику рассылки: {exc}",
            reply_markup=MAIN_KEYBOARD,
        )
        return

    if response.status_code != 200 or not isinstance(data, list):
        await message.answer(
            f"Ошибка статистики ({response.status_code}): {response.text}",
            reply_markup=MAIN_KEYBOARD,
        )
        return

    if not data:
        await message.answer("Пока нет данных по рассылкам.", reply_markup=MAIN_KEYBOARD)
        return

    lines = [f"{row['date']}: {row['processed']} пользователей" for row in data if row.get("date")]
    await message.answer(
        "Статистика по дням:\n" + "\n".join(lines),
        reply_markup=MAIN_KEYBOARD,
    )


async def start_broadcast(message: types.Message, user_id: int, settings: Dict[str, Any]):
    text = settings.get("text", "").strip()
    limit = settings.get("limit")
    interval = settings.get("interval", 0.0)

    waiting_msg = await message.answer("Запускаю рассылку... ⏳")

    try:
        response, data = await api_json(
            "post",
            "/send_start",
            json={
                "text": text,
                "limit": limit,
                "interval_seconds": interval,
                "source_chat": settings.get("source_chat"),
                "chat_title": settings.get("chat_title"),
            },
            timeout=30,
        )
    except Exception as exc:
        await waiting_msg.edit_text(f"Не удалось запустить рассылку: {exc}")
        return

    if response.status_code != 202 or not isinstance(data, dict):
        await waiting_msg.edit_text(
            f"Ошибка запуска рассылки ({response.status_code}): {response.text}"
        )
        return

    job_id = data.get("job_id")
    if not job_id:
        await waiting_msg.edit_text("Сервис не вернул идентификатор рассылки.")
        return

    keyboard = types.InlineKeyboardMarkup(row_width=2)
    keyboard.add(
        types.InlineKeyboardButton(
            text="Последние 10",
            callback_data=f"{BROADCAST_INFO_PREFIX}{job_id}:0",
        ),
        types.InlineKeyboardButton(
            text="Остановить",
            callback_data=f"{STOP_BROADCAST_PREFIX}{job_id}",
        ),
    )

    progress_message = await waiting_msg.edit_text(
        f"Рассылка `{job_id}` запущена.\n"
        f"Лимит: {limit or 'все'} пользователей\n"
        f"Интервал: {interval} c.\n"
        f"Чат: {settings.get('chat_title') or settings.get('source_chat') or 'не указан'}",
        parse_mode="Markdown",
        reply_markup=keyboard,
    )

    await poll_broadcast_status(progress_message, job_id, keyboard)


async def poll_broadcast_status(
    progress_message: types.Message,
    job_id: str,
    keyboard: types.InlineKeyboardMarkup,
):
    while True:
        await asyncio.sleep(5)
        try:
            response, data = await api_json(
                "get",
                "/send_status",
                params={"job_id": job_id},
                timeout=20,
            )
        except Exception as exc:
            await progress_message.edit_text(f"Не удалось получить статус рассылки: {exc}")
            return

        if response.status_code == 404:
            await progress_message.edit_text("Рассылка не найдена или уже удалена.")
            return

        if response.status_code != 200 or not isinstance(data, dict):
            await progress_message.edit_text(
                f"Ошибка статуса рассылки ({response.status_code}): {response.text}"
            )
            return

        status = data.get("status")
        processed = data.get("processed", 0)
        total = data.get("total", 0)
        sent_success = data.get("sent_success", 0)
        sent_failed = data.get("sent_failed", 0)
        message_text = data.get("message") or ""

        status_text = (
            f"Рассылка `{job_id}` — *{status}*\n"
            f"Всего получателей: {total}\n"
            f"Обработано: {processed}\n"
            f"Успешно: {sent_success}\n"
            f"С ошибкой: {sent_failed}"
        )
        if message_text:
            status_text += f"\n\n{message_text}"

        reply_markup = keyboard if status == "running" else None
        if status in {"done", "error", "cancelled"}:
            reply_markup = types.InlineKeyboardMarkup().add(
                types.InlineKeyboardButton(
                    text="Последние 10",
                    callback_data=f"{BROADCAST_INFO_PREFIX}{job_id}:0",
                )
            )

        try:
            await progress_message.edit_text(
                status_text,
                parse_mode="Markdown",
                reply_markup=reply_markup,
            )
        except MessageNotModified:
            pass

        if status in {"done", "error", "cancelled"}:
            return


@dp.message_handler(commands=["start"])
async def cmd_start(message: types.Message):
    text = (
        "Привет! 👋\n\n"
        "Я бот для скрапа участников из групп/каналов.\n\n"
        "Команды:\n"
        "/scrape – создать новую задачу на сбор участников и получить CSV после завершения.\n"
        "/exports – список всех готовых выгрузок.\n"
        "/broadcast – массовая рассылка по собранным пользователям.\n\n"
        "Когда нажмёшь /scrape, я попрошу ссылку или @юзернейм чата."
    )
    await message.answer(text, reply_markup=MAIN_KEYBOARD)


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
        "Я запущу задачу на сбор всех доступных участников и пришлю CSV, когда она завершится."
    )
    await message.answer(text, parse_mode="Markdown", reply_markup=MAIN_KEYBOARD)


@dp.message_handler(commands=["exports"])
async def cmd_exports(message: types.Message):
    try:
        response, data = await api_json("get", "/scrape_exports", timeout=20)
    except Exception as exc:
        await message.answer(f"Не удалось получить список выгрузок: {exc}", reply_markup=MAIN_KEYBOARD)
        return

    if response.status_code != 200 or not isinstance(data, list):
        await message.answer(
            f"Ошибка от сервиса экспорта ({response.status_code}): {response.text}",
            reply_markup=MAIN_KEYBOARD,
        )
        return

    if not data:
        await message.answer(
            "Готовых CSV пока нет. Создай новую задачу через /scrape.",
            reply_markup=MAIN_KEYBOARD,
        )
        return

    keyboard = types.InlineKeyboardMarkup(row_width=1)
    buttons_added = 0

    for export in data:
        filename = export.get("filename")
        if not filename:
            continue
        created_at = export.get("created_at")
        label = filename
        if created_at:
            label = f"{filename} ({created_at.replace('T', ' ')[:19]})"

        token = filename
        if len(f"{CALLBACK_PREFIX}{token}") > 64:
            token = uuid.uuid4().hex
        export_tokens[token] = filename

        keyboard.add(
            types.InlineKeyboardButton(
                text=label,
                callback_data=f"{CALLBACK_PREFIX}{token}",
            )
        )
        buttons_added += 1

    if buttons_added == 0:
        await message.answer(
            "Готовых CSV пока нет. Создай новую задачу через /scrape.",
            reply_markup=MAIN_KEYBOARD,
        )
        return

    keyboard.add(
        types.InlineKeyboardButton(
            text="Очистить список",
            callback_data=CLEAR_EXPORTS_CALLBACK,
        )
    )
    keyboard.add(
        types.InlineKeyboardButton(
            text="Скачать всю БД CSV",
            callback_data=FULL_EXPORT_CALLBACK,
        )
    )

    await message.answer("Выбери файл для скачивания:", reply_markup=keyboard)


@dp.message_handler(commands=["broadcast"])
async def cmd_broadcast(message: types.Message):
    user_id = message.from_user.id
    broadcast_states[user_id] = {"step": "waiting_chat"}

    try:
        response, data = await api_json("get", "/scrape_exports", timeout=20)
    except Exception as exc:
        await message.answer(f"Не удалось получить список экспортов: {exc}", reply_markup=MAIN_KEYBOARD)
        broadcast_states.pop(user_id, None)
        return

    if response.status_code != 200 or not isinstance(data, list):
        await message.answer(
            f"Ошибка при получении экспортов ({response.status_code}): {response.text}",
            reply_markup=MAIN_KEYBOARD,
        )
        broadcast_states.pop(user_id, None)
        return

    chats = []
    for export in data:
        filename = export.get("filename")
        if not filename:
            continue
        chat_title = export.get("chat_title") or filename
        source_chat = export.get("source_chat")
        chats.append({"filename": filename, "chat_title": chat_title, "source_chat": source_chat})

    if not chats:
        await message.answer("Нет доступных экспортов. Сначала собери участников через /scrape.", reply_markup=MAIN_KEYBOARD)
        broadcast_states.pop(user_id, None)
        return

    broadcast_states[user_id]["chats"] = chats
    broadcast_states[user_id]["chat_offset"] = 0

    await send_chat_selection(message, user_id)


async def send_chat_selection(target_message: types.Message, user_id: int):
    state = broadcast_states.get(user_id)
    if not state:
        return

    chats: List[Dict[str, Any]] = state.get("chats", [])
    offset = state.get("chat_offset", 0)
    page = chats[offset : offset + 5]

    keyboard = types.InlineKeyboardMarkup(row_width=1)
    for chat in page:
        title = chat.get("chat_title") or chat.get("filename")
        source_chat = chat.get("source_chat") or chat.get("chat_title")
        keyboard.add(
            types.InlineKeyboardButton(
                text=title,
                callback_data=f"broadcast_select:{source_chat}:{title}",
            )
        )

    nav_buttons = []
    if offset > 0:
        nav_buttons.append(
            types.InlineKeyboardButton("⟵ Назад", callback_data="broadcast_prev")
        )
    if offset + 5 < len(chats):
        nav_buttons.append(
            types.InlineKeyboardButton("Далее ⟶", callback_data="broadcast_next")
        )
    if nav_buttons:
        keyboard.row(*nav_buttons)

    keyboard.add(
        types.InlineKeyboardButton("Отмена", callback_data="broadcast_cancel")
    )

    await target_message.answer(
        "Выбери экспорт/чат для рассылки:", reply_markup=keyboard
    )


@dp.message_handler(commands=["stats"])
async def cmd_stats(message: types.Message):
    await send_broadcast_stats_message(message)


@dp.message_handler(lambda m: m.text and m.text.strip().lower() == "статистика по дням")
async def handle_stats_button_text(message: types.Message):
    await send_broadcast_stats_message(message)


@dp.message_handler(content_types=types.ContentTypes.TEXT)
async def handle_text(message: types.Message):
    user_id = message.from_user.id
    state = user_states.get(user_id)
    broadcast_state = broadcast_states.get(user_id)

    if broadcast_state:
        step = broadcast_state.get("step")
        if step == "waiting_chat":
            await message.answer("Сначала выбери чат из списка выше.", reply_markup=MAIN_KEYBOARD)
            return
        if step == "waiting_text":
            broadcast_state["text"] = message.text
            broadcast_state["step"] = "waiting_limit"
            await message.answer("Сколько пользователей обработать? Введите число или `all`.", parse_mode="Markdown")
            return
        if step == "waiting_limit":
            limit_text = message.text.strip().lower()
            if limit_text in ("all", "все"):
                broadcast_state["limit"] = None
            else:
                try:
                    limit_value = int(limit_text)
                    if limit_value <= 0:
                        raise ValueError
                    broadcast_state["limit"] = limit_value
                except ValueError:
                    await message.answer("Нужно указать положительное число или `all`.", parse_mode="Markdown")
                    return
            broadcast_state["step"] = "waiting_interval"
            await message.answer("Введите интервал между сообщениями в секундах (можно 0).")
            return
        if step == "waiting_interval":
            try:
                interval_value = float(message.text.strip().replace(",", "."))
                if interval_value < 0:
                    raise ValueError
            except ValueError:
                await message.answer("Интервал должен быть числом 0 или больше.")
                return
            broadcast_state["interval"] = interval_value
            await start_broadcast(message, user_id, broadcast_state)
            broadcast_states.pop(user_id, None)
            return

    # если мы ждем от этого юзера ссылку для скрапа
    if state == "waiting_for_chat":
        chat_ref = message.text.strip()

        # сбрасываем состояние
        user_states[user_id] = ""

        awaiting_msg = await message.answer("Создаю задачу на сбор участников... ⏳")

        try:
            response, data = await api_json(
                "post",
                "/scrape",
                json={"chat": chat_ref},
                timeout=20,
            )
        except Exception as exc:
            await awaiting_msg.edit_text(f"Не смог достучаться до API скрапера: {exc}")
            return

        if response.status_code != 202 or not isinstance(data, dict):
            await awaiting_msg.edit_text(
                f"Ошибка от скрапера ({response.status_code}): {response.text}"
            )
            return

        job_id = data.get("job_id")
        if not job_id:
            await awaiting_msg.edit_text("Скрапер не вернул идентификатор задачи 😕")
            return

        await awaiting_msg.edit_text(
            f"Задача `{job_id}` запущена.\n"
            f"Чат: `{chat_ref}`\n"
            "Буду проверять прогресс и пришлю CSV, как только всё будет готово.",
            parse_mode="Markdown",
        )

        progress_message = await message.answer("Жду обновлений от скрапера...")
        last_processed = -1
        last_total = -1

        status_data = None
        while True:
            try:
                status_response, status_data = await api_json(
                    "get",
                    "/scrape_status",
                    params={"job_id": job_id},
                    timeout=20,
                )
            except Exception as exc:
                await progress_message.edit_text(f"Ошибка при проверке статуса: {exc}")
                return

            if status_response.status_code == 404:
                await progress_message.edit_text("Задача не найдена. Попробуй ещё раз.")
                return

            if status_response.status_code != 200 or not isinstance(status_data, dict):
                await progress_message.edit_text(
                    f"Скрапер вернул ошибку ({status_response.status_code}): {status_response.text}"
                )
                return

            status = status_data.get("status")
            processed = status_data.get("processed", 0)
            total = status_data.get("total", 0)

            if status == "running":
                if processed != last_processed or total != last_total:
                    progress_text = (
                        f"Задача `{job_id}` выполняется…\n"
                        f"Обработано записей: {processed}\n"
                        f"Уникальных участников в базе: {total}"
                    )
                    try:
                        await progress_message.edit_text(
                            progress_text,
                            parse_mode="Markdown",
                        )
                    except MessageNotModified:
                        pass
                    last_processed = processed
                    last_total = total
                await asyncio.sleep(5)
                continue

            if status == "error":
                error_text = status_data.get("error") or "Неизвестная ошибка"
                await progress_message.edit_text(
                    f"Задача `{job_id}` завершилась с ошибкой:\n{error_text}",
                    parse_mode="Markdown",
                )
                return

            if status == "done":
                total = status_data.get("total", total)
                try:
                    await progress_message.edit_text(
                        f"Задача `{job_id}` завершилась. Формирую CSV...",
                        parse_mode="Markdown",
                    )
                except MessageNotModified:
                    pass
                break

            await progress_message.edit_text(
                f"Неожиданный статус задачи: {status}"
            )
            return

        try:
            csv_response = await api_request(
                "get",
                "/scrape_result",
                params={"job_id": job_id},
                timeout=120,
            )
        except Exception as exc:
            await progress_message.edit_text(f"Не удалось получить CSV: {exc}")
            return

        if csv_response.status_code != 200:
            await progress_message.edit_text(
                f"Скрапер не смог отдать CSV ({csv_response.status_code}): {csv_response.text}"
            )
            return

        filename = status_data.get("csv_path")
        if filename:
            filename = os.path.basename(filename)
        else:
            filename = f"members_{job_id}.csv"

        csv_bytes = io.BytesIO(csv_response.content)
        csv_bytes.name = filename

        processed_count = status_data.get("processed", processed)
        caption = (
            f"Готово ✅\n"
            f"Чат: `{chat_ref}`\n"
            f"Получено записей: *{processed_count}*\n"
            f"Уникальных участников: *{total}*.\n\n"
            "Файл добавлен в общий список /exports."
        )

        await message.answer_document(
            types.InputFile(csv_bytes),
            caption=caption,
            parse_mode="Markdown",
        )

        try:
            await progress_message.edit_text(
                f"Задача `{job_id}` завершена, CSV отправлен ✅",
                parse_mode="Markdown",
            )
        except MessageNotModified:
            pass

    else:
        # если не в режиме скрапа — просто подсказываем команды
        await message.answer("Если хочешь собрать участников – нажми /scrape 🙂", reply_markup=MAIN_KEYBOARD)


@dp.callback_query_handler(lambda c: c.data and c.data.startswith(CALLBACK_PREFIX))
async def handle_export_download(callback_query: types.CallbackQuery):
    token = callback_query.data[len(CALLBACK_PREFIX) :]
    filename = export_tokens.get(token, token)

    await callback_query.answer("Готовлю файл…")

    try:
        response = await api_request(
            "get",
            f"/scrape_export/{filename}",
            timeout=120,
        )
    except Exception as exc:
        await callback_query.message.answer(f"Не удалось скачать файл: {exc}")
        return

    if response.status_code != 200:
        await callback_query.message.answer(
            f"Ошибка при скачивании ({response.status_code}): {response.text}"
        )
        return

    csv_bytes = io.BytesIO(response.content)
    csv_bytes.name = filename

    await bot.send_document(
        callback_query.from_user.id,
        types.InputFile(csv_bytes),
        caption=f"Экспорт {filename}",
    )


@dp.callback_query_handler(lambda c: c.data == CLEAR_EXPORTS_CALLBACK)
async def handle_clear_exports(callback_query: types.CallbackQuery):
    await callback_query.answer("Очищаю список…")

    try:
        response, data = await api_json("post", "/scrape_exports/clear", timeout=60)
    except Exception as exc:
        await callback_query.message.answer(f"Не удалось очистить список: {exc}")
        return

    if response.status_code != 200:
        await callback_query.message.answer(
            f"Ошибка при очистке ({response.status_code}): {response.text}"
        )
        return

    export_tokens.clear()

    deleted = (data or {}).get("deleted", 0) if isinstance(data, dict) else 0
    await callback_query.message.edit_text(
        f"Список экспортов очищен. Удалено файлов: {deleted}."
    )


@dp.callback_query_handler(lambda c: c.data == FULL_EXPORT_CALLBACK)
async def handle_full_export(callback_query: types.CallbackQuery):
    await callback_query.answer("Готовлю полный экспорт…")

    try:
        response = await api_request("get", "/scrape_export/full", timeout=180)
    except Exception as exc:
        await callback_query.message.answer(f"Не удалось получить полный экспорт: {exc}")
        return

    if response.status_code != 200:
        await callback_query.message.answer(
            f"Ошибка при экспорте ({response.status_code}): {response.text}"
        )
        return

    filename = "members_full.csv"
    disposition = response.headers.get("Content-Disposition")
    if disposition and "filename=" in disposition:
        filename = disposition.split("filename=")[-1].strip('";')

    csv_bytes = io.BytesIO(response.content)
    csv_bytes.name = filename

    await bot.send_document(
        callback_query.from_user.id,
        types.InputFile(csv_bytes),
        caption="Полный экспорт всех участников.",
    )


@dp.callback_query_handler(lambda c: c.data == "broadcast_prev")
async def handle_broadcast_prev(callback_query: types.CallbackQuery):
    user_id = callback_query.from_user.id
    state = broadcast_states.get(user_id)
    if not state:
        await callback_query.answer("Сессия рассылки не найдена.", show_alert=True)
        return
    offset = max(0, state.get("chat_offset", 0) - 5)
    state["chat_offset"] = offset
    await callback_query.message.delete()
    await send_chat_selection(callback_query.message, user_id)


@dp.callback_query_handler(lambda c: c.data == "broadcast_next")
async def handle_broadcast_next(callback_query: types.CallbackQuery):
    user_id = callback_query.from_user.id
    state = broadcast_states.get(user_id)
    if not state:
        await callback_query.answer("Сессия рассылки не найдена.", show_alert=True)
        return
    offset = state.get("chat_offset", 0) + 5
    if offset >= len(state.get("chats", [])):
        offset = state.get("chat_offset", 0)
    state["chat_offset"] = offset
    await callback_query.message.delete()
    await send_chat_selection(callback_query.message, user_id)


@dp.callback_query_handler(lambda c: c.data == "broadcast_cancel")
async def handle_broadcast_cancel(callback_query: types.CallbackQuery):
    user_id = callback_query.from_user.id
    broadcast_states.pop(user_id, None)
    await callback_query.answer("Рассылка отменена.")
    await callback_query.message.edit_text("Выбор рассылки отменён.", reply_markup=None)


@dp.callback_query_handler(lambda c: c.data and c.data.startswith("broadcast_select:"))
async def handle_broadcast_select(callback_query: types.CallbackQuery):
    try:
        _, source_chat, chat_title = callback_query.data.split(":", 2)
    except ValueError:
        await callback_query.answer("Некорректный выбор.", show_alert=True)
        return

    user_id = callback_query.from_user.id
    state = broadcast_states.get(user_id)
    if not state:
        await callback_query.answer("Сессия рассылки не найдена.", show_alert=True)
        return

    state["source_chat"] = source_chat
    state["chat_title"] = chat_title
    state["step"] = "waiting_text"

    await callback_query.message.edit_text(
        f"Выбран чат: {chat_title}\n\nТеперь введите текст рассылки:",
        reply_markup=None,
    )


@dp.callback_query_handler(lambda c: c.data and c.data.startswith(STOP_BROADCAST_PREFIX))
async def handle_stop_broadcast(callback_query: types.CallbackQuery):
    job_id = callback_query.data[len(STOP_BROADCAST_PREFIX) :]
    await callback_query.answer("Останавливаю рассылку...")

    try:
        response, data = await api_json(
            "post",
            "/send_stop",
            params={"job_id": job_id},
            timeout=20,
        )
    except Exception as exc:
        await callback_query.message.answer(f"Не удалось остановить рассылку: {exc}")
        return

    if response.status_code == 404:
        await callback_query.message.answer("Рассылка уже завершена или не найдена.")
        return

    if response.status_code != 200:
        await callback_query.message.answer(
            f"Ошибка остановки ({response.status_code}): {response.text}"
        )
        return

    status_msg = (data or {}).get("status", "unknown") if isinstance(data, dict) else "unknown"
    await callback_query.message.answer(f"Статус остановки для `{job_id}`: {status_msg}", parse_mode="Markdown")


@dp.callback_query_handler(lambda c: c.data and c.data.startswith(BROADCAST_INFO_PREFIX))
async def handle_broadcast_info(callback_query: types.CallbackQuery):
    payload = callback_query.data[len(BROADCAST_INFO_PREFIX) :]
    if ":" in payload:
        job_id, offset_raw = payload.split(":", 1)
        try:
            offset = int(offset_raw)
        except ValueError:
            offset = 0
    else:
        job_id = payload
        offset = 0

    try:
        response, data = await api_json(
            "get",
            "/send_log",
            params={"job_id": job_id, "offset": offset, "limit": 10},
            timeout=20,
        )
    except Exception as exc:
        await callback_query.message.answer(f"Не удалось получить лог рассылки: {exc}")
        return

    if response.status_code != 200 or not isinstance(data, dict):
        await callback_query.message.answer(
            f"Ошибка логов ({response.status_code}): {response.text}"
        )
        return

    entries = data.get("entries", [])
    text = _format_log_entries(entries)

    reply_markup = None
    next_offset = data.get("next_offset")
    if data.get("has_more") and next_offset is not None:
        reply_markup = types.InlineKeyboardMarkup().add(
            types.InlineKeyboardButton(
                text="Показать ещё",
                callback_data=f"{BROADCAST_INFO_PREFIX}{job_id}:{next_offset}",
            )
        )

    await callback_query.message.answer(
        f"Последние записи рассылки `{job_id}`:\n{text}",
        parse_mode="Markdown",
        reply_markup=reply_markup,
    )
    await callback_query.answer()




if __name__ == "__main__":
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    executor.start_polling(dp, skip_updates=True)
