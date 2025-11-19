# bot.py
import asyncio
import os
import io
import logging
import uuid
import html
from typing import Dict, Any, List, Tuple, Optional

import requests
from aiogram import Bot, Dispatcher, executor, types
from aiogram.utils.exceptions import MessageNotModified, MessageToDeleteNotFound
from dotenv import load_dotenv

load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN")
SCRAPER_API_URL = os.getenv("SCRAPER_API_URL", "http://127.0.0.1:8000").rstrip("/")
PROMO_FOLDER_NAME = os.getenv("PROMO_FOLDER_NAME", "Бесплатно PR").strip()

logging.basicConfig(level=logging.INFO)

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher(bot)

# Простое хранение "состояния" в памяти: кто сейчас вводит ссылку для скрапа
user_states: Dict[int, str] = {}  # user_id -> "waiting_for_chat"
broadcast_states: Dict[int, Dict[str, Any]] = {}
promo_states: Dict[int, Dict[str, Any]] = {}
dialog_states: Dict[int, Dict[str, Any]] = {}


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
PROMO_MENU_CALLBACK = "promo_menu"
PROMO_GROUPS_CALLBACK = "promo_groups"
PROMO_MESSAGES_CALLBACK = "promo_messages"
PROMO_MESSAGE_ADD_CALLBACK = "promo_message_add"
PROMO_MESSAGE_DELETE_PREFIX = "promo_message_del:"
PROMO_SCHEDULE_CALLBACK = "promo_schedule"
PROMO_SCHEDULE_EDIT_PREFIX = "promo_schedule_edit:"
PROMO_STATUS_CALLBACK = "promo_status"
PROMO_SLOTS_CALLBACK = "promo_slots"
PROMO_SUMMARY_CALLBACK = "promo_summary"
PROMO_START_CALLBACK = "promo_start"
PROMO_STOP_CALLBACK = "promo_stop"
PROMO_CLOSE_CALLBACK = "promo_close"
PROMO_SLOT_LABELS = {
    "morning": "Утро",
    "noon": "Обед",
    "evening": "Вечер",
}
PROMO_SLOT_EMOJI = {
    "morning": "🌅",
    "noon": "🌤️",
    "evening": "🌙",
}
DIALOGS_MENU_COMMAND = "dialogs_menu"
DIALOGS_PAGE_PREFIX = "dlgpage:"
DIALOG_SELECT_PREFIX = "dlgsel:"
DIALOG_REFRESH_PREFIX = "dlgref:"
DIALOG_VIEW_REFRESH_PREFIX = "dlgview"
DIALOG_MORE_PREFIX = "dlgmore:"
DIALOG_BACK_CALLBACK = "dlgback"
DIALOG_COMPOSE_CALLBACK = "dlgcompose"
DIALOG_HELP_CALLBACK = "dlggpt"
DIALOG_SEND_CONFIRM = "dlgsend"
DIALOG_SEND_CANCEL = "dlgcancel"
DIALOG_DRAFT_HELP = "dlghdraft"
DIALOG_SUGGEST_PREFIX = "dlgsugg:"
DIALOG_LIST_REFRESH = "dialogs_refresh"
export_tokens: Dict[str, str] = {}
current_scrape_job_id: Optional[str] = None

MAIN_KEYBOARD = types.ReplyKeyboardMarkup(resize_keyboard=True)
MAIN_KEYBOARD.row(
    types.KeyboardButton("Сбор"),
    types.KeyboardButton("Экспорты"),
)
MAIN_KEYBOARD.row(
    types.KeyboardButton("Рассылка"),
    types.KeyboardButton("Статистика по дням"),
)
MAIN_KEYBOARD.row(types.KeyboardButton("Реклама"), types.KeyboardButton("Диалоги"))

SCRAPE_KEYBOARD = types.ReplyKeyboardMarkup(resize_keyboard=True)
SCRAPE_KEYBOARD.row(types.KeyboardButton("/scrape"))
SCRAPE_KEYBOARD.row(types.KeyboardButton("Остановить сбор"), types.KeyboardButton("Назад"))

EXPORTS_KEYBOARD = types.ReplyKeyboardMarkup(resize_keyboard=True)
EXPORTS_KEYBOARD.row(types.KeyboardButton("/exports"))
EXPORTS_KEYBOARD.row(types.KeyboardButton("Назад"))

BROADCAST_KEYBOARD = types.ReplyKeyboardMarkup(resize_keyboard=True)
BROADCAST_KEYBOARD.row(types.KeyboardButton("/broadcast"))
BROADCAST_KEYBOARD.row(types.KeyboardButton("Назад"))

PROMO_KEYBOARD = types.ReplyKeyboardMarkup(resize_keyboard=True)
PROMO_KEYBOARD.row(types.KeyboardButton("/promo"))
PROMO_KEYBOARD.row(types.KeyboardButton("Назад"))


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


def _short_label(value: str, limit: int = 32) -> str:
    if len(value) <= limit:
        return value
    return value[: limit - 1] + "…"


def _parse_time_string(value: str) -> Optional[Tuple[int, int]]:
    cleaned = value.strip().replace(" ", "")
    if not cleaned:
        return None
    if ":" in cleaned:
        parts = cleaned.split(":", 1)
    elif "." in cleaned:
        parts = cleaned.split(".", 1)
    else:
        if len(cleaned) in {3, 4} and cleaned.isdigit():
            if len(cleaned) == 3:
                cleaned = "0" + cleaned
            parts = [cleaned[:2], cleaned[2:]]
        else:
            return None
    try:
        hour = int(parts[0])
        minute = int(parts[1])
    except (ValueError, IndexError):
        return None
    if 0 <= hour <= 23 and 0 <= minute <= 59:
        return hour, minute
    return None


def _safe_text(value: Optional[str]) -> str:
    return html.escape(value or "")


def _format_group_link(title: Optional[str], link: Optional[str]) -> str:
    display = title or link or "Без названия"
    safe_display = html.escape(display)
    if link:
        if link.startswith("https://"):
            safe_link = html.escape(link, quote=True)
        elif link.startswith("@"):
            safe_link = html.escape(f"https://t.me/{link.lstrip('@')}", quote=True)
        else:
            safe_link = None
        if safe_link:
            return f'<a href="{safe_link}">{safe_display}</a>'
    return safe_display


def _format_message_html(text: Optional[str]) -> str:
    if not text:
        return "<i>[без текста]</i>"
    return html.escape(text)


async def _respond_with_markup(
    target_message: types.Message,
    text: str,
    reply_markup: Optional[types.InlineKeyboardMarkup] = None,
    *,
    edit: bool = False,
    parse_mode: Optional[str] = None,
):
    if edit:
        try:
            await target_message.edit_text(text, reply_markup=reply_markup, parse_mode=parse_mode)
        except MessageNotModified:
            pass
    else:
        await target_message.answer(text, reply_markup=reply_markup, parse_mode=parse_mode)


async def send_promo_menu_message(target_message: types.Message, *, edit: bool = False):
    keyboard = types.InlineKeyboardMarkup(row_width=2)
    keyboard.row(
        types.InlineKeyboardButton("Группы", callback_data=PROMO_GROUPS_CALLBACK),
        types.InlineKeyboardButton("Сообщения", callback_data=PROMO_MESSAGES_CALLBACK),
    )
    keyboard.row(
        types.InlineKeyboardButton("Расписание", callback_data=PROMO_SCHEDULE_CALLBACK),
        types.InlineKeyboardButton("Статус сегодня", callback_data=PROMO_STATUS_CALLBACK),
    )
    keyboard.add(types.InlineKeyboardButton("Закрыть", callback_data=PROMO_CLOSE_CALLBACK))
    text = (
        "Меню рекламных рассылок:\n"
        f"• Группы — автоматически подгружаются из папки '{PROMO_FOLDER_NAME}'.\n"
        "• Сообщения — набор рекламных текстов для рандомного выбора.\n"
        "• Расписание — время отправки утром/днём/вечером."
    )
    await _respond_with_markup(target_message, text, keyboard, edit=edit)


async def send_promo_groups_view(target_message: types.Message, *, edit: bool = False):
    try:
        response, data = await api_json("get", "/promo/groups", timeout=20)
    except Exception as exc:
        await target_message.answer(f"Не удалось получить группы: {exc}")
        return

    if response.status_code != 200 or not isinstance(data, list):
        await target_message.answer(
            f"Ошибка при получении групп ({response.status_code}): {response.text}"
        )
        return

    folder_label = html.escape(PROMO_FOLDER_NAME or "папки")
    header = (
        f"Группы берутся автоматически из папки '{folder_label}'.\n"
        "Добавь нужные чаты в эту папку в Telegram, и бот подхватит их сам."
    )
    if not data:
        text = header + "\n\nПапка пока пуста."
    else:
        lines = [header, "", "Список групп:"]
        for group in data:
            title = group.get("title") or group.get("link")
            link_value = group.get("link")
            status = html.escape(group.get("last_status") or "—")
            lines.append(
                f"#{group['id']}: {_format_group_link(title, link_value)} (последний статус: {status})"
            )
        text = "\n".join(lines)

    keyboard = types.InlineKeyboardMarkup(row_width=1)
    keyboard.add(types.InlineKeyboardButton("🔄 Обновить", callback_data=PROMO_GROUPS_CALLBACK))
    keyboard.add(types.InlineKeyboardButton("⬅️ Назад", callback_data=PROMO_MENU_CALLBACK))

    await _respond_with_markup(target_message, text, keyboard, edit=edit, parse_mode="HTML")


async def send_promo_messages_view(target_message: types.Message, *, edit: bool = False):
    try:
        response, data = await api_json("get", "/promo/messages", timeout=20)
    except Exception as exc:
        await target_message.answer(f"Не удалось получить сообщения: {exc}")
        return

    if response.status_code != 200 or not isinstance(data, list):
        await target_message.answer(
            f"Ошибка при получении сообщений ({response.status_code}): {response.text}"
        )
        return

    if not data:
        text = "Сообщения для рекламы ещё не добавлены."
    else:
        lines = ["Сохранённые сообщения:"]
        for item in data:
            preview = _short_label(item.get("text") or "", 80)
            lines.append(f"#{item['id']}: {preview}")
        text = "\n".join(lines)

    keyboard = types.InlineKeyboardMarkup(row_width=1)
    keyboard.add(types.InlineKeyboardButton("➕ Добавить сообщение", callback_data=PROMO_MESSAGE_ADD_CALLBACK))
    for item in data[:10]:
        preview = _short_label(item.get("text") or "", 20)
        keyboard.add(
            types.InlineKeyboardButton(
                f"Удалить #{item['id']}",
                callback_data=f"{PROMO_MESSAGE_DELETE_PREFIX}{item['id']}",
            )
        )
    keyboard.add(types.InlineKeyboardButton("⬅️ Назад", callback_data=PROMO_MENU_CALLBACK))

    await _respond_with_markup(target_message, text, keyboard, edit=edit)


async def send_promo_schedule_view(target_message: types.Message, *, edit: bool = False):
    try:
        response, data = await api_json("get", "/promo/schedule", timeout=20)
    except Exception as exc:
        await target_message.answer(f"Не удалось получить расписание: {exc}")
        return

    if response.status_code != 200 or not isinstance(data, list):
        await target_message.answer(
            f"Ошибка при получении расписания ({response.status_code}): {response.text}"
        )
        return

    lines = ["Текущее расписание (время Киев):"]
    for entry in data:
        label = PROMO_SLOT_LABELS.get(entry["slot"], entry["slot"])
        lines.append(f"• {label}: {entry['hour']:02d}:{entry['minute']:02d}")
    text = "\n".join(lines)

    keyboard = types.InlineKeyboardMarkup(row_width=1)
    for entry in data:
        label = PROMO_SLOT_LABELS.get(entry["slot"], entry["slot"])
        keyboard.add(
            types.InlineKeyboardButton(
                f"Изменить {label}",
                callback_data=f"{PROMO_SCHEDULE_EDIT_PREFIX}{entry['slot']}",
            )
        )
    keyboard.add(types.InlineKeyboardButton("⬅️ Назад", callback_data=PROMO_MENU_CALLBACK))

    await _respond_with_markup(target_message, text, keyboard, edit=edit)


async def send_promo_status_view(target_message: types.Message, *, edit: bool = False):
    try:
        response, data = await api_json("get", "/promo/status", timeout=20)
    except Exception as exc:
        await target_message.answer(f"Не удалось получить статус: {exc}")
        return

    if response.status_code != 200 or not isinstance(data, dict):
        await target_message.answer(
            f"Ошибка при получении статуса ({response.status_code}): {response.text}"
        )
        return

    slots = data.get("slots", [])
    group_summary = data.get("group_summary", [])
    is_paused = bool(data.get("is_paused"))
    current_slot = data.get("current_slot")
    lines = [
        f"Статус за {_safe_text(data.get('day'))}",
        "Автоматическая рассылка: "
        + ("остановлена" if is_paused else "активна"),
        f"Отправлено: {data.get('total_sent', 0)}, с ошибкой: {data.get('total_failed', 0)}",
        "",
        "Текущий слот:",
    ]
    slot_blocks: Dict[str, str] = {}
    for slot in slots:
        slot_code = slot.get("slot")
        label = PROMO_SLOT_LABELS.get(slot_code, slot_code)
        emoji = PROMO_SLOT_EMOJI.get(slot_code, "")
        slot_lines = [
            f"{emoji} {html.escape(label)} — {_safe_text(slot.get('scheduled_for'))}"
        ]
        entries = slot.get("entries") or []
        if not entries:
            slot_lines.append("   ещё не отправлено")
        else:
            for entry in entries:
                group_title = entry.get("group_title") or entry.get("link")
                slot_lines.append(f"{emoji} {_format_group_link(group_title, entry.get('link'))}")
                sent_time = entry.get("sent_at") or "—"
                status = entry.get("status") or "unknown"
                status_icon = "✅" if status == "sent" else "⚠️"
                msg_id = entry.get("message_id")
                slot_lines.append(f"   Время (Киев): {_safe_text(sent_time)}")
                slot_lines.append(f"   Статус: {status_icon} {html.escape(status)}")
                msg_label = msg_id if msg_id else "?"
                if status == "sent":
                    slot_lines.append(f"   Отправлено сообщение #{msg_label}.")
                else:
                    slot_lines.append(f"   Попытка сообщения #{msg_label}.")
                details = entry.get("details")
                if details and status != "sent":
                    slot_lines.append(f"   Детали: {_safe_text(details)}")
                slot_lines.append("")
        slot_blocks[slot_code] = "\n".join(slot_lines).strip()

    current_block = None
    if current_slot and current_slot in slot_blocks:
        current_block = slot_blocks[current_slot]
    elif slot_blocks:
        current_block = next(iter(slot_blocks.values()))
    else:
        current_block = "Нет доступных слотов"
    lines.append(current_block)
    text = "\n".join(lines).strip()

    keyboard = types.InlineKeyboardMarkup(row_width=1)
    control_buttons = [
        types.InlineKeyboardButton("▶️ Старт", callback_data=PROMO_START_CALLBACK),
        types.InlineKeyboardButton("⏹ Стоп", callback_data=PROMO_STOP_CALLBACK),
    ]
    keyboard.row(*control_buttons)
    if len(slot_blocks) > 1:
        keyboard.add(types.InlineKeyboardButton("Показать другие слоты", callback_data=PROMO_SLOTS_CALLBACK))
    keyboard.add(types.InlineKeyboardButton("Итог по группам", callback_data=PROMO_SUMMARY_CALLBACK))
    keyboard.add(types.InlineKeyboardButton("Обновить", callback_data=PROMO_STATUS_CALLBACK))
    keyboard.add(types.InlineKeyboardButton("⬅️ Назад", callback_data=PROMO_MENU_CALLBACK))

    await _respond_with_markup(target_message, text, keyboard, edit=edit, parse_mode="HTML")


async def send_promo_summary_view(target_message: types.Message, *, edit: bool = False):
    try:
        response, data = await api_json("get", "/promo/status", timeout=20)
    except Exception as exc:
        await target_message.answer(f"Не удалось получить итог: {exc}")
        return

    if response.status_code != 200 or not isinstance(data, dict):
        await target_message.answer(
            f"Ошибка при получении итога ({response.status_code}): {response.text}"
        )
        return

    group_summary = data.get("group_summary", [])
    lines = [f"Итог по группам за {_safe_text(data.get('day'))}:"]
    if not group_summary:
        lines.append("— нет групп")
    else:
        for group in group_summary:
            title = group.get("title") or group.get("link")
            lines.append(
                f"• {_format_group_link(title, group.get('link'))}: {group.get('sent', 0)} успешно, {group.get('failed', 0)} с ошибкой"
            )
    text = "\n".join(lines)

    keyboard = types.InlineKeyboardMarkup(row_width=1)
    keyboard.add(types.InlineKeyboardButton("Назад", callback_data=PROMO_STATUS_CALLBACK))

    await _respond_with_markup(target_message, text, keyboard, edit=edit, parse_mode="HTML")


async def send_promo_slots_view(target_message: types.Message, *, edit: bool = False):
    try:
        response, data = await api_json("get", "/promo/status", timeout=20)
    except Exception as exc:
        await target_message.answer(f"Не удалось получить слоты: {exc}")
        return

    if response.status_code != 200 or not isinstance(data, dict):
        await target_message.answer(
            f"Ошибка при получении слотов ({response.status_code}): {response.text}"
        )
        return

    slots = data.get("slots", [])
    lines = ["Все слоты:"]
    for slot in slots:
        slot_code = slot.get("slot")
        label = PROMO_SLOT_LABELS.get(slot_code, slot_code)
        emoji = PROMO_SLOT_EMOJI.get(slot_code, "")
        lines.append(f"{emoji} {html.escape(label)} — {_safe_text(slot.get('scheduled_for'))}")
        entries = slot.get("entries") or []
        if not entries:
            lines.append("   ещё не отправлено")
            continue
        for entry in entries:
            group_name = entry.get("group_title") or entry.get("link")
            lines.append(f"{emoji} {_format_group_link(group_name, entry.get('link'))}")
            sent_time = entry.get("sent_at") or "—"
            status = entry.get("status") or "unknown"
            status_icon = "✅" if status == "sent" else "⚠️"
            msg_id = entry.get("message_id")
            lines.append(f"   Время (Киев): {_safe_text(sent_time)}")
            lines.append(f"   Статус: {status_icon} {html.escape(status)}")
            msg_label = msg_id if msg_id else "?"
            if status == "sent":
                lines.append(f"   Отправлено сообщение #{msg_label}.")
            else:
                lines.append(f"   Попытка сообщения #{msg_label}.")
            details = entry.get("details")
            if details and status != "sent":
                lines.append(f"   Детали: {_safe_text(details)}")
            lines.append("")
    text = "\n".join(lines).strip()

    keyboard = types.InlineKeyboardMarkup(row_width=1)
    keyboard.add(types.InlineKeyboardButton("Назад", callback_data=PROMO_STATUS_CALLBACK))

    await _respond_with_markup(target_message, text, keyboard, edit=edit, parse_mode="HTML")


async def send_dialogs_list_message(
    target_message: types.Message,
    user_id: int,
    page: int = 0,
    *,
    edit: bool = False,
):
    try:
        response, data = await api_json("get", "/dialogs", params={"page": page}, timeout=20)
    except Exception as exc:
        await target_message.answer(f"Не удалось получить диалоги: {exc}")
        return

    if response.status_code != 200 or not isinstance(data, dict):
        await target_message.answer(
            f"Ошибка списка диалогов ({response.status_code}): {response.text}"
        )
        return

    items = data.get("items", [])
    lines = [f"Диалоги (страница {page + 1}):"]
    if not items:
        lines.append("— нет личных переписок")
    else:
        for item in items:
            name_link = _format_group_link(item.get("name"), item.get("link"))
            username = item.get("username")
            username_text = f" (@{username})" if username else ""
            last_message = html.escape(item.get("last_message") or "")
            prefix = "📩 " if item.get("unread") else ""
            lines.append(f"{prefix}{name_link}{username_text}")
            if last_message:
                lines.append(f"<i>{last_message}</i>")
            lines.append("")
    text = "\n".join(lines).strip()

    keyboard = types.InlineKeyboardMarkup(row_width=1)
    for item in items:
        label = ("📩 " if item.get("unread") else "") + (item.get("name") or "Без названия")
        if item.get("username"):
            label += f" (@{item['username']})"
        callback_data = f"{DIALOG_SELECT_PREFIX}{item['peer_id']}:{page}"
        keyboard.add(types.InlineKeyboardButton(label[:60], callback_data=callback_data))

    nav_buttons = []
    if page > 0:
        nav_buttons.append(
            types.InlineKeyboardButton("⬅️", callback_data=f"{DIALOGS_PAGE_PREFIX}{page - 1}")
        )
    if data.get("has_more"):
        nav_buttons.append(
            types.InlineKeyboardButton("➡️", callback_data=f"{DIALOGS_PAGE_PREFIX}{page + 1}")
        )
    if nav_buttons:
        keyboard.row(*nav_buttons)

    keyboard.add(
        types.InlineKeyboardButton(
            "Обновить",
            callback_data=f"{DIALOG_REFRESH_PREFIX}{page}",
        )
    )

    dialog_state = dialog_states.get(user_id, {})
    dialog_state.update({"mode": "list", "page": page})
    dialog_states[user_id] = dialog_state

    await _respond_with_markup(target_message, text, keyboard, edit=edit, parse_mode="HTML")


async def send_dialog_view_message(
    target_message: types.Message,
    user_id: int,
    peer_id: int,
    *,
    offset_id: Optional[int] = None,
    edit: bool = False,
    notice: Optional[str] = None,
):
    params = {"offset_id": offset_id} if offset_id else {}
    try:
        response, data = await api_json(
            "get",
            f"/dialogs/{peer_id}/messages",
            params=params,
            timeout=20,
        )
    except Exception as exc:
        await target_message.answer(f"Не удалось получить сообщения: {exc}")
        return

    if response.status_code != 200 or not isinstance(data, dict):
        await target_message.answer(
            f"Ошибка сообщений ({response.status_code}): {response.text}"
        )
        return

    dialog_info = data.get("dialog") or {}
    dialog_name = dialog_info.get("name") or dialog_info.get("link")
    header = _format_group_link(dialog_name, dialog_info.get("link"))
    lines = [f"Диалог з {header}"]
    if notice:
        lines.extend(["", f"<b>{html.escape(notice)}</b>"])
    lines.append("")
    lines.append("Останні повідомлення:")

    messages = data.get("messages", [])
    if not messages:
        lines.append("— історія пуста")
    else:
        for item in reversed(messages):
            prefix = html.escape(item.get("sender") or ("Я" if item.get("is_outgoing") else "Кандидат"))
            text_html = _format_message_html(item.get("text"))
            lines.append(f"&gt; <b>{prefix}</b>: {text_html}")
    text = "\n".join(lines)

    keyboard = types.InlineKeyboardMarkup(row_width=2)
    keyboard.row(
        types.InlineKeyboardButton("Отправить", callback_data=f"{DIALOG_COMPOSE_CALLBACK}:{peer_id}"),
        types.InlineKeyboardButton("Помощь GPT", callback_data=f"{DIALOG_HELP_CALLBACK}:{peer_id}"),
    )
    keyboard.row(
        types.InlineKeyboardButton("Обновить", callback_data=f"{DIALOG_VIEW_REFRESH_PREFIX}:{peer_id}"),
        types.InlineKeyboardButton("⬅️ Назад", callback_data=DIALOG_BACK_CALLBACK),
    )
    if data.get("has_more") and data.get("next_offset"):
        keyboard.add(
            types.InlineKeyboardButton(
                "Показать ранее",
                callback_data=f"{DIALOG_MORE_PREFIX}{peer_id}:{data['next_offset']}",
            )
        )

    state = dialog_states.get(user_id, {})
    state.update(
        {
            "mode": "view",
            "peer_id": peer_id,
            "dialog_title": dialog_name,
            "dialog_link": dialog_info.get("link"),
            "page": state.get("page", 0),
            "next_offset": data.get("next_offset"),
            "draft": None,
            "suggestions": [],
        }
    )
    dialog_states[user_id] = state

    await _respond_with_markup(target_message, text, keyboard, edit=edit, parse_mode="HTML")


async def send_dialog_suggestions(user_id: int, peer_id: int, draft: Optional[str], reply_message: types.Message):
    payload = {"draft": draft}
    try:
        response, data = await api_json("post", f"/dialogs/{peer_id}/suggest", json=payload, timeout=60)
    except Exception as exc:
        await reply_message.answer(f"Не удалось получить подсказки: {exc}")
        return
    if response.status_code != 200 or not isinstance(data, dict):
        await reply_message.answer(
            f"Ошибка подсказок ({response.status_code}): {response.text}"
        )
        return

    suggestions = data.get("suggestions") or []
    if not suggestions:
        await reply_message.answer("GPT не вернул варианты.")
        return

    dialog_states.setdefault(user_id, {})["suggestions"] = suggestions
    keyboard = types.InlineKeyboardMarkup(row_width=3)
    for idx, _ in enumerate(suggestions):
        label = f"Вариант {idx + 1}"
        keyboard.add(
            types.InlineKeyboardButton(
                label,
                callback_data=f"{DIALOG_SUGGEST_PREFIX}{idx}:{peer_id}",
            )
        )
    keyboard.add(
        types.InlineKeyboardButton(
            "Отмена",
            callback_data=DIALOG_SEND_CANCEL,
        )
    )
    lines = ["Варианты ответов:"]
    for idx, suggestion in enumerate(suggestions, 1):
        lines.append(f"{idx}) {_format_message_html(suggestion)}")
    text = "\n".join(lines)
    await reply_message.answer(text, reply_markup=keyboard, parse_mode="HTML")


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
    source_chat = settings.get("source_chat")
    if not source_chat:
        await message.answer("Не выбран чат для рассылки. Запусти /broadcast заново.", reply_markup=MAIN_KEYBOARD)
        return
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
        chat_display = data.get("chat_title") or data.get("source_chat") or "не указан"

        status_text = (
            f"Рассылка `{job_id}` — *{status}*\n"
            f"Чат: {chat_display}\n"
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
        "/promo – реклама в выбранных группах по расписанию.\n\n"
        "Когда нажмёшь /scrape, я попрошу ссылку или @юзернейм чата."
    )
    await message.answer(text, reply_markup=MAIN_KEYBOARD)


@dp.message_handler(lambda m: m.text == "Сбор")
async def handle_main_scrape_menu(message: types.Message):
    await message.answer("Меню сбора:", reply_markup=SCRAPE_KEYBOARD)


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
    await message.answer(text, parse_mode="Markdown", reply_markup=SCRAPE_KEYBOARD)


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
    for idx, chat in enumerate(page, start=offset):
        title = chat.get("chat_title") or chat.get("filename")
        keyboard.add(
            types.InlineKeyboardButton(
                text=title,
                callback_data=f"broadcast_select:{idx}",
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


@dp.message_handler(lambda m: m.text == "Экспорты")
async def handle_main_exports_menu(message: types.Message):
    await message.answer("Меню экспортов:", reply_markup=EXPORTS_KEYBOARD)
    await cmd_exports(message)


@dp.message_handler(lambda m: m.text == "Рассылка")
async def handle_main_broadcast_menu(message: types.Message):
    await message.answer("Меню рассылок:", reply_markup=BROADCAST_KEYBOARD)
    await cmd_broadcast(message)


async def open_promo_menu(message: types.Message):
    user_id = message.from_user.id
    promo_states.pop(user_id, None)
    await message.answer("Меню рекламы:", reply_markup=PROMO_KEYBOARD)
    await send_promo_menu_message(message)


@dp.message_handler(commands=["promo"])
async def cmd_promo(message: types.Message):
    await open_promo_menu(message)


@dp.message_handler(lambda m: m.text == "Реклама")
async def handle_main_promo_menu(message: types.Message):
    await open_promo_menu(message)


async def open_dialogs_menu(message: types.Message):
    user_id = message.from_user.id
    await send_dialogs_list_message(message, user_id, page=0, edit=False)


@dp.message_handler(commands=["dialogs"])
async def cmd_dialogs(message: types.Message):
    await open_dialogs_menu(message)


@dp.message_handler(lambda m: m.text == "Диалоги")
async def handle_dialogs_button(message: types.Message):
    await open_dialogs_menu(message)


@dp.message_handler(lambda m: m.text == "Назад")
async def handle_back_to_main(message: types.Message):
    promo_states.pop(message.from_user.id, None)
    await message.answer("Возврат в главное меню.", reply_markup=MAIN_KEYBOARD)


@dp.message_handler(lambda m: m.text == "Остановить сбор")
async def handle_stop_scrape_text(message: types.Message):
    global current_scrape_job_id
    if not current_scrape_job_id:
        await message.answer("Сейчас нет активного сбора.", reply_markup=SCRAPE_KEYBOARD)
        return
    try:
        response, data = await api_json(
            "post",
            "/scrape_stop",
            params={"job_id": current_scrape_job_id},
            timeout=20,
        )
    except Exception as exc:
        await message.answer(f"Не удалось остановить сбор: {exc}", reply_markup=SCRAPE_KEYBOARD)
        return

    if response.status_code != 200 or not isinstance(data, dict):
        await message.answer(
            f"Ошибка остановки ({response.status_code}): {response.text}",
            reply_markup=SCRAPE_KEYBOARD,
        )
        return

    status = data.get("status", "unknown")
    await message.answer(f"Сбор {current_scrape_job_id} остановлен: {status}", reply_markup=SCRAPE_KEYBOARD)
    if status in {"cancelling", "cancelled"}:
        current_scrape_job_id = None


@dp.callback_query_handler(lambda c: c.data == PROMO_MENU_CALLBACK)
async def handle_promo_menu_callback(callback_query: types.CallbackQuery):
    await callback_query.answer()
    await send_promo_menu_message(callback_query.message, edit=True)


@dp.callback_query_handler(lambda c: c.data == PROMO_GROUPS_CALLBACK)
async def handle_promo_groups_callback(callback_query: types.CallbackQuery):
    await callback_query.answer()
    await send_promo_groups_view(callback_query.message, edit=True)


@dp.callback_query_handler(lambda c: c.data == PROMO_MESSAGES_CALLBACK)
async def handle_promo_messages_callback(callback_query: types.CallbackQuery):
    await callback_query.answer()
    await send_promo_messages_view(callback_query.message, edit=True)


@dp.callback_query_handler(lambda c: c.data == PROMO_SCHEDULE_CALLBACK)
async def handle_promo_schedule_callback(callback_query: types.CallbackQuery):
    await callback_query.answer()
    await send_promo_schedule_view(callback_query.message, edit=True)


@dp.callback_query_handler(lambda c: c.data == PROMO_STATUS_CALLBACK)
async def handle_promo_status_callback(callback_query: types.CallbackQuery):
    await callback_query.answer()
    await send_promo_status_view(callback_query.message, edit=True)


@dp.callback_query_handler(lambda c: c.data == PROMO_SUMMARY_CALLBACK)
async def handle_promo_summary_callback(callback_query: types.CallbackQuery):
    await callback_query.answer()
    await send_promo_summary_view(callback_query.message, edit=True)


@dp.callback_query_handler(lambda c: c.data == PROMO_SLOTS_CALLBACK)
async def handle_promo_slots_callback(callback_query: types.CallbackQuery):
    await callback_query.answer()
    await send_promo_slots_view(callback_query.message, edit=True)


@dp.callback_query_handler(lambda c: c.data == PROMO_START_CALLBACK)
async def handle_promo_start_callback(callback_query: types.CallbackQuery):
    await callback_query.answer("Запускаю…")
    try:
        response, data = await api_json("post", "/promo/resume", timeout=20)
    except Exception as exc:
        await callback_query.message.answer(f"Не удалось запустить рекламу: {exc}")
        return
    if response.status_code != 200:
        await callback_query.message.answer(
            f"Ошибка запуска ({response.status_code}): {response.text}"
        )
        return
    await callback_query.message.answer("Рекламная рассылка запущена ✅")
    await send_promo_status_view(callback_query.message, edit=True)


@dp.callback_query_handler(lambda c: c.data == PROMO_STOP_CALLBACK)
async def handle_promo_stop_callback(callback_query: types.CallbackQuery):
    await callback_query.answer("Останавливаю…")
    try:
        response, data = await api_json("post", "/promo/pause", timeout=20)
    except Exception as exc:
        await callback_query.message.answer(f"Не удалось остановить рекламу: {exc}")
        return
    if response.status_code != 200:
        await callback_query.message.answer(
            f"Ошибка остановки ({response.status_code}): {response.text}"
        )
        return
    await callback_query.message.answer("Рекламная рассылка остановлена ⏹")
    await send_promo_status_view(callback_query.message, edit=True)


@dp.callback_query_handler(lambda c: c.data == PROMO_MESSAGE_ADD_CALLBACK)
async def handle_promo_message_add_callback(callback_query: types.CallbackQuery):
    user_id = callback_query.from_user.id
    promo_states[user_id] = {"mode": "add_message"}
    await callback_query.answer("Введите текст")
    await callback_query.message.answer(
        "Пришли текст рекламного сообщения. Можно использовать несколько строк."
    )


@dp.callback_query_handler(lambda c: c.data and c.data.startswith(PROMO_MESSAGE_DELETE_PREFIX))
async def handle_promo_message_delete_callback(callback_query: types.CallbackQuery):
    await callback_query.answer("Удаляю…")
    try:
        message_id = int(callback_query.data[len(PROMO_MESSAGE_DELETE_PREFIX) :])
    except ValueError:
        await callback_query.message.answer("Некорректный идентификатор сообщения.")
        return
    try:
        response, data = await api_json("delete", f"/promo/messages/{message_id}", timeout=20)
    except Exception as exc:
        await callback_query.message.answer(f"Не удалось удалить сообщение: {exc}")
        return
    if response.status_code != 200:
        await callback_query.message.answer(
            f"Ошибка удаления сообщения ({response.status_code}): {response.text}"
        )
        return
    await callback_query.message.answer("Сообщение удалено ✅")
    await send_promo_messages_view(callback_query.message, edit=True)


@dp.callback_query_handler(lambda c: c.data and c.data.startswith(PROMO_SCHEDULE_EDIT_PREFIX))
async def handle_promo_schedule_edit_callback(callback_query: types.CallbackQuery):
    slot = callback_query.data[len(PROMO_SCHEDULE_EDIT_PREFIX) :]
    label = PROMO_SLOT_LABELS.get(slot, slot)
    promo_states[callback_query.from_user.id] = {"mode": "edit_schedule", "slot": slot}
    await callback_query.answer("Укажи время")
    await callback_query.message.answer(
        f"Введи новое время для {label} в формате ЧЧ:ММ. Например 09:30"
    )


@dp.callback_query_handler(lambda c: c.data == PROMO_CLOSE_CALLBACK)
async def handle_promo_close_callback(callback_query: types.CallbackQuery):
    await callback_query.answer("Меню закрыто")
    await callback_query.message.edit_text("Меню рекламы закрыто.")


@dp.callback_query_handler(lambda c: c.data and c.data.startswith(DIALOGS_PAGE_PREFIX))
async def handle_dialog_page(callback_query: types.CallbackQuery):
    try:
        page = int(callback_query.data[len(DIALOGS_PAGE_PREFIX) :])
    except ValueError:
        await callback_query.answer("Некорректная страница", show_alert=True)
        return
    await callback_query.answer()
    await send_dialogs_list_message(callback_query.message, callback_query.from_user.id, page=page, edit=True)


@dp.callback_query_handler(lambda c: c.data and c.data.startswith(DIALOG_REFRESH_PREFIX))
async def handle_dialog_refresh(callback_query: types.CallbackQuery):
    try:
        page = int(callback_query.data[len(DIALOG_REFRESH_PREFIX) :])
    except ValueError:
        page = dialog_states.get(callback_query.from_user.id, {}).get("page", 0)
    await callback_query.answer("Обновляю…")
    await send_dialogs_list_message(callback_query.message, callback_query.from_user.id, page=page, edit=True)


@dp.callback_query_handler(lambda c: c.data and c.data.startswith(DIALOG_SELECT_PREFIX))
async def handle_dialog_select(callback_query: types.CallbackQuery):
    payload = callback_query.data[len(DIALOG_SELECT_PREFIX) :]
    try:
        peer_id_str, page_str = payload.split(":", 1)
        peer_id = int(peer_id_str)
        page = int(page_str)
    except ValueError:
        await callback_query.answer("Некорректный выбор", show_alert=True)
        return
    state = dialog_states.get(callback_query.from_user.id, {})
    state.update({"page": page})
    dialog_states[callback_query.from_user.id] = state
    await callback_query.answer()
    await send_dialog_view_message(callback_query.message, callback_query.from_user.id, peer_id, edit=True)


@dp.callback_query_handler(lambda c: c.data == DIALOG_BACK_CALLBACK)
async def handle_dialog_back(callback_query: types.CallbackQuery):
    page = dialog_states.get(callback_query.from_user.id, {}).get("page", 0)
    await callback_query.answer()
    await send_dialogs_list_message(callback_query.message, callback_query.from_user.id, page=page, edit=True)


@dp.callback_query_handler(lambda c: c.data and c.data.startswith(DIALOG_VIEW_REFRESH_PREFIX))
async def handle_dialog_view_refresh(callback_query: types.CallbackQuery):
    try:
        peer_id = int(callback_query.data[len(DIALOG_VIEW_REFRESH_PREFIX) + 1 :])
    except ValueError:
        peer_id = dialog_states.get(callback_query.from_user.id, {}).get("peer_id")
    if not peer_id:
        await callback_query.answer("Диалог не выбран", show_alert=True)
        return
    await callback_query.answer("Обновляю…")
    await send_dialog_view_message(callback_query.message, callback_query.from_user.id, peer_id, edit=True)


@dp.callback_query_handler(lambda c: c.data and c.data.startswith(DIALOG_MORE_PREFIX))
async def handle_dialog_more(callback_query: types.CallbackQuery):
    payload = callback_query.data[len(DIALOG_MORE_PREFIX) :]
    try:
        peer_id_str, offset_str = payload.split(":", 1)
        peer_id = int(peer_id_str)
        offset = int(offset_str)
    except ValueError:
        await callback_query.answer("Некорректный запрос", show_alert=True)
        return
    await callback_query.answer()
    await send_dialog_view_message(callback_query.message, callback_query.from_user.id, peer_id, offset_id=offset, edit=True)


@dp.callback_query_handler(lambda c: c.data and c.data.startswith(DIALOG_COMPOSE_CALLBACK))
async def handle_dialog_compose(callback_query: types.CallbackQuery):
    try:
        peer_id = int(callback_query.data.split(":", 1)[1])
    except (ValueError, IndexError):
        peer_id = dialog_states.get(callback_query.from_user.id, {}).get("peer_id")
    if not peer_id:
        await callback_query.answer("Диалог не выбран", show_alert=True)
        return
    state = dialog_states.get(callback_query.from_user.id, {})
    state.update({"mode": "await_text", "peer_id": peer_id, "draft": None})
    dialog_states[callback_query.from_user.id] = state
    await callback_query.answer()
    await callback_query.message.answer("Напиши сообщение для отправки. После ввода я предложу отправить или получить помощь GPT.")


@dp.callback_query_handler(lambda c: c.data and c.data.startswith(DIALOG_HELP_CALLBACK))
async def handle_dialog_help(callback_query: types.CallbackQuery):
    try:
        peer_id = int(callback_query.data.split(":", 1)[1])
    except (ValueError, IndexError):
        peer_id = dialog_states.get(callback_query.from_user.id, {}).get("peer_id")
    if not peer_id:
        await callback_query.answer("Диалог не выбран", show_alert=True)
        return
    draft = dialog_states.get(callback_query.from_user.id, {}).get("draft")
    await callback_query.answer("Генерирую…")
    await send_dialog_suggestions(callback_query.from_user.id, peer_id, draft, callback_query.message)


@dp.callback_query_handler(lambda c: c.data == DIALOG_SEND_CANCEL)
async def handle_dialog_cancel(callback_query: types.CallbackQuery):
    state = dialog_states.get(callback_query.from_user.id, {})
    if state.get("mode") == "await_text" or state.get("mode") == "draft_ready":
        state["mode"] = "view"
        state["draft"] = None
    dialog_states[callback_query.from_user.id] = state
    await callback_query.answer("Черновик очищен")


@dp.callback_query_handler(lambda c: c.data == DIALOG_SEND_CONFIRM)
async def handle_dialog_send_confirm(callback_query: types.CallbackQuery):
    state = dialog_states.get(callback_query.from_user.id, {})
    peer_id = state.get("peer_id")
    draft = (state.get("draft") or "").strip()
    if not peer_id or not draft:
        await callback_query.answer("Нет сообщения", show_alert=True)
        return
    try:
        response, data = await api_json(
            "post",
            f"/dialogs/{peer_id}/send",
            json={"text": draft},
            timeout=20,
        )
    except Exception as exc:
        await callback_query.message.answer(f"Не удалось отправить: {exc}")
        return
    if response.status_code != 200 or not isinstance(data, dict):
        await callback_query.message.answer(
            f"Ошибка отправки ({response.status_code}): {response.text}"
        )
        return
    state["draft"] = None
    state["mode"] = "view"
    dialog_states[callback_query.from_user.id] = state
    await callback_query.answer("Отправлено")
    await send_dialog_view_message(callback_query.message, callback_query.from_user.id, peer_id, edit=True, notice="Сообщение отправлено")


@dp.callback_query_handler(lambda c: c.data == DIALOG_DRAFT_HELP)
async def handle_dialog_draft_help(callback_query: types.CallbackQuery):
    state = dialog_states.get(callback_query.from_user.id, {})
    peer_id = state.get("peer_id")
    draft = (state.get("draft") or "").strip()
    if not peer_id or not draft:
        await callback_query.answer("Нет черновика", show_alert=True)
        return
    await callback_query.answer("Думаю…")
    await send_dialog_suggestions(callback_query.from_user.id, peer_id, draft, callback_query.message)


@dp.callback_query_handler(lambda c: c.data and c.data.startswith(DIALOG_SUGGEST_PREFIX))
async def handle_dialog_suggest_choice(callback_query: types.CallbackQuery):
    payload = callback_query.data[len(DIALOG_SUGGEST_PREFIX) :]
    try:
        idx_str, peer_id_str = payload.split(":", 1)
        idx = int(idx_str)
        peer_id = int(peer_id_str)
    except ValueError:
        await callback_query.answer("Некорректный выбор", show_alert=True)
        return
    state = dialog_states.get(callback_query.from_user.id, {})
    suggestions = state.get("suggestions") or []
    if idx < 0 or idx >= len(suggestions):
        await callback_query.answer("Нет такого варианта", show_alert=True)
        return
    text = suggestions[idx]
    try:
        response, data = await api_json(
            "post",
            f"/dialogs/{peer_id}/send",
            json={"text": text},
            timeout=20,
        )
    except Exception as exc:
        await callback_query.message.answer(f"Не удалось отправить: {exc}")
        return
    if response.status_code != 200:
        await callback_query.message.answer(
            f"Ошибка отправки ({response.status_code}): {response.text}"
        )
        return
    state["draft"] = None
    state["mode"] = "view"
    dialog_states[callback_query.from_user.id] = state
    await callback_query.answer("Отправлено")
    await send_dialog_view_message(callback_query.message, callback_query.from_user.id, peer_id, edit=True, notice="Вариант отправлен")


@dp.message_handler(content_types=types.ContentTypes.TEXT)
async def handle_text(message: types.Message):
    user_id = message.from_user.id
    state = user_states.get(user_id)
    broadcast_state = broadcast_states.get(user_id)
    promo_state = promo_states.get(user_id)
    dialog_state = dialog_states.get(user_id)
    global current_scrape_job_id

    if dialog_state and dialog_state.get("mode") in {"await_text", "draft_ready"}:
        text_value = (message.text or "").strip()
        if not text_value:
            await message.answer("Текст не может быть пустым.")
            return
        dialog_state["draft"] = text_value
        dialog_state["mode"] = "draft_ready"
        dialog_states[user_id] = dialog_state
        preview = _format_message_html(text_value)
        keyboard = types.InlineKeyboardMarkup(row_width=2)
        keyboard.row(
            types.InlineKeyboardButton("Отправить", callback_data=DIALOG_SEND_CONFIRM),
            types.InlineKeyboardButton("Помощь", callback_data=DIALOG_DRAFT_HELP),
        )
        keyboard.add(types.InlineKeyboardButton("Отмена", callback_data=DIALOG_SEND_CANCEL))
        await message.answer(
            f"Черновик:\n{preview}\n\nВыбери действие:",
            reply_markup=keyboard,
            parse_mode="HTML",
        )
        return

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

    if promo_state:
        text_value = (message.text or "").strip()
        lowered = text_value.lower()
        if lowered in {"отмена", "cancel"}:
            promo_states.pop(user_id, None)
            await message.answer("Действие отменено.", reply_markup=MAIN_KEYBOARD)
            return
        mode = promo_state.get("mode")
        if mode == "add_message":
            if not text_value:
                await message.answer("Текст сообщения не должен быть пустым.")
                return
            payload = {"text": message.text}
            try:
                response, data = await api_json("post", "/promo/messages", json=payload, timeout=20)
            except Exception as exc:
                await message.answer(f"Не удалось сохранить сообщение: {exc}")
                return
            if response.status_code != 200 or not isinstance(data, dict):
                await message.answer(
                    f"Ошибка при сохранении сообщения ({response.status_code}): {response.text}"
                )
                return
            promo_states.pop(user_id, None)
            await message.answer("Сообщение добавлено ✅")
            await send_promo_messages_view(message)
            return
        elif mode == "edit_schedule":
            slot = promo_state.get("slot")
            parsed = _parse_time_string(text_value)
            if not parsed:
                await message.answer("Нужно время в формате ЧЧ:ММ, например 09:00")
                return
            hour, minute = parsed
            payload = {"slot": slot, "hour": hour, "minute": minute}
            try:
                response, data = await api_json("put", "/promo/schedule", json=payload, timeout=20)
            except Exception as exc:
                await message.answer(f"Не удалось обновить расписание: {exc}")
                return
            if response.status_code != 200 or not isinstance(data, dict):
                await message.answer(
                    f"Ошибка при обновлении расписания ({response.status_code}): {response.text}"
                )
                return
            promo_states.pop(user_id, None)
            label = PROMO_SLOT_LABELS.get(slot, slot)
            await message.answer(f"{label} обновлено на {hour:02d}:{minute:02d} ✅")
            await send_promo_schedule_view(message)
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
        current_scrape_job_id = job_id

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
    await callback_query.answer()
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
    await callback_query.answer()
    await send_chat_selection(callback_query.message, user_id)


@dp.callback_query_handler(lambda c: c.data == "broadcast_cancel")
async def handle_broadcast_cancel(callback_query: types.CallbackQuery):
    user_id = callback_query.from_user.id
    broadcast_states.pop(user_id, None)
    await callback_query.answer("Рассылка отменена.")
    await callback_query.message.edit_text("Выбор рассылки отменён.", reply_markup=None)


@dp.callback_query_handler(lambda c: c.data and c.data.startswith("broadcast_select:"))
async def handle_broadcast_select(callback_query: types.CallbackQuery):
    user_id = callback_query.from_user.id
    state = broadcast_states.get(user_id)
    if not state:
        await callback_query.answer("Сессия рассылки не найдена.", show_alert=True)
        return
    try:
        index = int(callback_query.data.split(":", 1)[1])
    except (ValueError, IndexError):
        await callback_query.answer("Некорректный выбор.", show_alert=True)
        return
    chats = state.get("chats", [])
    if index < 0 or index >= len(chats):
        await callback_query.answer("Чат не найден.", show_alert=True)
        return
    selected = chats[index]
    source_chat = selected.get("source_chat") or selected.get("chat_title") or selected.get("filename")
    chat_title = selected.get("chat_title") or selected.get("filename")

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
        f"Последние записи рассылки {job_id}:\n{text}",
        parse_mode=None,
        reply_markup=reply_markup,
    )
    await callback_query.answer()




if __name__ == "__main__":
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    executor.start_polling(dp, skip_updates=True)
