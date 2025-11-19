# bot.py
import re
import os
import json
from aiogram import Bot, Dispatcher, types
from aiogram.enums import ParseMode
from aiogram.client.default import DefaultBotProperties
from aiogram.filters import CommandStart
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.exceptions import TelegramBadRequest
import logging
from datetime import datetime, timezone
from aiogram import F
from aiogram.types import CallbackQuery
from db import toggle_reaction, count_reactions, get_user_reputation, get_user_stats
import asyncio
from typing import Dict, Tuple, Optional

# ключ = (chat_id, message_id)
_PENDING_UPDATES: Dict[Tuple[int, int], dict] = {}
from db import get_message_by_id  # -> dict: {"id": int, "text": str, "sender_id": int, "sender_username": Optional[str],
                                  #            "chat_id": int, "message_id": int, "chat_username": Optional[str]}
# базовая настройка: и в консоль, и INFO видно
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
log = logging.getLogger("otc_bot")  # имя на твой вкус

BOT_TOKEN = os.getenv("BOT_TOKEN")
if not BOT_TOKEN:
    raise RuntimeError("❌ BOT_TOKEN is missing! Please set environment variable BOT_TOKEN")

OTC_GROUP_USERNAME = "otc_wtb_only"  # ваша группа с репостами
CHAT_INDEX_FILE = os.path.abspath("otc_chats_index.json")
_chat_index: dict[str, dict] = {}

KNOWN_ITEMS = {
    "exchanges": [
        "binance", "bybit", "okx", "huobi", "htx",
        "gate io", "bitget", "mexc", "kucoin", "bingx",
        "coinlist", "paxful", "cryptocom", "crypto com",
        "bc game", "bcgame", "fragment", "weex", "arkham"
    ],
    "payments_banks": [
        # классические банки
        "bunq", "n26", "monzo", "santander", "bbva",
        "ing", "finom", "vivid", "chase", "c24",
        "trade republic", "billions", "bank of america",

        # финтех / нео банки
        "revolut", "revolut business", "revolut personal",
        "wise", "wise business", "wise personal",
        "paysera", "icard", "zen", "zen business",
        "airwallex", "mercury", "bitsa", "wirex",
        "genome", "sumup", "persona", "trustee", "trustee plus",

        # платежки
        "stripe", "stripe business", "paypal", "paypal business",
        "cashapp", "alipay", "nexo", "ozon", "twitter"
    ],
    "kyc_verification": [
        "kyc", "wts kyc", "kyc service", "kyc or ready",
        "kyc bybit", "kyc service or", "kyc accepted",
        "kyc / acc", "kyc / ready", "kyc or",
        "kyc by", "wts kyc service", "wts kyc or",
        "other kyc", "other kyc accepted", "kyc 8",
        "blockpass", "persona", "sumsub", "onfido",
        "holonym", "buildpad", "buidlpad", "solayer",
        "kaito", "arkham", "legion", "civic",
        "sandbox", "echo"
    ],
    "marketplaces_services": [
        "fragment", "tiktok", "temu", "ozon", "airbnb",
        "telegram", "twitter", "whatsapp", "esim",
        "game", "bc game", "bcgame", "bet365"
    ],
    "crypto_wallets": [
        "metamask", "trustwallet", "phantom", "coinbase wallet",
        "ledger", "trezor", "tronlink"
    ],
    "countries": [
        "usa", "indonesia", "spain", "egypt", "philippines",
        "uganda", "georgia", "america", "germany", "armenia",
        "africa", "russia", "costa rica", "italy", "zambia",
        "vietnam", "rwanda", "angola", "uruguay", "paraguay",
        "argentina", "bolivia", "peru", "brazil", "chile",
        "colombia", "el salvador", "mexico"
    ],
    "misc": [
        "iban", "llc", "emulator", "passport",
        "vcc", "accs", "accounts", "ready account",
        "ready acc", "old account", "merchant", "premium",
        "crypto", "wallet", "stake", "escrow", "reviews",
        "selfie", "verification"
    ]
}


bot = Bot(
    token=BOT_TOKEN,
    default=DefaultBotProperties(parse_mode=ParseMode.HTML),
)
dp = Dispatcher()

# Память “в одном процессе”: какой message_id бота последний для конкретного user_id
LAST_REPLY_ID: dict[int, int] = {}



def compute_rating_percent(likes: int, dislikes: int) -> int:
    """
    База = 50%. Баланс голосов сдвигает рейтинг в диапазоне 0..100.
    Формула: 50 + 50 * (likes - dislikes) / max(1, likes + dislikes)
    Примеры:
      0/0 -> 50%
      10/0 -> 100%
      0/10 -> 0%
      5/5 -> 50%
      7/3 -> 50 + 50*(4/10)=70%
    """
    total = likes + dislikes
    score = 50.0 + 50.0 * ( (likes - dislikes) / (total if total > 0 else 1) )
    return max(0, min(100, round(score)))

def stars_from_percent(pct: int, max_stars: int = 5) -> str:
    """
    0% -> 0 звёзд, 100% -> 5 звёзд. Берём целое число звёзд (можно сделать половинки позже).
    """
    full = int(round((pct / 100.0) * max_stars))
    full = max(0, min(max_stars, full))
    return "⭐" * full + "☆" * (max_stars - full)

def build_reaction_kb(row_id: int, likes: int, dislikes: int, start_payload: str) -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text=f"✅ {likes}", callback_data=f"like_{row_id}"),
            InlineKeyboardButton(text=f"❌ {dislikes}", callback_data=f"dislike_{row_id}"),
        ],
        [
            InlineKeyboardButton(text="💬 Contact buyer", url=f"https://t.me/otc_darwin_bot?start={start_payload}")
        ],
    ])
    return kb

def load_chat_index(path: str = CHAT_INDEX_FILE) -> None:
    global _chat_index
    if _chat_index:
        return
    try:
        with open(path, "r", encoding="utf-8") as f:
            _chat_index = json.load(f)
    except Exception:
        _chat_index = {}

def get_chat_meta(chat_id: int | None):
    """Возвращает (username, title) по chat_id из JSON."""
    if not chat_id:
        return None, None
    load_chat_index()
    data = _chat_index.get(str(chat_id))
    if not data:
        return None, None
    return data.get("username"), data.get("title")


def buyer_link(sender_id: int, username: Optional[str]) -> Tuple[str, bool]:
    """Возвращает (url, is_https)."""
    if username:
        u = username.lstrip("@")
        return f"https://t.me/{u}", True
    return f"tg://user?id={sender_id}", False


def otc_post_link(otc_msg_id: int) -> str:
    """Ссылка на пост в вашей группе по её username и message_id."""
    return f"https://t.me/{OTC_GROUP_USERNAME}/{otc_msg_id}"


def original_post_link(chat_username: str | None, chat_id: int | None, msg_id: int | None) -> str | None:
    """
    Возвращает ссылку на оригинальное сообщение:
    - если у чата есть username -> https://t.me/<username>/<msg_id>
    - иначе (приватный канал/супергруппа) -> https://t.me/c/<internal_id>/<msg_id>, где internal_id = chat_id без -100
    Важно: c-ссылки открываются только у участников чата.
    """
    if not msg_id:
        return None

    # Публичный чат
    if chat_username:
        u = chat_username.lstrip("@")
        return f"https://t.me/{u}/{msg_id}"

    # Приватный канал/супергруппа -> c-link
    if chat_id:
        s = str(chat_id)
        internal_id = s[4:] if s.startswith("-100") else str(abs(int(chat_id)))
        return f"https://t.me/c/{internal_id}/{msg_id}"

    return None


@dp.message(CommandStart())
async def start_handler(message: types.Message) -> None:
    # ждём формат:
    #   /start <row_id>
    #   /start <row_id>_<otc_msg_id>
    user = message.from_user
    log.info(
        "START clicked by user_id=%s, username=%s, full_name=%s, text=%r, at=%s",
        user.id,
        user.username,
        user.full_name,
        message.text,
        datetime.now(timezone.utc).isoformat(),
    )

    parts = (message.text or "").split(maxsplit=1)
    if len(parts) < 2:
        await message.answer("Open me from the button under the buyer’s post.")
        return

    m = re.fullmatch(r"\s*(\d+)(?:_(\d+))?\s*", parts[1])
    if not m:
        await message.answer("Bad link format.")
        return

    row_id = int(m.group(1))
    otc_msg_id = int(m.group(2)) if m.group(2) else None

    row = get_message_by_id(row_id)
    if not row:
        await message.answer("❌ Buyer request not found.")
        return

    sender_id = int(row.get("sender_id") or 0)
    username = row.get("sender_username")

    # Для кнопки перехода к оригиналу
    orig_chat_id = int(row.get("chat_id") or 0)
    orig_msg_id = int(row.get("message_id") or 0)
    orig_chat_username = row.get("chat_username")

    idx_username, idx_title = get_chat_meta(orig_chat_id)
    group_username = orig_chat_username or idx_username
    group_title = idx_title

    url, is_https = buyer_link(sender_id, username)

    # Шапка + 3 способа связи + fallback
    # 1) username (если есть) — кликается всегда
    # 2) tg://user?id=... (или https, если есть username) — мы всё равно делаем кликабельным
    # 3) оригинальное сообщение (если доступно)
    lines: list[str] = []

    lines.append("📬 <b>How to contact the buyer:</b>")
    if username:
        u = username.lstrip("@")
        lines.append(f"1) Username: <a href=\"https://t.me/{u}\">@{u}</a>  <i>(recommended)</i>")
    else:
        lines.append("1) Username: <i>not available</i>")

    # Универсальная ссылка (если есть username — она дублирует пункт 1, но оставим для консистентности)
    if is_https:
        lines.append(f"2) Profile link: <a href=\"{url}\">{url}</a>")
    else:
        # tg:// может не кликается у части клиентов, но всё равно показываем
        lines.append(f"2) Profile link: <a href=\"{url}\">{url}</a>")

    # Оригинальный пост — если его можно открыть
    orig_url = original_post_link(group_username, orig_chat_id, orig_msg_id)
    if orig_url:
        if group_username:
            lines.append(
                f"3) Original message: <a href=\"{orig_url}\">view</a> "
                f"(in <a href=\"https://t.me/{group_username}\">@{group_username}</a>)"
            )
            lines.append("<i>If it doesn’t open — join the group first.</i>")
        else:
            lines.append(f"3) Original message: <a href=\"{orig_url}\">{orig_url}</a>")

    if group_username or group_title:
        g_label = f"@{group_username}" if group_username else (group_title or "source group")
        lines.append(f"\n👥 Group: <a href=\"https://t.me/{group_username}\">{g_label}</a>")

    # Fallback к админу
    lines.append("")
    lines.append("If none of the above works, please contact admin: <a href=\"https://t.me/studios_by_darwin\">@studios_by_darwin</a>")
    lines.append("")
    lines.append("✅ Good luck with the deal!")

    text = "\n".join(lines)

    # Кнопки:
    buttons: list[list[InlineKeyboardButton]] = []

    # Кнопка “Message buyer” — только если есть username (иначе смысла нет)
    if username:
        u = username.lstrip("@")
        buttons.append([InlineKeyboardButton(text="💬 Message buyer", url=f"https://t.me/{u}")])

    if otc_msg_id:
        buttons.append([InlineKeyboardButton(text="🔙 Back to group post", url=otc_post_link(otc_msg_id))])

    if orig_url:
        buttons.append([InlineKeyboardButton(text="📩 View original post", url=orig_url)])

    if group_username:
        buttons.append([InlineKeyboardButton(text="🔗 Open group", url=f"https://t.me/{group_username}")])

    kb: InlineKeyboardMarkup | None = InlineKeyboardMarkup(inline_keyboard=buttons) if buttons else None

    chat_id = message.chat.id
    old_msg_id = LAST_REPLY_ID.get(chat_id)

    # Пытаемся красиво обновить прошлый ответ бота
    if old_msg_id:
        try:
            await bot.edit_message_text(
                chat_id=chat_id,
                message_id=old_msg_id,
                text=text,
                reply_markup=kb,
                disable_web_page_preview=True,
                parse_mode=ParseMode.HTML,
            )
        except TelegramBadRequest:
            new_msg = await bot.send_message(
                chat_id=chat_id,
                text=text,
                reply_markup=kb,
                disable_web_page_preview=True,
            )
            LAST_REPLY_ID[chat_id] = new_msg.message_id
            try:
                await bot.delete_message(chat_id=chat_id, message_id=old_msg_id)
            except TelegramBadRequest:
                pass
    else:
        new_msg = await bot.send_message(
            chat_id=chat_id,
            text=text,
            reply_markup=kb,
            disable_web_page_preview=True,
        )
        LAST_REPLY_ID[chat_id] = new_msg.message_id

    # Чистим команду пользователя
    try:
        await message.delete()
    except TelegramBadRequest:
        pass

def extract_tags(text: str) -> list[str]:
    """Возвращает список хэштегов из словаря по тексту."""
    tags = []
    lowered = text.lower()
    for category, words in KNOWN_ITEMS.items():
        for w in words:
            if w in lowered:
                tags.append("#" + w.replace(" ", "_"))  # пробелы → "_"
    return list(set(tags))  # убираем дубликаты

def render_post_body(row_id: int, likes: int, dislikes: int) -> str:
    row = get_message_by_id(row_id)
    cleaned_text = (row.get("text") or "").strip()
    c_text = clean_text(cleaned_text)
    rating_pct = compute_rating_percent(likes, dislikes)
    stars = stars_from_percent(rating_pct)
    sender_id = row.get("sender_id")
    tags = extract_tags(c_text)
    tags_line = ", ".join(tags) if tags else ""

    # Заглушки — позже подставишь реальные значения
    user_total_messages, user_reviews_count = get_user_stats(sender_id)


    parts = []
    parts.append("<b>💸 New WTB message</b>")

    parts.append(f"\n<b>About user ({stars}):</b>")
    parts.append(
        "<blockquote>"
        f"~ <i>User rating:</i> {rating_pct}%\n"
        f"~ <i>Total messages:</i> {user_total_messages}\n"
        f"~ <i>Number of reviews:</i> {user_reviews_count}"
        "</blockquote>"
    )

    parts.append("<b>Text:</b>")
    parts.append(f"<blockquote>{c_text}</blockquote>")

    if tags_line:
        parts.append(f"\n<i>#{'</i> <i>#'.join(t.lstrip('#') for t in tags)}</i>")

    return "\n".join(parts)

async def _flush_one_update(chat_id: int, message_id: int):
    key = (chat_id, message_id)
    data = _PENDING_UPDATES.get(key)
    if not data:
        return

    row_id: int = data["row_id"]
    likes: int = data["likes"]
    dislikes: int = data["dislikes"]
    start_payload: str = data["start_payload"]
    msg_obj: types.Message = data["message"]

    # тело поста (рейтинг + звёзды) и клавиатура
    body = render_post_body(row_id, likes, dislikes)
    kb = build_reaction_kb(row_id, likes, dislikes, start_payload)

    try:
        await msg_obj.edit_text(
            body,
            reply_markup=kb,
            disable_web_page_preview=True,
            parse_mode=ParseMode.HTML,
        )
    except TelegramBadRequest as e:
        log.warning("edit_text failed chat=%s msg=%s: %s", chat_id, message_id, e)
        # fallback — хотя бы кнопки
        try:
            await msg_obj.edit_reply_markup(reply_markup=kb)
        except Exception as e2:
            log.warning("edit_reply_markup fallback failed: %s", e2)
    finally:
        data["scheduled"] = False
        data["last_sent"] = (likes, dislikes)

def _schedule_coalesced_update(message: types.Message, *, row_id: int, likes: int, dislikes: int, start_payload: str, delay: float = 1.0):
    key = (message.chat.id, message.message_id)
    entry = _PENDING_UPDATES.get(key)
    if not entry:
        entry = {
            "row_id": row_id,
            "likes": likes,
            "dislikes": dislikes,
            "start_payload": start_payload,
            "message": message,
            "scheduled": False,
            "task": None,
            "last_sent": None,
        }
        _PENDING_UPDATES[key] = entry
    else:
        entry["row_id"] = row_id
        entry["likes"] = likes
        entry["dislikes"] = dislikes
        entry["start_payload"] = start_payload
        entry["message"] = message

    if not entry["scheduled"]:
        entry["scheduled"] = True

        async def _job():
            await asyncio.sleep(delay)
            await _flush_one_update(message.chat.id, message.message_id)

        entry["task"] = asyncio.create_task(_job())


def _schedule_coalesced_update(message: types.Message, *, row_id: int, likes: int, dislikes: int, start_payload: str, delay: float = 1.0):
    """Кладёт (или обновляет) состояние и ставит отправку через delay сек, если ещё не запланирована."""
    key = (message.chat.id, message.message_id)
    entry = _PENDING_UPDATES.get(key)
    if not entry:
        entry = {
            "row_id": row_id,
            "likes": likes,
            "dislikes": dislikes,
            "start_payload": start_payload,
            "message": message,
            "scheduled": False,
            "task": None,
            "last_sent": None,  # (likes, dislikes)
        }
        _PENDING_UPDATES[key] = entry
    else:
        # обновляем только изменяемые поля
        entry["row_id"] = row_id
        entry["likes"] = likes
        entry["dislikes"] = dislikes
        entry["start_payload"] = start_payload
        entry["message"] = message  # на случай, если объект поменялся

    if not entry["scheduled"]:
        entry["scheduled"] = True

        async def _job():
            # копим клики в течение delay
            await asyncio.sleep(delay)
            await _flush_one_update(message.chat.id, message.message_id)

        entry["task"] = asyncio.create_task(_job())

def clean_text(text: str) -> str:
    """Убираем контакты: @юзеры, ссылки, телефоны."""
    t = text or ""
    t = re.sub(r"@\w{3,32}", "[hidden]", t)               # @username
    t = re.sub(r"https?://\S+|t\.me/\S+", "[hidden]", t)  # URL + t.me
    t = re.sub(r"\+?\d[\d\-\s()]{7,}", "[hidden]", t)     # телефоны
    return t.strip()

@dp.callback_query(F.data.regexp(r"^(like|dislike)_(\d+)$"))
async def reaction_handler(cq: CallbackQuery):
    try:
        action, row_id_str = cq.data.split("_", 1)
        row_id = int(row_id_str)
        user_id = cq.from_user.id
        new_reaction = 1 if action == "like" else -1

        # 1) изменить реакцию в БД
        result = toggle_reaction(row_id=row_id, user_id=user_id, new_reaction=new_reaction)

        # 2) пересчитать свежие значения по пользователю, а не по посту
        msg = get_message_by_id(row_id)  # берём сообщение, чтобы узнать sender_id
        sender_id = msg["sender_id"]
        likes, dislikes = get_user_reputation(sender_id)

        # 3) запланировать объединённое обновление (текст + кнопки)
        otc_msg_id = cq.message.message_id
        start_payload = f"{row_id}_{otc_msg_id}"

        _schedule_coalesced_update(
            cq.message,
            row_id=row_id,
            likes=likes,
            dislikes=dislikes,
            start_payload=start_payload,
            delay=1.0,  # можно 1.5–2.0 при высоком трафике
        )

        # мгновенный ответ пользователю
        if result == "added":
            await cq.answer("Saved ✅", show_alert=False)
        elif result == "removed":
            await cq.answer("Removed ↩️", show_alert=False)
        else:
            await cq.answer("Switched 🔁", show_alert=False)

    except Exception:
        log.exception("reaction handler error")
        await cq.answer("Error", show_alert=False)


async def main() -> None:
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())