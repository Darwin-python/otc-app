# collector.py
import os, re, json, asyncio  # ← добавил json
from datetime import datetime, timezone
from telethon import TelegramClient, events, Button
from db import init_db, save_message, mark_deleted_in_archive, exists_same_text_for_sender, get_username_for_sender
import logging
from telethon.tl.functions.messages import GetDialogFiltersRequest
from telethon.tl.types import DialogFilter
from telethon import utils as tg_utils
from html import escape
from html import escape
from db import count_reactions, save_published_post, get_user_reputation, get_user_stats

def compute_rating_percent(likes: int, dislikes: int) -> int:
    total = likes + dislikes
    score = 50.0 + 50.0 * ((likes - dislikes) / (total if total > 0 else 1))
    return max(0, min(100, round(score)))

def stars_from_percent(pct: int, max_stars: int = 5) -> str:
    full = int(round((pct / 100.0) * max_stars))
    full = max(0, min(max_stars, full))
    return "⭐" * full + "☆" * (max_stars - full)

logger = logging.getLogger("buy_detector")
API_ID   = 29635442
API_HASH = "7db8d1a90eacb8c2203fc4b2c728c216"

# ⚠️ Лучше хранить в переменных окружения
BOT_TOKEN    = "8399795660:AAFxn4x7bwPXNzhJ2RzSGC-6Pkjr9YyuFS4"
TARGET_GROUP = "otc_wtb_only"  # @-префикс можно опустить

CHAT_INDEX_PATH = os.path.abspath("otc_chats_index.json")

# Отдельные сессии: юзер — читает, бот — пишет
USER_SESSION = os.path.abspath("otc_user.session")
BOT_SESSION  = os.path.abspath("otc_bot.session")

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

# ——— Утилиты ———

def render_stars(value: float, max_stars: int = 5) -> str:
    """
    value: 0..5 (можно дробное). Для простоты без половинок.
    """
    value = max(0, min(float(value), max_stars))
    full = int(round(value))          # округляем до целого
    empty = max_stars - full
    return "⭐" * full + "☆" * empty

async def build_chats_index(client: TelegramClient, chat_ids: set[int], out_path: str = CHAT_INDEX_PATH) -> dict[int, dict]:
    """
    Собирает индекс по чатам: { chat_id: { "username": str|None, "title": str, "internal_id": str } }
    internal_id пригодится для t.me/c/<internal>/<msg_id> (приватные супергруппы/каналы).
    """
    index: dict[int, dict] = {}
    if not chat_ids:
        return index

    print(f"[chats-index] building for {len(chat_ids)} chats…")
    for i, cid in enumerate(sorted(chat_ids), start=1):
        try:
            # entity по chat_id
            ent = await client.get_entity(cid)
            # нормализуем chat_id так, как его будет отдавать Telethon в событиях
            full_id = await client.get_peer_id(ent)  # обычно вида -100123…
            title = getattr(ent, "title", None) or getattr(ent, "first_name", None) or getattr(ent, "last_name", None) or ""
            username = getattr(ent, "username", None)

            # internal_id для c-ссылок (без -100)
            s = str(full_id)
            internal_id = s[4:] if s.startswith("-100") else str(abs(int(full_id)))

            index[int(full_id)] = {
                "username": username,              # например: "my_public_group"
                "title": title,                    # красивое имя чата
                "internal_id": internal_id,        # для t.me/c/<internal>/<msg_id>
            }

            # чуть-чуть притормаживаем, чтобы не ловить rate limit
            if i % 10 == 0:
                await asyncio.sleep(0.2)

        except Exception as e:
            logging.getLogger("buy_detector").warning(f"[chats-index] failed for {cid}: {e}")
            continue

    # сохраняем JSON
    try:
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(index, f, ensure_ascii=False, indent=2)
        print(f"[chats-index] saved -> {out_path} ({len(index)} items)")
    except Exception as e:
        logging.getLogger("buy_detector").error(f"[chats-index] save error: {e}")

    return index


def link_to_chat(chat_id: int, index: dict[int, dict]) -> str | None:
    """
    Возвращает ссылку на чат (паблик или приват):
      - если есть username: https://t.me/<username>
      - иначе: https://t.me/c/<internal_id>
    """
    meta = index.get(int(chat_id))
    if not meta:
        return None
    username = meta.get("username")
    if username:
        return f"https://t.me/{username}"
    internal = meta.get("internal_id")
    return f"https://t.me/c/{internal}" if internal else None

async def get_chats_from_folder(client, folder_name: str) -> set[int]:
    """
    Возвращает chat_id всех чатов, явно включённых в папку (фильтр) с указанным именем.
    Учитывает, что title может быть TextWithEntities.
    """
    try:
        res = await client(GetDialogFiltersRequest())
        filters = getattr(res, "filters", []) or []
    except Exception as e:
        logger.error(f"Ошибка при получении фильтров: {e}")
        return set()

    target = (folder_name or "").strip().lower()
    for f in filters:
        if not isinstance(f, DialogFilter):
            continue

        # title может быть str или TextWithEntities
        title_obj = getattr(f, "title", "") or ""
        title_str = getattr(title_obj, "text", title_obj)  # берем .text если это TextWithEntities
        title_str = str(title_str).strip().lower()

        if title_str != target:
            continue

        chat_ids: set[int] = set()
        for peer in getattr(f, "include_peers", []) or []:
            try:
                pid = await client.get_peer_id(peer)
                chat_ids.add(pid)
            except Exception as e:
                logger.debug(f"Не смог получить peer_id для {peer}: {e}")
        return chat_ids

    logger.warning(f"Папка '{folder_name}' не найдена или пуста.")
    return set()

def extract_tags(text: str) -> list[str]:
    """Возвращает список хэштегов из словаря по тексту."""
    tags = []
    lowered = text.lower()
    for category, words in KNOWN_ITEMS.items():
        for w in words:
            if w in lowered:
                tags.append("#" + w.replace(" ", "_"))  # пробелы → "_"
    return list(set(tags))  # убираем дубликаты

async def resolve_username(user_client, user_id: int) -> str | None:
    """
    Пытается максимально надёжно получить username по user_id.
    Работает только если текущий аккаунт (user_client) может "видеть" этого пользователя
    (например, они состоят в одном чате или у юзера публичный профиль).
    Возвращает username (без @) или None.
    """
    from telethon.tl.functions.users import GetFullUserRequest
    from telethon.tl.types import PeerUser

    # 1. Пробуем самый быстрый способ: get_entity
    try:
        user = await user_client.get_entity(user_id)
        if getattr(user, "username", None):
            return user.username
    except Exception as e:
        logger.debug(f"get_entity failed for {user_id}: {e}")

    # 2. Пробуем input_entity
    try:
        user = await user_client.get_input_entity(user_id)
        if getattr(user, "username", None):
            return user.username
    except Exception as e:
        logger.debug(f"get_input_entity failed for {user_id}: {e}")

    # 3. Пробуем через peer (бывает помогает)
    try:
        peer = await user_client.get_peer_id(user_id)
        if isinstance(peer, PeerUser):
            user = await user_client.get_entity(peer)
            if getattr(user, "username", None):
                return user.username
    except Exception as e:
        logger.debug(f"get_peer_id/peer->entity failed for {user_id}: {e}")

    # 4. Самый "дорогой" запрос — GetFullUser (возвращает полные данные профиля)
    try:
        full = await user_client(GetFullUserRequest(user_id))
        if getattr(full.user, "username", None):
            return full.user.username
    except Exception as e:
        logger.debug(f"GetFullUserRequest failed for {user_id}: {e}")

    # 5. Можно ещё попробовать найти пользователя в чатах, где вы оба состоите (но это уже перебор)
    # Пример: user = await user_client.get_participants(chat, search=str(user_id))

    logger.warning(f"Could not resolve username for {user_id}")
    return None

def clean_text(text: str) -> str:
    """Убираем контакты: @юзеры, ссылки, телефоны."""
    t = text or ""
    t = re.sub(r"@\w{3,32}", "[hidden]", t)               # @username
    t = re.sub(r"https?://\S+|t\.me/\S+", "[hidden]", t)  # URL + t.me
    t = re.sub(r"\+?\d[\d\-\s()]{7,}", "[hidden]", t)     # телефоны
    return t.strip()

BUY_PATTERNS = (
    r"\bwtb\b", r"\bbuy\b", r"#wtb\b",
    r"\bneed\b", r"\blooking\s*for\b",
)
_buy_re = re.compile("|".join(BUY_PATTERNS), re.I)

def is_buy_message(text: str) -> bool:
    """Простая эвристика распознавания WTB/покупки."""
    if not text:
        return False
    # игнорируем типичные WTS-маркеры
    if re.search(r"\bwts\b|#wts\b|\bsell(ing)?\b", text, re.I):
        return False
    return bool(_buy_re.search(text))

# ——— Основная логика ———

async def main():
    init_db()


    # 1) Клиент-пользователь (чтение исходных чатов)
    user_client = TelegramClient(USER_SESSION, API_ID, API_HASH)
    await user_client.start()  # тут обязательно await
    me = await user_client.get_me()
    print(me)
    WATCH_CHATS = await get_chats_from_folder(user_client, "OTC")
    print(f"[init] папка OTC найдена, чатов: {len(WATCH_CHATS)}")

    chats_index = await build_chats_index(user_client, WATCH_CHATS, CHAT_INDEX_PATH)

    # 2) Клиент-бот (публикация в канал/группу)
    bot_client = TelegramClient(BOT_SESSION, API_ID, API_HASH)
    await bot_client.start(bot_token=BOT_TOKEN)

    @user_client.on(events.NewMessage(chats=WATCH_CHATS))
    async def on_new(event):
        msg = event.message
        chat_id = event.chat_id
        sender_id = event.sender_id
        text = msg.message or ""

        # 0) сначала ищем в БД
        from db import get_username_for_sender
        sender_username = get_username_for_sender(sender_id)


        # 1) если нет в БД — пробуем быстро получить из объекта
        if not sender_username:

            try:
                sender = await event.get_sender()
                sender_username = getattr(sender, "username", None)
            except Exception:
                sender_username = None

        # 2) если всё ещё нет — "тяжёлое" разрешение
        if not sender_username:
            sender_username = await resolve_username(user_client, sender_id)

        # reply-to
        reply_to_msg_id = None
        rt = getattr(msg, "reply_to", None)
        if rt:
            reply_to_msg_id = getattr(rt, "reply_to_msg_id", None) or getattr(rt, "reply_to_top_id", None)
        dup = exists_same_text_for_sender(sender_id, text)
        # сохраняем в БД (добавлен sender_username)
        row = save_message(
            message_id=msg.id,
            chat_id=chat_id,
            sender_id=sender_id,
            sender_username=sender_username,  # 👈 вот здесь
            ts_utc=msg.date,
            text=text,
            reply_to_msg_id=reply_to_msg_id,
        )

        print(
            f"[archive] chat={chat_id} id={row['id']} inserted={row['inserted']} "
            f"msg_id={msg.id} sender_id={sender_id} username={sender_username or '-'}"
        )

        # если это покупка — публикуем в целевую группу от имени бота
        if is_buy_message(text):

            if dup or len(text) > 300:
                print(f"[skip-post] duplicate for sender={sender_id} (same text seen before) or to long text")
            else:
                cleaned = clean_text(text)
                cleaned_safe = escape(cleaned)
                row_id = row["id"]
                sender_id = sender_id  # 👈 добавляем
                tags = extract_tags(cleaned)
                tags_line = ", ".join(tags) if tags else ""

                user_total_messages, user_reviews_count = get_user_stats(sender_id)

                # 0) берём свежий агрегат по пользователю
                likes, dislikes = get_user_reputation(sender_id)
                rating_pct = compute_rating_percent(likes, dislikes)
                stars_str = stars_from_percent(rating_pct)

                # 1) тело поста (с цитатами и актуальным рейтингом)
                parts = []
                parts.append("<b>💸 New WTB message</b>")

                parts.append(f"\n<b>About user ({stars_str}):</b>")
                parts.append(
                    "<blockquote>"
                    f"~ <i>User rating:</i> {rating_pct}%\n"
                    f"~ <i>Total messages:</i> {user_total_messages}\n"
                    f"~ <i>Number of reviews:</i> {user_reviews_count}"
                    "</blockquote>"
                )

                parts.append("<b>Text:</b>")
                parts.append(f"<blockquote>{cleaned_safe}</blockquote>")

                if tags_line:
                    parts.append(f"\n<i>#{'</i> <i>#'.join(t.lstrip('#') for t in tags)}</i>")

                body = "\n".join(parts)

                # 2) публикуем пост без кнопок
                posted = await bot_client.send_message(
                    entity=TARGET_GROUP,
                    message=body,
                    link_preview=False,
                    parse_mode="HTML",
                )

                otc_msg_id = posted.id
                start_payload = f"{row_id}_{otc_msg_id}"

                # (опционально) сохраним связку
                try:
                    save_published_post(row_id=row_id, chat_id=posted.chat_id, message_id=posted.id)
                except Exception:
                    pass

                # 3) реакции + контакт (ставим счётчики текущие для пользователя)
                buttons = [
                    [
                        Button.inline(f"✅ {likes}", data=f"like_{row_id}"),
                        Button.inline(f"❌ {dislikes}", data=f"dislike_{row_id}"),
                    ],
                    [
                        Button.url("💬 Contact buyer", f"https://t.me/otc_darwin_bot?start={start_payload}"),
                    ],
                ]

                await posted.edit(
                    text=body,
                    buttons=buttons,
                    link_preview=False,
                    parse_mode="HTML",
                )

    @user_client.on(events.MessageDeleted(chats=WATCH_CHATS))
    async def on_deleted(event):
        chat_id = event.chat_id
        ids = list(event.deleted_ids or [])
        if not chat_id or not ids:
            return
        updated = mark_deleted_in_archive(
            chat_id=chat_id,
            message_ids=ids,
            deleted_at=datetime.now(timezone.utc)
        )
        print(f"[deleted] chat={chat_id} ids={ids} marked_in_archive={updated}")

    print("collector running… (Ctrl+C для выхода)")
    await user_client.run_until_disconnected()

if __name__ == "__main__":
    asyncio.run(main())