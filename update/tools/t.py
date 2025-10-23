from telethon.tl.functions.messages import GetDialogFiltersRequest
from telethon.tl.types import DialogFilter
import logging
import re
import json
from pathlib import Path
from typing import Dict, List
logger = logging.getLogger("buy_detector")



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

def clean_text(text: str) -> str:
    """Убираем контакты: @юзеры, ссылки, телефоны."""
    t = text or ""
    t = re.sub(r"@\w{3,32}", "[hidden]", t)               # @username
    t = re.sub(r"https?://\S+|t\.me/\S+", "[hidden]", t)  # URL + t.me
    t = re.sub(r"\+?\d[\d\-\s()]{7,}", "[hidden]", t)     # телефоны
    return t.strip()


def compute_rating_percent(likes: int, dislikes: int) -> int:
    total = likes + dislikes
    score = 50.0 + 50.0 * ((likes - dislikes) / (total if total > 0 else 1))
    return max(0, min(100, round(score)))

def stars_from_percent(pct: int, max_stars: int = 5) -> str:
    full = int(round((pct / 100.0) * max_stars))
    full = max(0, min(max_stars, full))
    return "⭐" * full + "☆" * (max_stars - full)



def load_topics_map(path: str = "topics.json", general_topic_id: int = 1) -> tuple[dict[str, int], int]:
    """
    Загружает topics.json и возвращает:
      - словарь {title_lower: topic_id}
      - id главного топика (general)
    """
    file_path = Path(path)
    if not file_path.exists():
        raise FileNotFoundError(f"topics file not found: {file_path}")

    with file_path.open("r", encoding="utf-8") as f:
        topics_data = json.load(f)

    topic_map = {t["title"].lower(): t["topic_id"] for t in topics_data.get("topics", [])}
    return topic_map, general_topic_id


TOPIC_MAP, GENERAL_TOPIC_ID = load_topics_map("topics.json", general_topic_id=1)

def get_destinations(
    text: str,
    topic_map: Dict[str, int] = TOPIC_MAP,
    general_topic_id: int = GENERAL_TOPIC_ID,
    max_hits: int = 10,
) -> List[int]:
    """
    Определяет список топиков, куда публиковать сообщение.
    Всегда включает general_topic_id.
    Возвращает отсортированный список topic_id.
    """
    text_l = f" {text.lower()} "
    hits = {general_topic_id}

    for title, tid in topic_map.items():
        pattern = r"(?<!\w)" + re.escape(title) + r"(?!\w)"
        if re.search(pattern, text_l):
            hits.add(tid)
            if len(hits) >= (max_hits + 1):  # +1 за general
                break

    return sorted(hits)