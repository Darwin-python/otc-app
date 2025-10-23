# db.py
import os, hashlib
from datetime import datetime, timezone
import psycopg
from psycopg.rows import dict_row

PG_DSN = os.getenv("PG_DSN", "postgresql://otc_user:supersecret@localhost:5432/otc_app")

CREATE_SQL = """
CREATE SCHEMA IF NOT EXISTS otc;

CREATE TABLE IF NOT EXISTS otc.messages_archive (
    id               BIGSERIAL PRIMARY KEY,
    message_id       BIGINT      NOT NULL,
    chat_id          BIGINT      NOT NULL,
    sender_id        BIGINT      NOT NULL,
    sender_username  TEXT        NULL,                -- 👈 новое поле
    ts_utc           TIMESTAMPTZ NOT NULL,
    text             TEXT        NOT NULL,
    processed        BOOLEAN     NOT NULL DEFAULT FALSE,
    text_hash        TEXT        NOT NULL,
    duplicates_count INTEGER     NOT NULL DEFAULT 0,
    deleted          BOOLEAN     NOT NULL DEFAULT FALSE,
    deleted_at       TIMESTAMPTZ NULL,
    reply_to_msg_id  BIGINT      NULL,
    CONSTRAINT uniq_chat_sender_text UNIQUE (chat_id, sender_id, text_hash)
);

CREATE INDEX IF NOT EXISTS idx_messages_archive_chat_id   ON otc.messages_archive (chat_id);
CREATE INDEX IF NOT EXISTS idx_messages_archive_ts        ON otc.messages_archive (ts_utc DESC);
CREATE INDEX IF NOT EXISTS idx_messages_archive_processed ON otc.messages_archive (processed);
CREATE INDEX IF NOT EXISTS idx_messages_archive_deleted   ON otc.messages_archive (deleted);
CREATE INDEX IF NOT EXISTS idx_messages_archive_reply_to  ON otc.messages_archive (reply_to_msg_id);
CREATE INDEX IF NOT EXISTS idx_messages_archive_username  ON otc.messages_archive (sender_username);


-- реакции под карточками (взаимоисключающие: 1 = like, -1 = dislike)
CREATE TABLE IF NOT EXISTS otc.listing_reaction (
    row_id   BIGINT      NOT NULL,
    user_id  BIGINT      NOT NULL,
    reaction SMALLINT    NOT NULL CHECK (reaction IN (-1, 1)),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (row_id, user_id)
);
CREATE INDEX IF NOT EXISTS idx_listing_reaction_row ON otc.listing_reaction (row_id);

-- где опубликована карточка (чтобы знать какое сообщение редактировать)
CREATE TABLE IF NOT EXISTS otc.published_post (
    row_id     BIGINT      PRIMARY KEY,      -- твой внутренний id (из messages_archive.id или иной)
    chat_id    BIGINT      NOT NULL,
    message_id BIGINT      NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_published_post_msg ON otc.published_post (chat_id, message_id);

CREATE TABLE IF NOT EXISTS otc.user_reputation (
    user_id   BIGINT PRIMARY KEY,
    likes     INT NOT NULL DEFAULT 0,
    dislikes  INT NOT NULL DEFAULT 0,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
"""

MIGRATE_SQL = """
DO $$
BEGIN
    -- user_reputation
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.tables
        WHERE table_schema='otc' AND table_name='user_reputation'
    ) THEN
        CREATE TABLE otc.user_reputation (
            user_id   BIGINT PRIMARY KEY,
            likes     INT NOT NULL DEFAULT 0,
            dislikes  INT NOT NULL DEFAULT 0,
            updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
        );
    END IF;
    -- listing_reaction
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.tables
        WHERE table_schema='otc' AND table_name='listing_reaction'
    ) THEN
        CREATE TABLE otc.listing_reaction (
            row_id   BIGINT      NOT NULL,
            user_id  BIGINT      NOT NULL,
            reaction SMALLINT    NOT NULL CHECK (reaction IN (-1, 1)),
            created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            PRIMARY KEY (row_id, user_id)
        );
        CREATE INDEX IF NOT EXISTS idx_listing_reaction_row ON otc.listing_reaction (row_id);
    END IF;
    
    -- published_post
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.tables
        WHERE table_schema='otc' AND table_name='published_post'
    ) THEN
        CREATE TABLE otc.published_post (
            row_id     BIGINT      PRIMARY KEY,
            chat_id    BIGINT      NOT NULL,
            message_id BIGINT      NOT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now()
        );
        CREATE INDEX IF NOT EXISTS idx_published_post_msg ON otc.published_post (chat_id, message_id);
    END IF;
    
    -- sender_username
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema='otc' AND table_name='messages_archive' AND column_name='sender_username'
    ) THEN
        ALTER TABLE otc.messages_archive ADD COLUMN sender_username TEXT NULL;
        CREATE INDEX IF NOT EXISTS idx_messages_archive_username ON otc.messages_archive (sender_username);
    END IF;

    -- duplicates_count
    BEGIN
        ALTER TABLE otc.messages_archive ADD COLUMN duplicates_count INTEGER NOT NULL DEFAULT 0;
    EXCEPTION WHEN duplicate_column THEN END;

    -- deleted / deleted_at
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema='otc' AND table_name='messages_archive' AND column_name='deleted'
    ) THEN
        ALTER TABLE otc.messages_archive ADD COLUMN deleted BOOLEAN NOT NULL DEFAULT FALSE;
        CREATE INDEX IF NOT EXISTS idx_messages_archive_deleted ON otc.messages_archive (deleted);
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema='otc' AND table_name='messages_archive' AND column_name='deleted_at'
    ) THEN
        ALTER TABLE otc.messages_archive ADD COLUMN deleted_at TIMESTAMPTZ NULL;
    END IF;

    -- reply_to_msg_id
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema='otc' AND table_name='messages_archive' AND column_name='reply_to_msg_id'
    ) THEN
        ALTER TABLE otc.messages_archive ADD COLUMN reply_to_msg_id BIGINT NULL;
        CREATE INDEX IF NOT EXISTS idx_messages_archive_reply_to ON otc.messages_archive (reply_to_msg_id);
    END IF;

    -- заменить старый уникальный ключ, если остался
    IF EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid='otc.messages_archive'::regclass AND conname='uniq_sender_text'
    ) THEN
        ALTER TABLE otc.messages_archive DROP CONSTRAINT uniq_sender_text;
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid='otc.messages_archive'::regclass AND conname='uniq_chat_sender_text'
    ) THEN
        ALTER TABLE otc.messages_archive
          ADD CONSTRAINT uniq_chat_sender_text UNIQUE (chat_id, sender_id, text_hash);
    END IF;
END$$;
"""

UPSERT_SQL = """
INSERT INTO otc.messages_archive
    (message_id, chat_id, sender_id, sender_username, ts_utc, text, text_hash, reply_to_msg_id)
VALUES
    (%(message_id)s, %(chat_id)s, %(sender_id)s, %(sender_username)s, %(ts_utc)s, %(text)s, %(text_hash)s, %(reply_to_msg_id)s)
ON CONFLICT (chat_id, sender_id, text_hash) DO UPDATE
SET duplicates_count = otc.messages_archive.duplicates_count + 1,
    -- обновляем username если появился новый
    sender_username = COALESCE(EXCLUDED.sender_username, otc.messages_archive.sender_username),
    reply_to_msg_id = COALESCE(otc.messages_archive.reply_to_msg_id, EXCLUDED.reply_to_msg_id)
RETURNING id, (xmax = 0) AS inserted, duplicates_count;
"""

UPDATE_DELETED_SQL = """
UPDATE otc.messages_archive
SET deleted = TRUE,
    deleted_at = COALESCE(deleted_at, %(deleted_at)s)
WHERE chat_id = %(chat_id)s
  AND message_id = ANY(%(message_ids)s)
RETURNING id;
"""

# REACTIONS
UPSERT_REACTION_SQL = """
INSERT INTO otc.listing_reaction (row_id, user_id, reaction)
VALUES (%(row_id)s, %(user_id)s, %(reaction)s)
ON CONFLICT (row_id, user_id) DO UPDATE
SET reaction = EXCLUDED.reaction,
    created_at = now()
RETURNING reaction;
"""

DELETE_REACTION_SQL = """
DELETE FROM otc.listing_reaction
WHERE row_id = %(row_id)s AND user_id = %(user_id)s
RETURNING 1;
"""

GET_REACTION_SQL = """
SELECT reaction FROM otc.listing_reaction
WHERE row_id = %(row_id)s AND user_id = %(user_id)s
"""

COUNT_REACTIONS_SQL = """
SELECT
  COUNT(*) FILTER (WHERE reaction = 1)  AS likes,
  COUNT(*) FILTER (WHERE reaction = -1) AS dislikes
FROM otc.listing_reaction
WHERE row_id = %(row_id)s
"""

# PUBLISHED POST
UPSERT_PUBLISHED_POST_SQL = """
INSERT INTO otc.published_post (row_id, chat_id, message_id)
VALUES (%(row_id)s, %(chat_id)s, %(message_id)s)
ON CONFLICT (row_id) DO UPDATE
SET chat_id = EXCLUDED.chat_id,
    message_id = EXCLUDED.message_id
RETURNING row_id;
"""

GET_PUBLISHED_POST_SQL = """
SELECT chat_id, message_id
FROM otc.published_post
WHERE row_id = %(row_id)s
"""



def init_db():
    with psycopg.connect(PG_DSN, row_factory=dict_row) as conn, conn.cursor() as cur:
        cur.execute(CREATE_SQL)
        cur.execute(MIGRATE_SQL)
        conn.commit()

def _sha256(t: str) -> str:
    return hashlib.sha256((t or "").encode("utf-8")).hexdigest()

def save_message(*, message_id: int, chat_id: int, sender_id: int, ts_utc, text: str,
                 reply_to_msg_id: int | None, sender_username: str | None = None):
    norm = " ".join((text or "").strip().split())
    h = _sha256(norm)
    with psycopg.connect(PG_DSN, row_factory=dict_row) as conn, conn.cursor() as cur:
        cur.execute(
            UPSERT_SQL,
            {
                "message_id": message_id,
                "chat_id": chat_id,
                "sender_id": sender_id,
                "sender_username": sender_username,
                "ts_utc": ts_utc,
                "text": text,
                "text_hash": h,
                "reply_to_msg_id": reply_to_msg_id,
            },
        )
        res = cur.fetchone()
        conn.commit()
    return res

def mark_deleted_in_archive(*, chat_id: int, message_ids: list[int], deleted_at: datetime | None = None) -> int:
    if not message_ids:
        return 0
    if deleted_at is None:
        deleted_at = datetime.now(timezone.utc)
    with psycopg.connect(PG_DSN, row_factory=dict_row) as conn, conn.cursor() as cur:
        cur.execute(UPDATE_DELETED_SQL, {
            "chat_id": chat_id,
            "message_ids": message_ids,
            "deleted_at": deleted_at,
        })
        n = cur.rowcount
        conn.commit()
    return n

def get_message_by_id(msg_id: int):
    with psycopg.connect(PG_DSN, row_factory=dict_row) as conn, conn.cursor() as cur:
        cur.execute("SELECT * FROM otc.messages_archive WHERE id = %s", (msg_id,))
        return cur.fetchone()


def exists_same_text_for_sender(sender_id: int, text: str) -> bool:
    """Проверяет точное совпадение text (без нормализации) для данного sender_id.
    Дополнительно логирует найденные совпадения.
    """
    with psycopg.connect(PG_DSN, row_factory=dict_row) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT id, message_id, ts_utc, octet_length(text) AS len, text
            FROM otc.messages_archive
            WHERE sender_id = %s AND text = %s
            ORDER BY ts_utc DESC
            LIMIT 1
            """,
            (sender_id, text),
        )
        row = cur.fetchone()

        if row:
            return True
        else:
            return False

def get_username_for_sender(sender_id: int) -> str | None:
    """Возвращает любой известный username по sender_id из архива сообщений."""
    with psycopg.connect(PG_DSN, row_factory=dict_row) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT sender_username
            FROM otc.messages_archive
            WHERE sender_id = %s AND sender_username IS NOT NULL
            ORDER BY ts_utc DESC
            LIMIT 1
            """,
            (sender_id,),
        )
        row = cur.fetchone()
        if row and row.get("sender_username"):
            return row["sender_username"].lstrip("@")
        return None

def save_published_post(*, row_id: int, chat_id: int, message_id: int):
    with psycopg.connect(PG_DSN, row_factory=dict_row) as conn, conn.cursor() as cur:
        cur.execute(UPSERT_PUBLISHED_POST_SQL, {
            "row_id": row_id, "chat_id": chat_id, "message_id": message_id
        })
        conn.commit()

def get_published_post(row_id: int) -> dict | None:
    with psycopg.connect(PG_DSN, row_factory=dict_row) as conn, conn.cursor() as cur:
        cur.execute(GET_PUBLISHED_POST_SQL, {"row_id": row_id})
        return cur.fetchone()

def get_reaction(row_id: int, user_id: int) -> int | None:
    with psycopg.connect(PG_DSN, row_factory=dict_row) as conn, conn.cursor() as cur:
        cur.execute(GET_REACTION_SQL, {"row_id": row_id, "user_id": user_id})
        r = cur.fetchone()
        return None if not r else int(r["reaction"])

def set_reaction(row_id: int, user_id: int, reaction: int):
    """Установить/изменить реакцию (1 или -1)."""
    with psycopg.connect(PG_DSN, row_factory=dict_row) as conn, conn.cursor() as cur:
        cur.execute(UPSERT_REACTION_SQL, {
            "row_id": row_id, "user_id": user_id, "reaction": reaction
        })
        conn.commit()

def remove_reaction(row_id: int, user_id: int) -> bool:
    with psycopg.connect(PG_DSN, row_factory=dict_row) as conn, conn.cursor() as cur:
        cur.execute(DELETE_REACTION_SQL, {"row_id": row_id, "user_id": user_id})
        deleted = cur.rowcount > 0
        conn.commit()
        return deleted

def count_reactions(row_id: int) -> tuple[int, int]:
    with psycopg.connect(PG_DSN, row_factory=dict_row) as conn, conn.cursor() as cur:
        cur.execute(COUNT_REACTIONS_SQL, {"row_id": row_id})
        r = cur.fetchone() or {"likes": 0, "dislikes": 0}
        return int(r["likes"] or 0), int(r["dislikes"] or 0)

def toggle_reaction(row_id: int, user_id: int, new_reaction: int) -> str:
    cur = get_reaction(row_id, user_id)
    if cur is None:
        set_reaction(row_id, user_id, new_reaction)
        result = "added"
    elif cur == new_reaction:
        remove_reaction(row_id, user_id)
        result = "removed"
    else:
        set_reaction(row_id, user_id, new_reaction)
        result = "switched"

    # обновляем агрегат по sender_id
    # сначала узнаем чей это row_id
    msg = get_message_by_id(row_id)
    if msg:
        update_user_reputation(msg["sender_id"])

    return result

def get_user_stats(sender_id: int) -> tuple[int, int]:
    """
    Возвращает (total_messages, reviews_count) для пользователя.
    total_messages: количество сообщений в messages_archive
    reviews_count: количество всех реакций (like/dislike) на эти сообщения
    """
    with psycopg.connect(PG_DSN, row_factory=dict_row) as conn, conn.cursor() as cur:
        # сколько сообщений
        cur.execute(
            "SELECT COUNT(*) AS cnt FROM otc.messages_archive WHERE sender_id=%s AND deleted=FALSE",
            (sender_id,)
        )
        total_messages = cur.fetchone()["cnt"]

        # сколько отзывов (реакций)
        cur.execute(
            """
            SELECT COUNT(*) AS cnt
            FROM otc.listing_reaction lr
            JOIN otc.messages_archive ma ON lr.row_id = ma.id
            WHERE ma.sender_id=%s
            """,
            (sender_id,)
        )
        reviews_count = cur.fetchone()["cnt"]

    return total_messages, reviews_count

def get_user_reputation(user_id: int) -> tuple[int, int]:
    """Возвращает (likes, dislikes) для пользователя."""
    with psycopg.connect(PG_DSN, row_factory=dict_row) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT likes, dislikes FROM otc.user_reputation WHERE user_id=%s",
            (user_id,)
        )
        row = cur.fetchone()
        if not row:
            return 0, 0
        return int(row["likes"]), int(row["dislikes"])

def update_user_reputation(user_id: int):
    """Пересчитывает total лайки/дизлайки из listing_reaction и обновляет user_reputation."""
    with psycopg.connect(PG_DSN, row_factory=dict_row) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT
              COUNT(*) FILTER (WHERE reaction = 1)  AS likes,
              COUNT(*) FILTER (WHERE reaction = -1) AS dislikes
            FROM otc.listing_reaction lr
            JOIN otc.messages_archive ma ON lr.row_id = ma.id
            WHERE ma.sender_id = %s
            """,
            (user_id,)
        )
        row = cur.fetchone()
        likes, dislikes = int(row["likes"] or 0), int(row["dislikes"] or 0)

        cur.execute(
            """
            INSERT INTO otc.user_reputation (user_id, likes, dislikes, updated_at)
            VALUES (%s, %s, %s, now())
            ON CONFLICT (user_id) DO UPDATE
            SET likes=EXCLUDED.likes,
                dislikes=EXCLUDED.dislikes,
                updated_at=now()
            """,
            (user_id, likes, dislikes),
        )
        conn.commit()
    return likes, dislikes