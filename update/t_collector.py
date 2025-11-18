import os
import asyncio
import random
from db import (init_db,
                exists_same_text_for_sender,
                save_message,
                get_user_stats,
                get_user_reputation,
                save_published_post)

from telethon import TelegramClient, events, Button
from telethon.errors import FloodWaitError
from html import escape

from tools.t import (get_chats_from_folder,
                     resolve_username,
                     is_buy_message,
                     clean_text,
                     compute_rating_percent,
                     stars_from_percent,
                     get_destinations)

from telethon.sessions import SQLiteSession
from dotenv import load_dotenv
load_dotenv()

API_ID = int(os.getenv("API_ID"))
API_HASH = os.getenv("API_HASH")
USER_SESSION_PATH = "sessions/otc_user.session"

BOT_TOKEN = os.getenv("BOT_TOKEN")
BOT_SESSION_PATH = "sessions/otc_bot.session"
TARGET_GROUP = os.getenv("TARGET_GROUP")


async def main():
    init_db()

    # клиент-пользователь (читает OTC чаты + будет автопостинг)
    user_client = TelegramClient(SQLiteSession(USER_SESSION_PATH), API_ID, API_HASH)
    await user_client.start()

    # читаем список чатов в папке OTC
    WATCH_CHATS = await get_chats_from_folder(user_client, "OTC")
    print(f"[init] папка OTC найдена, чатов: {len(WATCH_CHATS)}")

    # клиент-бот — публикует в твой OTC канал
    bot_client = TelegramClient(BOT_SESSION_PATH, API_ID, API_HASH)
    await bot_client.start(bot_token=BOT_TOKEN)

    # ===============================================================
    # 🔥 АВТОПОСТИНГ: 1 раз в 3 часа делает круг по всем чатам
    # ===============================================================
    async def autopost_loop():
        POST_TEXT = (
            "<b>WTB/WTS</b>\n"
            "Услуги студии разработки!\n"
            "<b>Development studio services!</b>\n\n"
            "Пишите в лс\n"
            "<b>DM</b>"
        )

        WAIT_BETWEEN_ROUNDS = 3 * 3600  # 3 часа

        while True:
            print("\n=== 🔄 AUTPOST: Новый круг публикаций начат ===")

            for chat in WATCH_CHATS:
                chat_id = chat

                try:
                    print(f"[autopost] Публикую в чат: {chat_id}")
                    await user_client.send_message(
                        chat_id,
                        POST_TEXT,
                        parse_mode="HTML",
                        link_preview=False
                    )

                except FloodWaitError as e:
                    print(f"[autopost] ⚠️ FloodWait {e.seconds}s — жду...")
                    await asyncio.sleep(e.seconds)
                except Exception as e:
                    print(f"[autopost ERROR] {e}")

                # задержка 40–90 сек между чатами
                delay = random.randint(40, 90)
                print(f"[autopost] Жду {delay} сек…")
                await asyncio.sleep(delay)

            print("=== ⏳ AUTPOST: Круг завершён. Ожидание 3 часа... ===\n")
            await asyncio.sleep(WAIT_BETWEEN_ROUNDS)

    # запускаем автопостинг как параллельную задачу
    asyncio.create_task(autopost_loop())

    # ===============================================================
    # 🔥 ЛОВИМ НОВЫЕ WTB/WTS и постим в твой OTC канал
    # ===============================================================
    @user_client.on(events.NewMessage(chats=WATCH_CHATS))
    async def on_new(event):
        msg = event.message
        chat_id = event.chat_id
        sender_id = event.sender_id
        text = msg.message or ""

        from db import get_username_for_sender
        sender_username = get_username_for_sender(sender_id)

        if not sender_username:
            try:
                sender = await event.get_sender()
                sender_username = getattr(sender, "username", None)
            except Exception:
                sender_username = None

        if not sender_username:
            sender_username = await resolve_username(user_client, sender_id)

        # поиск reply
        reply_to_msg_id = None
        rt = getattr(msg, "reply_to", None)
        if rt:
            reply_to_msg_id = getattr(rt, "reply_to_msg_id", None) or getattr(rt, "reply_to_top_id", None)

        # проверка дубликата
        dup = exists_same_text_for_sender(sender_id, text)

        # сохраняем в бд
        row = save_message(
            message_id=msg.id,
            chat_id=chat_id,
            sender_id=sender_id,
            sender_username=sender_username,
            ts_utc=msg.date,
            text=text,
            reply_to_msg_id=reply_to_msg_id,
        )

        print(
            f"[archive] chat={chat_id} id={row['id']} inserted={row['inserted']} "
            f"msg_id={msg.id} sender_id={sender_id} username={sender_username or '-'}"
        )

        if is_buy_message(text):
            if dup or len(text) > 300:
                print(f"[skip-post] duplicate for sender={sender_id} или слишком длинный текст")
                return

            cleaned = clean_text(text)
            cleaned_safe = escape(cleaned)
            row_id = row["id"]

            user_total_messages, user_reviews_count = get_user_stats(sender_id)

            likes, dislikes = get_user_reputation(sender_id)
            rating_pct = compute_rating_percent(likes, dislikes)
            stars_str = stars_from_percent(rating_pct)
            topics = get_destinations(text)

            # формируем красивый пост
            parts = []
            parts.append("<b>💸 New WTB message</b>\n")
            parts.append(f"<b>About user ({stars_str}):</b>")
            parts.append(
                "<blockquote>"
                f"~ <i>User rating:</i> {rating_pct}%\n"
                f"~ <i>Total messages:</i> {user_total_messages}\n"
                f"~ <i>Number of reviews:</i> {user_reviews_count}"
                "</blockquote>"
            )
            parts.append("<b>Text:</b>")
            parts.append(f"<blockquote>{cleaned_safe}</blockquote>")

            body = "\n".join(parts)

            for topic_id in topics:
                posted = await bot_client.send_message(
                    entity=TARGET_GROUP,
                    message=body,
                    link_preview=False,
                    parse_mode="HTML",
                    reply_to=topic_id,
                )

                otc_msg_id = posted.id
                start_payload = f"{row_id}_{otc_msg_id}"

                try:
                    save_published_post(row_id=row_id, chat_id=posted.chat_id, message_id=posted.id)
                except Exception:
                    pass

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

                await asyncio.sleep(0.3)

    print("collector running… (Ctrl+C для выхода)")
    await user_client.run_until_disconnected()


if __name__ == "__main__":
    asyncio.run(main())