import os
import asyncio
from db import (init_db,
                exists_same_text_for_sender,
                save_message,
                get_user_stats,
                get_user_reputation,
                save_published_post)

from telethon import TelegramClient, events, Button, types
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

BOT_TOKEN = (os.getenv("BOT_TOKEN"))
BOT_SESSION_PATH = "sessions/otc_bot.session"
TARGET_GROUP = (os.getenv("TARGET_GROUP"))


async def main():
    init_db()

    # 1) Клиент-пользователь (чтение исходных чатов)
    user_client = TelegramClient(SQLiteSession(USER_SESSION_PATH), API_ID, API_HASH)
    await user_client.start()
    # 2) Чтение OTC чатов из папки
    WATCH_CHATS = await get_chats_from_folder(user_client, "OTC")
    print(f"[init] папка OTC найдена, чатов: {len(WATCH_CHATS)}")

    # 3) Клиент-бот (публикация в канал/группу)
    bot_client = TelegramClient(BOT_SESSION_PATH, API_ID, API_HASH)
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

        if is_buy_message(text):
            if dup or len(text) > 300:
                print(f"[skip-post] duplicate for sender={sender_id} (same text seen before) or to long text")
            else:
                cleaned = clean_text(text)
                cleaned_safe = escape(cleaned)
                row_id = row["id"]
                sender_id = sender_id  # 👈 добавляем

                user_total_messages, user_reviews_count = get_user_stats(sender_id)

                # 0) берём свежий агрегат по пользователю
                likes, dislikes = get_user_reputation(sender_id)
                rating_pct = compute_rating_percent(likes, dislikes)
                stars_str = stars_from_percent(rating_pct)
                topics = get_destinations(text)

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



                body = "\n".join(parts)
                print(topics)
                for topic_id in topics:  # topics: Iterable[int]
                    # 1) отправляем пост в нужный топик (через reply_to = top_msg_id)
                    posted = await bot_client.send_message(
                        entity=TARGET_GROUP,
                        message=body,
                        link_preview=False,
                        parse_mode="HTML",
                        reply_to=topic_id,  # 👈 главное изменение: просто int
                    )

                    otc_msg_id = posted.id
                    start_payload = f"{row_id}_{otc_msg_id}"

                    # 2) сохраняем связку
                    try:
                        save_published_post(row_id=row_id, chat_id=posted.chat_id, message_id=posted.id)
                    except Exception:
                        pass

                    # 3) кнопки
                    buttons = [
                        [
                            Button.inline(f"✅ {likes}", data=f"like_{row_id}"),
                            Button.inline(f"❌ {dislikes}", data=f"dislike_{row_id}"),
                        ],
                        [
                            Button.url("💬 Contact buyer", f"https://t.me/otc_darwin_bot?start={start_payload}"),
                        ],
                    ]

                    # можно сразу отправлять с buttons=..., но если нужен реальный message_id в диплинке — оставляем edit
                    await posted.edit(
                        text=body,
                        buttons=buttons,
                        link_preview=False,
                        parse_mode="HTML",
                    )

                    await asyncio.sleep(0.3)  # анти-флуд

    print("collector running… (Ctrl+C для выхода)")
    await user_client.run_until_disconnected()

if __name__ == "__main__":
    asyncio.run(main())

