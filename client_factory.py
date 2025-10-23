# client_factory.py
import os, asyncio
from getpass import getpass
from telethon import TelegramClient
from telethon.errors import SessionPasswordNeededError

API_ID = 29635442
API_HASH = "7db8d1a90eacb8c2203fc4b2c728c216"
SESSION_PATH = os.path.abspath("otc_user.session")

async def get_client() -> TelegramClient:
    client = TelegramClient(
        SESSION_PATH, API_ID, API_HASH,
        device_model="iPhone 14 Pro",
        system_version="iOS 17.5",
        app_version="10.10",
        lang_code="en", system_lang_code="en-US"
    )
    await client.connect()

    if not await client.is_user_authorized():
        # интерактивный логин
        phone = input("Введите телефон (+380...): ").strip()
        await client.send_code_request(phone)
        code = input("Код из Telegram: ").strip()
        try:
            await client.sign_in(phone=phone, code=code)
        except SessionPasswordNeededError:
            pwd = getpass("Пароль 2FA: ")
            await client.sign_in(password=pwd)

        me = await client.get_me()
        print(f"Авторизация успешна: @{me.username or me.first_name} (id={me.id})")
    else:
        me = await client.get_me()
        print(f"Сессия найдена: @{me.username or me.first_name} (id={me.id})")

    return client

# пример использования
async def main():
    client = await get_client()
    async with client:
        dialogs = await client.get_dialogs(limit=5)
        for d in dialogs:
            print("Диалог:", d.name)

if __name__ == "__main__":
    asyncio.run(main())