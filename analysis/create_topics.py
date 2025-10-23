# create_topics_bot.py
import asyncio
import json
import os
import re
from datetime import datetime
from dotenv import load_dotenv
from telegram import Bot

load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN", "")
CHAT_ID = os.getenv("TARGET_GROUP", "")  # -100... или @username
OUTPUT_JSON = os.getenv("TOPICS_JSON", "topics.json")

assert BOT_TOKEN and CHAT_ID, "Fill .env: BOT_TOKEN, TARGET_GROUP"

TOPICS = [
    # 🔹 Криптобиржи
    "binance","bybit","okx","bitget","kucoin","coinbase","mexc","kraken","gate","huobi htx",
    "whitebit","exmo","bitmart","bingx","bitstamp","lbank","poloniex","bitmex","uphold",
    "cryptocom","nexo","coinex","paxful","localbitcoins","hotbit","latoken","deribit",
    "gemini","bitrue","bittrex",

    # 🔹 KYC / ID providers
    "sumsub","onfido","shufti","idnow","trulioo","persona","blockpass","holonym","fragment","coinlist",
    "veriff","identitymind","passbase","idenfy","ekyc",

    # 🔹 Платёжки / финтех / банки
    "wise","revolut","skrill","neteller","jeton","advcash","payoneer","moonpay","ramp","redotpay",
    "paysera","stripe","bunq","n26","vivid","finom","ing","santander","bbva","payoneer business",
    "blackcatcard","icard","genome","wirex","airwallex","cashapp","venmo","zelle","paypal","mercadopago",
    "oxxo","upi","pix","monzo","starling bank","tinkoff","alipay","wechat pay","yoomoney","qiwi",
    "paysend","worldremit","western union","moneygram",

    # 🔹 Маркетплейсы / сервисы
    "tiktok shop","shopee","amazon","ebay","aliexpress","airbnb","ozon","temu","bet365","cashapp",
    "etsy","shopify","lazada","walmart","target","zalando","shein","netflix","spotify","apple","google play",

    # 🔹 Гео / страны (ключевые регионы)
    "usa","spain","germany","italy","france","ukraine","romania","poland","argentina","mexico",
    "philippines","nigeria","kenya","egypt","indonesia","brazil","turkey","thailand","vietnam",
    "india","pakistan","bangladesh","colombia","venezuela","peru","chile","ecuador","morocco",
    "algeria","south africa","jordan","armenia","latvia","uruguay","paraguay","bolivia","myanmar",

    # 🔹 Документы / идентификация
    "iban","passport","llc","vcc","esim","sim card","drivers license","utility bill","bank statement",
    "id card","residence permit","company incorporation","certificate of incumbency",

    # 🔹 Прокси / инструменты / прочее
    "astroproxy","trustee","cryptomus","solayer","buidlpad","buildpad","game bcgame","galxe",
    "notary","sandbox","proxy emulator","linkedin","old tg groups","emulator","android emulator",
    "antidetect browser","multiaccounting","cookies","rental id","synthetic id"
]

def slugify(title: str) -> str:
    t = title.lower().strip()
    t = t.replace("&", "and")
    t = re.sub(r"[^\w\s+-]", "", t)
    t = re.sub(r"\s+", " ", t)
    return t

async def main():
    bot = Bot(token=BOT_TOKEN)

    # загружаем уже созданные топики (если есть)
    topics_map = {}
    if os.path.exists(OUTPUT_JSON):
        with open(OUTPUT_JSON, "r", encoding="utf-8") as f:
            topics_map = {t["slug"]: t for t in json.load(f).get("topics", [])}

    for idx, title in enumerate(TOPICS, 1):
        slug = slugify(title)
        if slug in topics_map:
            print(f"⏩ Пропускаю '{title}' — уже есть")
            continue

        try:
            res = await bot.create_forum_topic(
                chat_id=CHAT_ID,
                name=title[:128]
            )
            topic_id = res.message_thread_id
            topics_map[slug] = {
                "title": title,
                "topic_id": topic_id,
                "created_at": datetime.utcnow().isoformat() + "Z"
            }
            print(f"✅ Создан топик: {title} → {topic_id}")
        except Exception as e:
            print(f"⚠️ Ошибка при создании '{title}': {e}")

        # пауза после каждой попытки
        await asyncio.sleep(3)

        # каждые 20 топиков — длинная пауза
        if idx % 20 == 0:
            print("⏳ Пауза 60 секунд, чтобы не словить flood...")
            await asyncio.sleep(60)

    payload = {
        "channel": {"id": CHAT_ID},
        "generated_at": datetime.utcnow().isoformat() + "Z",
        "topics": list(topics_map.values())
    }

    with open(OUTPUT_JSON, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)

    print(f"\nСохранено в {OUTPUT_JSON} ({len(payload['topics'])} тем)")

if __name__ == "__main__":
    asyncio.run(main())