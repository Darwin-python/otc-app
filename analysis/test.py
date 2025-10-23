# analyze_wtb.py
import os
import re
import json
import csv
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Tuple, Any, Iterable, Optional

import psycopg
from psycopg.rows import dict_row

PG_DSN = os.getenv("PG_DSN", "postgresql://otc_user:supersecret@localhost:5432/otc_app")

# --- словари ключевых слов (можешь расширять) ---
KNOWN_ITEMS: Dict[str, List[str]] = {
    "exchanges": [
        "binance","bybit","okx","huobi","htx","gate io","bitget","mexc","kucoin","bingx",
        "coinlist","paxful","cryptocom","crypto com","bc game","bcgame","fragment","weex","arkham"
    ],
    "payments_banks": [
        "bunq","n26","monzo","santander","bbva","ing","finom","vivid","chase","c24",
        "trade republic","billions","bank of america",
        "revolut","revolut business","revolut personal",
        "wise","wise business","wise personal",
        "paysera","icard","zen","zen business",
        "airwallex","mercury","bitsa","wirex","genome","sumup","persona","trustee","trustee plus",
        "stripe","stripe business","paypal","paypal business",
        "cashapp","alipay","nexo","ozon","twitter"
    ],
    "kyc_verification": [
        "kyc","wts kyc","kyc service","kyc or ready","kyc bybit","kyc service or","kyc accepted",
        "kyc / acc","kyc / ready","kyc or","kyc by","wts kyc service","wts kyc or","other kyc",
        "other kyc accepted","kyc 8","blockpass","persona","sumsub","onfido",
        "holonym","buildpad","buidlpad","solayer","kaito","arkham","legion","civic","sandbox","echo"
    ],
    "marketplaces_services": [
        "fragment","tiktok","temu","ozon","airbnb","telegram","twitter","whatsapp","esim",
        "game","bc game","bcgame","bet365"
    ],
    "crypto_wallets": [
        "metamask","trustwallet","phantom","coinbase wallet","ledger","trezor","tronlink"
    ],
    "countries": [
        "usa","indonesia","spain","egypt","philippines","uganda","georgia","america","germany",
        "armenia","africa","russia","costa rica","italy","zambia","vietnam","rwanda","angola",
        "uruguay","paraguay","argentina","bolivia","peru","brazil","chile","colombia",
        "el salvador","mexico","india","nigeria","pakistan","bangladesh","nepal","kenya"
    ],
    "misc": [
        "iban","llc","emulator","passport","vcc","accs","accounts","ready account","ready acc",
        "old account","merchant","premium","crypto","wallet","stake","escrow","reviews","selfie","verification"
    ],
}

# --- эвристика WTB ---
BUY_PATTERNS = (
    r"\bwtb\b", r"#wtb\b", r"\bneed(s|ed)?\b", r"\bbuy(ing)?\b", r"\blooking\s*for\b",
)
SELL_NEG = r"\bwts\b|#wts\b|\bsell(ing)?\b"

_buy_re = re.compile("|".join(BUY_PATTERNS), re.I)
_sellneg_re = re.compile(SELL_NEG, re.I)

def is_wtb(text: str) -> bool:
    if not text:
        return False
    if _sellneg_re.search(text):
        return False
    return bool(_buy_re.search(text))

def normalize_spaces(s: str) -> str:
    return " ".join((s or "").split())

def extract_tags(text: str) -> Dict[str, List[str]]:
    """
    Возвращает: {category: [#tag, ...]} — без дубликатов в рамках категории.
    """
    tags: Dict[str, List[str]] = {}
    lowered = (text or "").lower()
    for cat, words in KNOWN_ITEMS.items():
        found = []
        for w in words:
            if w in lowered:
                found.append("#" + w.replace(" ", "_"))
        if found:
            # уникальные в категории
            tags[cat] = sorted(list(set(found)))
    return tags

# --- БД ---
def fetch_messages(days: Optional[int] = None) -> Iterable[Dict[str, Any]]:
    """
    Вытягивает сообщения из otc.messages_archive.
    Можно ограничить по дням от текущего UTC.
    """
    where = []
    params: List[Any] = []
    if days and days > 0:
        since = datetime.now(timezone.utc) - timedelta(days=days)
        where.append("ts_utc >= %s")
        params.append(since)

    sql = """
        SELECT id, message_id, chat_id, sender_id, sender_username,
               ts_utc, text, processed, text_hash, duplicates_count, deleted, reply_to_msg_id
        FROM otc.messages_archive
        {where}
        ORDER BY ts_utc DESC
    """.format(where=("WHERE " + " AND ".join(where)) if where else "")

    with psycopg.connect(PG_DSN, row_factory=dict_row) as conn, conn.cursor() as cur:
        cur.execute(sql, params)
        for row in cur:
            yield row

# --- анализ ---
def analyze_wtb(rows: Iterable[Dict[str, Any]]) -> Dict[str, Any]:
    total = 0
    wtb_rows: List[Dict[str, Any]] = []

    # счётчики
    cat_counters: Dict[str, Counter] = {cat: Counter() for cat in KNOWN_ITEMS.keys()}
    overall_tags = Counter()
    chats_counter = Counter()
    countries_counter = Counter()
    exchanges_counter = Counter()

    examples_by_tag: Dict[str, List[Tuple[int, str]]] = defaultdict(list)  # tag -> [(row_id, snippet), ...]

    for r in rows:
        txt = r["text"] or ""
        if not is_wtb(txt):
            continue
        total += 1
        wtb_rows.append(r)

        # чат/юзер статистика
        chats_counter[str(r["chat_id"])] += 1

        tags = extract_tags(txt)
        # суммарные теги
        for cat, taglist in tags.items():
            cat_counters[cat].update(taglist)
            overall_tags.update(taglist)
            if cat == "countries":
                countries_counter.update(taglist)
            if cat == "exchanges":
                exchanges_counter.update(taglist)
            # примеры
            for tg in taglist:
                if len(examples_by_tag[tg]) < 5:
                    examples_by_tag[tg].append((r["id"], (txt[:140] + "…") if len(txt) > 140 else txt))

    return {
        "total_wtb": total,
        "cat_counters": {k: dict(v) for k, v in cat_counters.items()},
        "overall_tags": dict(overall_tags),
        "top_chats": dict(chats_counter),
        "top_countries": dict(countries_counter),
        "top_exchanges": dict(exchanges_counter),
        "examples_by_tag": {k: v for k, v in examples_by_tag.items()},
        "sample_count": len(wtb_rows),
    }

def print_report(rep: Dict[str, Any], top: int = 20) -> None:
    def topn(d: Dict[str, int], n: int) -> List[Tuple[str, int]]:
        return sorted(d.items(), key=lambda kv: kv[1], reverse=True)[:n]

    print(f"\n=== WTB summary ===")
    print(f"Total WTB messages: {rep['total_wtb']:,}")

    print("\n— Top countries —")
    for tag, cnt in topn(rep["top_countries"], top):
        print(f"{tag:>20}  {cnt}")

    print("\n— Top exchanges —")
    for tag, cnt in topn(rep["top_exchanges"], top):
        print(f"{tag:>20}  {cnt}")

    print("\n— Top overall tags —")
    for tag, cnt in topn(rep["overall_tags"], top):
        print(f"{tag:>20}  {cnt}")

    print("\n— By category —")
    for cat, d in rep["cat_counters"].items():
        print(f"\n[{cat}]")
        for tag, cnt in topn(d, top):
            print(f"{tag:>20}  {cnt}")

def save_json(rep: Dict[str, Any], path: str) -> None:
    # примеры длинные — оставим как есть
    with open(path, "w", encoding="utf-8") as f:
        json.dump(rep, f, ensure_ascii=False, indent=2)
    print(f"\nSaved JSON report -> {path}")

def save_tags_csv(rep: Dict[str, Any], path: str) -> None:
    """
    Плоский CSV всех тегов с их категориями и частотами.
    """
    rows = []
    # категории
    for cat, d in rep["cat_counters"].items():
        for tag, cnt in d.items():
            rows.append({"category": cat, "tag": tag, "count": cnt})
    # отсортируем
    rows.sort(key=lambda r: (r["category"], -r["count"], r["tag"]))
    with open(path, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["category", "tag", "count"])
        w.writeheader()
        w.writerows(rows)
    print(f"Saved CSV tags -> {path}")

# --- CLI ---
def main():
    import argparse
    ap = argparse.ArgumentParser(description="Analyze WTB messages in otc.messages_archive")
    ap.add_argument("--days", type=int, default=None, help="Limit by last N days (UTC). If omitted, use all rows.")
    ap.add_argument("--top", type=int, default=20, help="How many top rows to print per block")
    ap.add_argument("--out", type=str, default=None, help="Save full JSON report to this path")
    ap.add_argument("--csv", type=str, default=None, help="Save tags category CSV to this path")
    args = ap.parse_args()

    msgs = list(fetch_messages(days=args.days))
    rep = analyze_wtb(msgs)
    print_report(rep, top=args.top)

    if args.out:
        save_json(rep, args.out)
    if args.csv:
        save_tags_csv(rep, args.csv)

if __name__ == "__main__":
    main()