# -*- coding: utf-8 -*-
import os
import re
import json
import csv
from collections import Counter, defaultdict
from typing import List, Dict, Tuple, Iterable, Set

import psycopg
from psycopg.rows import dict_row

try:
    from rapidfuzz import process as rf_process, fuzz as rf_fuzz
    HAVE_RAPIDFUZZ = True
except Exception:
    HAVE_RAPIDFUZZ = False

PG_DSN = os.getenv("PG_DSN", "postgresql://otc_user:supersecret@localhost:5432/otc_app")

# Пути к словарям (если их нет — создадим начальные)
KNOWN_PATH = "known_items.json"
ALIASES_PATH = "known_aliases.json"
STOP_PATH = "stop_tokens.json"

# ------------------------ SEED СЛОВАРИ ------------------------
SEED_KNOWN = [
    # биржи / крипто
    "binance", "bybit", "okx", "bitget", "kucoin", "coinbase", "bitexen",
    "exmo", "huobi", "gate", "mexc", "kraken", "whitebit",

    # kyc/провайдеры идентификации
    "sumsub", "onfido", "shufti", "idnow", "trulioo",

    # платёжки / банки / эмитенты карт
    "wise", "revolut", "jeton", "skrill", "neteller", "advcash", "payoneer",
    "moonpay", "ramp", "mercado pago", "oxxo", "upi", "pix",

    # маркетплейсы/шопы часто встречающиеся в постах
    "shoppe", "shopee", "tiktok shop", "amazon", "ebay", "aliexpress",

    # государства / гео (короткий набор, авто-майнинг расширит)
    "turkey", "poland", "argentina", "peru", "bolivia", "thailand", "morocco",
    "mexico", "chile", "colombia", "nicaragua", "myanmar", "ecuador",
    "venezuela", "jordan", "algeria", "brazil", "uruguay", "paraguay",
    "latvia", "kenya", "europe"
]

SEED_ALIASES = {
    # биржи
    "binance": ["bnb", "binance kyc", "binance acc", "binance account"],
    "bybit": ["bybit kyc", "bybit acc", "bybit account"],
    "okx": ["okex", "okx kyc"],
    "bitget": ["bitget kyc"],
    "coinbase": ["coinbase kyc", "cb"],
    "kucoin": ["kc", "kucoin kyc"],
    "whitebit": ["wb"],
    "bitexen": ["bitexen kyc"],

    # kyc
    "sumsub": ["sumsub link"],
    "onfido": ["onfido link"],

    # платёжки
    "wise": ["wise card", "wise acc", "transferwise"],
    "revolut": ["revo", "revoult", "revolut card"],
    "jeton": ["jeton cash"],
    "skrill": ["skrill card"],
    "advcash": ["advc", "advcash card"],
    "payoneer": ["payo", "payoneer card"],
    "moonpay": ["moon pay"],

    # маркетплейсы
    "shopee": ["shoppe"],  # нормализуем
    "tiktok shop": ["tiktokshop", "tt shop"],

    # гео — типичные опечатки
    "el salvador": ["elsavador", "el salvador", "el-salvador"],
}

SEED_STOP = [
    # общеязыковые маркеры «не предмета»
    "need", "have", "seller", "buyers", "looking", "business", "first",
    "from", "good", "price", "fast", "service", "ready", "link", "work",
    "escrow", "kyc", "api", "acc", "account", "bulk", "day", "long", "term",
    "quality", "only", "dm", "ok", "can", "your", "with", "and", "for", "the"
]

# ------------------------ УТИЛИТЫ ------------------------

def load_json(path: str, default):
    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    return default

def save_json(path: str, data):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

def normalize_text(t: str) -> str:
    t = (t or "").lower()
    # простая чистка: убираем юзернеймы/ссылки/телефоны, много пробелов
    t = re.sub(r"@\w{3,32}", " ", t)
    t = re.sub(r"https?://\S+|t\.me/\S+", " ", t)
    t = re.sub(r"\+?\d[\d\-\s()]{7,}", " ", t)
    t = re.sub(r"[^a-z0-9а-яё\s/_\-\.]", " ", t)
    t = re.sub(r"\s+", " ", t).strip()
    return t

def ngrams(tokens: List[str], n: int) -> Iterable[str]:
    for i in range(len(tokens) - n + 1):
        yield " ".join(tokens[i:i+n])

def tokenize(t: str) -> List[str]:
    return (t or "").split()

def alias_map_to_flat(aliases: Dict[str, List[str]]) -> Dict[str, str]:
    """Обратный индекс: alias -> canonical."""
    flat = {}
    for canon, arr in aliases.items():
        for a in arr:
            flat[normalize_text(a)] = canon
    return flat

# ------------------------ ИЗВЛЕКАТОР ------------------------

class DealItemExtractor:
    def __init__(self, known: Iterable[str], aliases: Dict[str, List[str]], stop: Iterable[str]):
        # канонические формы
        self.known: Set[str] = set(normalize_text(x) for x in known)
        # алиасы -> к какому канону маппить
        self.alias2canon: Dict[str, str] = alias_map_to_flat(aliases)
        # стоп-слова
        self.stop: Set[str] = set(normalize_text(x) for x in stop)

        # подготовим набор для прямого матчинга фраз (n=1..3 достаточно)
        self.max_ngram = 3
        self.known_phrases = set(self.known) | set(self.alias2canon.keys())

        if HAVE_RAPIDFUZZ:
            # список кандидатных ключей для fuzzy
            self._fuzzy_keys = list(self.known_phrases)

    def _canon(self, s: str) -> str:
        s = normalize_text(s)
        if s in self.alias2canon:
            return self.alias2canon[s]
        return s

    def extract(self, text: str) -> List[str]:
        """
        1) прямой матч n-грамм до 3 слов
        2) отбрасываем стоп-слова и одиночные мусорные токены
        3) fallback fuzzy по RapidFuzz (если включен)
        """
        t = normalize_text(text)
        toks = tokenize(t)
        found: Set[str] = set()

        # прямой матч n-грамм
        for n in range(self.max_ngram, 0, -1):
            for g in ngrams(toks, n):
                gg = g.strip()
                if gg in self.known_phrases:
                    found.add(self._canon(gg))

        # одиночные токены: убираем стопы и коротыши
        found = {x for x in found if x not in self.stop and len(x) >= 3}

        # если ничего не нашли — попробуем fuzzy для подозрительных токенов (латиница)
        if HAVE_RAPIDFUZZ and not found:
            candidates = []
            for tok in set(toks):
                if tok in self.stop or len(tok) < 3:
                    continue
                if not re.search(r"[a-z]", tok):
                    continue
                # ищем ближайший ключ среди известных фраз/алиасов
                match = rf_process.extractOne(tok, self._fuzzy_keys, scorer=rf_fuzz.QRatio)
                if match and match[1] >= 90:  # высокий порог, чтобы не ловить мусор
                    candidates.append(self._canon(match[0]))
            found.update(candidates)

        # финальная нормализация и сортировка
        out = sorted(found)
        return out

# ------------------------ ДОБЫЧА НОВЫХ КАНДИДАТОВ ------------------------

def mine_new_candidates(texts: Iterable[str],
                        extractor: DealItemExtractor,
                        min_len: int = 3,
                        top_k: int = 200) -> List[Tuple[str, int]]:
    """
    Простая авто-добыча кандидатов: частотные n-граммы (1..3),
    которых нет ни в known/alias, и не стоп-слова.
    """
    known = extractor.known
    aliases = set(extractor.alias2canon.keys())
    stop = extractor.stop
    vocab = Counter()

    for raw in texts:
        t = normalize_text(raw)
        toks = tokenize(t)
        grams = list(ngrams(toks, 1)) + list(ngrams(toks, 2)) + list(ngrams(toks, 3))
        for g in grams:
            if len(g) < min_len:
                continue
            if g in stop or g in known or g in aliases:
                continue
            # фильтр: должна быть латиница или цифры, а не чистый мусор
            if not re.search(r"[a-z0-9]", g):
                continue
            vocab[g] += 1

    return vocab.most_common(top_k)

# ------------------------ РАБОТА С БД ------------------------

def fetch_messages(limit: int | None = None) -> List[dict]:
    q = "SELECT id, chat_id, message_id, sender_id, ts_utc, text FROM otc.messages_archive WHERE text IS NOT NULL"
    if limit:
        q += f" ORDER BY ts_utc DESC LIMIT {int(limit)}"
    with psycopg.connect(PG_DSN, row_factory=dict_row) as conn, conn.cursor() as cur:
        cur.execute(q)
        return cur.fetchall()

# ------------------------ MAIN ------------------------

def main():
    # 1) грузим/инициализируем словари
    known = load_json(KNOWN_PATH, SEED_KNOWN)
    aliases = load_json(ALIASES_PATH, SEED_ALIASES)
    stop = load_json(STOP_PATH, SEED_STOP)

    extractor = DealItemExtractor(known, aliases, stop)

    # 2) берём все сообщения из БД
    rows = fetch_messages(limit=None)
    print(f"Loaded {len(rows)} messages from DB")

    # 3) извлекаем предметы по каждому сообщению
    per_msg: List[Tuple[int, int, int, str, List[str]]] = []  # (row_id, chat_id, message_id, iso_time, items)
    all_items = Counter()
    texts_for_mining = []

    for r in rows:
        items = extractor.extract(r["text"] or "")
        texts_for_mining.append(r["text"] or "")
        if items:
            per_msg.append((r["id"], r["chat_id"], r["message_id"], r["ts_utc"].isoformat(), items))
            all_items.update(items)

    # 4) пишем per-message CSV
    with open("deal_items_per_message.csv", "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(["row_id", "chat_id", "message_id", "ts_utc", "items"])
        for rid, cid, mid, ts, items in per_msg:
            w.writerow([rid, cid, mid, ts, ", ".join(items)])

    # 5) пишем топ частот
    with open("deal_items_top.csv", "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(["item", "count"])
        for item, cnt in all_items.most_common():
            w.writerow([item, cnt])

    # 6) авто-добыча новых кандидатов (под пополнение словаря)
    new_cands = mine_new_candidates(texts_for_mining, extractor, min_len=3, top_k=300)
    with open("deal_items_new_candidates.txt", "w", encoding="utf-8") as f:
        for term, freq in new_cands:
            f.write(f"{term}\t{freq}\n")

    print("Saved:")
    print("  - deal_items_per_message.csv")
    print("  - deal_items_top.csv")
    print("  - deal_items_new_candidates.txt")
    print("Tip: переносите нужные термины из new_candidates в known/aliases и запускайте снова.")

if __name__ == "__main__":
    main()