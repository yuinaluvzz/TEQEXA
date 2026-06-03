import os
import sqlite3
from contextlib import contextmanager
from config import DB_PATH, DATABASE_URL, USE_POSTGRES
import logging

logger = logging.getLogger(__name__)

# Lazy import psycopg2 only if needed
if USE_POSTGRES:
    import psycopg2
    import psycopg2.extras

def _ensure_sqlite_dir():
    os.makedirs(os.path.dirname(DB_PATH) or ".", exist_ok=True)

@contextmanager
def get_conn():
    if USE_POSTGRES:
        conn = psycopg2.connect(DATABASE_URL, cursor_factory=psycopg2.extras.RealDictCursor)
        try:
            yield conn
        finally:
            conn.close()
    else:
        _ensure_sqlite_dir()
        conn = sqlite3.connect(DB_PATH, timeout=30, isolation_level="EXCLUSIVE")
        conn.execute("PRAGMA foreign_keys = ON")
        conn.execute("PRAGMA journal_mode = WAL")
        conn.execute("PRAGMA synchronous = NORMAL")
        conn.execute("PRAGMA cache_size = 10000")
        try:
            yield conn
        finally:
            conn.close()

def _adapt_schema_for_postgres(sql_text: str) -> str:
    # Basic conversions from SQLite schema to Postgres-friendly SQL.
    s = sql_text
    s = s.replace("AUTOINCREMENT", "")
    s = s.replace("INTEGER PRIMARY KEY AUTOINCREMENT", "SERIAL PRIMARY KEY")
    s = s.replace("DEFAULT CURRENT_TIMESTAMP", "DEFAULT now()")
    s = s.replace("TEXT", "TEXT")
    # Remove PRAGMA lines if present
    s = "\n".join([line for line in s.splitlines() if not line.strip().upper().startswith("PRAGMA")])
    return s

def init_db(schema_path: str = "schema.sql"):
    logger.info("Initializing DB (USE_POSTGRES=%s)", USE_POSTGRES)
    with open(schema_path, "r", encoding="utf-8") as f:
        sql = f.read()
    if USE_POSTGRES:
        sql_pg = _adapt_schema_for_postgres(sql)
        with get_conn() as conn:
            cur = conn.cursor()
            # Split statements by semicolon and execute
            for stmt in sql_pg.split(";"):
                stmt = stmt.strip()
                if not stmt:
                    continue
                try:
                    cur.execute(stmt)
                except Exception as e:
                    logger.debug("Ignoring schema exec error: %s", e)
            conn.commit()
    else:
        with get_conn() as conn:
            conn.executescript(sql)
            conn.commit()
    # Seed tickers if missing
    _seed_tickers()

def _seed_tickers():
    TICKER_SEED = [
        ("EURO", 3000000, 60000),
        ("STRM", 128000, 40000),
        ("ASIA", 50000, 100000),
        ("CLAN", 36000, 30000),
        ("AMER", 252000, 180000),
        ("MENA", 52500, 70000),
        ("AFRI", 48000, 80000),
        ("PACI", 30000, 60000),
        ("BOTS", 1500, 10000),
    ]
    if USE_POSTGRES:
        with get_conn() as conn:
            cur = conn.cursor()
            for ticker, gp, sp in TICKER_SEED:
                cur.execute(
                    "INSERT INTO tickers(ticker, gold_pool, share_pool, day_start_price, is_frozen) VALUES (%s, %s, %s, %s, %s) ON CONFLICT (ticker) DO NOTHING",
                    (ticker, gp, sp, round(gp / sp, 6), 0)
                )
            conn.commit()
    else:
        with get_conn() as conn:
            cur = conn.execute("SELECT COUNT(*) FROM tickers")
            if cur.fetchone()[0] == 0:
                for ticker, gp, sp in TICKER_SEED:
                    conn.execute(
                        "INSERT OR IGNORE INTO tickers(ticker, gold_pool, share_pool, day_start_price, is_frozen) VALUES (?, ?, ?, ?, ?)",
                        (ticker, gp, sp, round(gp / sp, 6), 0)
                    )
            conn.commit()

def get_ledger_last_seen():
    with get_conn() as conn:
        if USE_POSTGRES:
            cur = conn.cursor()
            cur.execute("SELECT value FROM ledger_state WHERE key = %s", ("last_seen_time",))
            row = cur.fetchone()
            return int(row["value"]) if row and row.get("value") and str(row.get("value")).isdigit() else 0
        else:
            cur = conn.execute("SELECT value FROM ledger_state WHERE key = 'last_seen_time'")
            row = cur.fetchone()
            return int(row[0]) if row and row[0] and str(row[0]).isdigit() else 0

def set_ledger_last_seen(ts):
    with get_conn() as conn:
        if USE_POSTGRES:
            cur = conn.cursor()
            cur.execute("INSERT INTO ledger_state(key, value) VALUES (%s, %s) ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value", ("last_seen_time", str(int(ts))))
            conn.commit()
        else:
            conn.execute("INSERT OR REPLACE INTO ledger_state(key, value) VALUES ('last_seen_time', ?)", (str(int(ts)),))
            conn.commit()
