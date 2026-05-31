"""
territorial_bot_single_live_ledger.py

Single-file Territorial.io Market Discord bot prototype with live ledger ingestion.

Features:
- SQLite DB initialization and seeding (tickers)
- Constant-product AMM (buy/sell) with fees, per-trade cap, circuit breaker
- Live ledger ingestion from https://territorial.io/log/transactions using aiohttp
  with persistent last-seen timestamp to avoid reprocessing
- Mock ledger fallback (mock_ledger/ledger.json) for closed-beta testing
- Structured pending-nonce storage (JSON in audit_logs.details)
- Async-safe DB operations via asyncio.to_thread
- Admin commands: /force_verify, /freeze, /unfreeze, /audit, /export_withdrawals
- Discord commands: /link, /buy, /sell, /status
- Logging and safe error handling

Important:
- Create a .env file with DISCORD_TOKEN and ADMIN_CHANNEL_ID
- For closed-beta testing you can still use mock_ledger/ledger.json
- This is a prototype. For production, migrate to Postgres, secure secrets, and harden concurrency.

Run:
    python territorial_bot_single_live_ledger.py
"""

import os
import random
try:
    from aiohttp import web as aio_web
    AIOHTTP_AVAILABLE = True
except ImportError:
    AIOHTTP_AVAILABLE = False
import sqlite3
import json
import uuid
import asyncio
import logging
from decimal import Decimal, getcontext
from contextlib import contextmanager
from datetime import datetime, timedelta
from dotenv import load_dotenv

# logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)
background_tasks_started = False

# ---- Configuration and environment ----
load_dotenv()
DISCORD_TOKEN = os.getenv("DISCORD_TOKEN", "")
DB_PATH = os.getenv("DB_PATH")
if not DB_PATH:
    DB_PATH = "/data/market.db" if os.path.isdir("/data") else "./data/market.db"
if DB_PATH:
    db_dir = os.path.dirname(DB_PATH) or "."
    os.makedirs(db_dir, exist_ok=True)
ADMIN_CHANNEL_ID           = int(os.getenv("ADMIN_CHANNEL_ID", "0") or 0)
# Comma-separated Discord user IDs that may run admin commands (set via ADMIN secret)
ADMIN_IDS                  = {s.strip() for s in os.getenv("ADMIN", "").split(",") if s.strip()}
_market_ch_raw             = os.getenv("MARKET_CHANNEL_ID", "0") or "0"
try:
    MARKET_CHANNEL_ID = int(_market_ch_raw.strip().split("/")[-1])
except ValueError:
    MARKET_CHANNEL_ID = 0
    logger.warning("MARKET_CHANNEL_ID is invalid: %r", _market_ch_raw)
WITHDRAWAL_LOG_CHANNEL_ID  = 1510444352294621224   # #withdrawal-log — admin notifications
ACTIVITY_LOG_CHANNEL_ID    = 1510444499334336743   # #activity-log — all market events
TRADE_LOG_CHANNEL_ID       = 1510498410271211660   # #trade-log — all trade logs
GUILD_ID = int(os.getenv("GUILD_ID", "0") or 0)   # set for instant slash command sync
HEALTH_PORT = int(os.getenv("PORT", "8080"))
LEDGER_FILE = os.path.join("mock_ledger", "ledger.json")
LEDGER_POLL_INTERVAL = int(os.getenv("LEDGER_POLL_INTERVAL", "8"))
MARKET_UPDATE_INTERVAL = int(os.getenv("MARKET_UPDATE_INTERVAL", "30"))
VERIFICATION_AMOUNT = int(os.getenv("VERIFICATION_AMOUNT", "5"))

# Live ledger URL
LEDGER_URL = os.getenv("LEDGER_URL", "https://territorial.io/log/transactions")

# AMM parameters
getcontext().prec = 28
BASE_FEE = Decimal(os.getenv("BASE_FEE", "0.015"))  # 1.5%
PER_TRADE_CAP_PERCENT = Decimal(os.getenv("PER_TRADE_CAP_PERCENT", "0.01"))  # 1% of pool
PER_TRADE_CAP_ABS = Decimal(os.getenv("PER_TRADE_CAP_ABS", "300"))  # absolute cap
CIRCUIT_BREAKER_HARD = Decimal("0.15")  # 15% freeze

# Ensure directories
os.makedirs("mock_ledger", exist_ok=True)
os.makedirs("exports", exist_ok=True)

# logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# ---- Database helpers ----
@contextmanager
def get_conn():
    conn = sqlite3.connect(DB_PATH, timeout=30, isolation_level="EXCLUSIVE")
    conn.execute("PRAGMA foreign_keys = ON")
    try:
        yield conn
    finally:
        conn.close()


def init_db():
    sql = """
    PRAGMA foreign_keys = ON;
    CREATE TABLE IF NOT EXISTS users (
      discord_id TEXT PRIMARY KEY,
      game_name TEXT UNIQUE NOT NULL,
      internal_gold INTEGER NOT NULL DEFAULT 0,
      verified INTEGER NOT NULL DEFAULT 0,
      tier TEXT NOT NULL DEFAULT 'basic',
      created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
    );
    CREATE TABLE IF NOT EXISTS portfolios (
      discord_id TEXT NOT NULL,
      ticker TEXT NOT NULL,
      shares INTEGER NOT NULL DEFAULT 0,
      PRIMARY KEY (discord_id, ticker),
      FOREIGN KEY (discord_id) REFERENCES users(discord_id) ON DELETE CASCADE
    );
    CREATE TABLE IF NOT EXISTS tickers (
      ticker TEXT PRIMARY KEY,
      gold_pool INTEGER NOT NULL,
      share_pool INTEGER NOT NULL,
      day_start_price REAL NOT NULL,
      is_frozen INTEGER NOT NULL DEFAULT 0,
      last_updated TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
    );
    CREATE TABLE IF NOT EXISTS trades (
      trade_id INTEGER PRIMARY KEY AUTOINCREMENT,
      timestamp TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
      discord_id TEXT,
      ticker TEXT,
      type TEXT,
      gross_gold INTEGER,
      net_gold INTEGER,
      shares INTEGER,
      fee INTEGER,
      price_before TEXT,
      price_after TEXT,
      maker_flag INTEGER DEFAULT 0,
      status TEXT DEFAULT 'COMPLETED',
      reason TEXT,
      FOREIGN KEY (discord_id) REFERENCES users(discord_id)
    );
    CREATE TABLE IF NOT EXISTS verifications (
      tx_id TEXT PRIMARY KEY,
      discord_id TEXT,
      game_name TEXT,
      nonce TEXT,
      raw_payload TEXT,
      verified_at TEXT,
      FOREIGN KEY (discord_id) REFERENCES users(discord_id)
    );
    CREATE TABLE IF NOT EXISTS withdrawals (
      withdrawal_id INTEGER PRIMARY KEY AUTOINCREMENT,
      discord_id TEXT,
      amount INTEGER,
      status TEXT DEFAULT 'PENDING',
      admin_batch_id INTEGER,
      requested_at TEXT DEFAULT CURRENT_TIMESTAMP
    );
    CREATE TABLE IF NOT EXISTS limit_orders (
      order_id INTEGER PRIMARY KEY AUTOINCREMENT,
      discord_id TEXT,
      ticker TEXT,
      side TEXT,
      price REAL,
      shares INTEGER,
      created_at TEXT DEFAULT CURRENT_TIMESTAMP
    );
    CREATE TABLE IF NOT EXISTS audit_logs (
      log_id INTEGER PRIMARY KEY AUTOINCREMENT,
      actor TEXT,
      action TEXT,
      details TEXT,
      timestamp TEXT DEFAULT CURRENT_TIMESTAMP
    );
    CREATE TABLE IF NOT EXISTS ledger_state (
      key TEXT PRIMARY KEY,
      value TEXT
    );
    CREATE TABLE IF NOT EXISTS price_history (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      ticker TEXT NOT NULL,
      price REAL NOT NULL,
      recorded_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
    );
        CREATE INDEX IF NOT EXISTS idx_ph_ticker_time ON price_history(ticker, recorded_at);

        -- Achievements, dividends, and market events tables (Phase 1 additions)
        CREATE TABLE IF NOT EXISTS achievements (
            achievement_id INTEGER PRIMARY KEY AUTOINCREMENT,
            discord_id TEXT NOT NULL,
            achievement_name TEXT NOT NULL,
            earned_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (discord_id) REFERENCES users(discord_id),
            UNIQUE(discord_id, achievement_name)
        );
        CREATE TABLE IF NOT EXISTS dividends_paid (
            dividend_id INTEGER PRIMARY KEY AUTOINCREMENT,
            discord_id TEXT NOT NULL,
            ticker TEXT NOT NULL,
            amount INTEGER NOT NULL,
            paid_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (discord_id) REFERENCES users(discord_id)
        );
        CREATE TABLE IF NOT EXISTS market_events (
            event_id INTEGER PRIMARY KEY AUTOINCREMENT,
            event_name TEXT NOT NULL,
            event_description TEXT,
            affected_tickers TEXT,
            price_impact_percent REAL,
            event_time TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            posted_to_discord INTEGER DEFAULT 0
        );
        CREATE TABLE IF NOT EXISTS dividend_config (
            ticker TEXT PRIMARY KEY,
            dividend_yield REAL NOT NULL DEFAULT 0.02
        );
        CREATE INDEX IF NOT EXISTS idx_achievements_discord ON achievements(discord_id);
        CREATE INDEX IF NOT EXISTS idx_dividends_discord ON dividends_paid(discord_id);
        CREATE INDEX IF NOT EXISTS idx_market_events_time ON market_events(event_time);
    """
    with get_conn() as conn:
        conn.executescript(sql)
        # seed tickers if missing — (ticker, gold_pool, share_pool) → price = gold_pool/share_pool
        TICKER_SEED = [
            ("EURO", 3000000,  60000),   # 50.00 — Europe, flagship blue-chip
            ("STRM", 128000,  40000),    # 3.20 — Streamers, influencer premium
            ("ASIA",  50000, 100000),    # 0.50 — Asia, established market
            ("CLAN",  36000,  30000),    # 1.20 — Top clans composite
            ("AMER", 252000, 180000),    # 1.40 — Americas, emerging large-cap
            ("MENA",  52500,  70000),    # 0.75 — Middle East & North Africa
            ("AFRI",  48000,  80000),    # 0.60 — Africa, growth play
            ("PACI",  30000,  60000),    # 0.50 — Pacific, small-cap
            ("BOTS",   1500,  10000),    # 0.15 — Bot accounts, speculative
        ]
        cur = conn.execute("SELECT COUNT(*) FROM tickers")
        if cur.fetchone()[0] == 0:
            for ticker, gp, sp in TICKER_SEED:
                conn.execute(
                    "INSERT OR IGNORE INTO tickers(ticker, gold_pool, share_pool, day_start_price, is_frozen) VALUES (?, ?, ?, ?, ?)",
                    (ticker, gp, sp, round(gp / sp, 6), 0)
                )
        else:
            # Migration: update any ticker still at flat price (gold_pool == share_pool)
            # to its real starting price from TICKER_SEED.
            seed_map = {t: (gp, sp) for t, gp, sp in TICKER_SEED}
            flat_tickers = conn.execute(
                "SELECT ticker FROM tickers WHERE gold_pool = share_pool"
            ).fetchall()
            for (sym,) in flat_tickers:
                if sym in seed_map:
                    ngp, nsp = seed_map[sym]
                    conn.execute(
                        "UPDATE tickers SET gold_pool=?, share_pool=?, day_start_price=? WHERE ticker=?",
                        (ngp, nsp, round(ngp / nsp, 6), sym)
                    )
            # Ensure new tickers from TICKER_SEED exist even on old DBs
            for sym, gp, sp in TICKER_SEED:
                conn.execute(
                    "INSERT OR IGNORE INTO tickers(ticker, gold_pool, share_pool, day_start_price, is_frozen) VALUES (?, ?, ?, ?, ?)",
                    (sym, gp, sp, round(gp / sp, 6), 0)
                )
        # clean up legacy treasury row if present
        conn.execute("DELETE FROM users WHERE discord_id = 'TREASURY'")
        # initialize ledger_state
        cur = conn.execute("SELECT value FROM ledger_state WHERE key = 'last_seen_time'")
        if not cur.fetchone():
            conn.execute("INSERT OR IGNORE INTO ledger_state(key, value) VALUES ('last_seen_time', '0')")
        conn.commit()


def get_ledger_last_seen():
    with get_conn() as conn:
        cur = conn.execute("SELECT value FROM ledger_state WHERE key = 'last_seen_time'")
        row = cur.fetchone()
        return int(row[0]) if row and row[0].isdigit() else 0


def set_ledger_last_seen(ts):
    with get_conn() as conn:
        conn.execute("INSERT OR REPLACE INTO ledger_state(key, value) VALUES ('last_seen_time', ?)", (str(int(ts)),))
        conn.commit()


# ---- AMM engine ----
class AMM:
    def __init__(self):
        pass

    def _fetch_ticker(self, conn, ticker):
        cur = conn.execute("SELECT gold_pool, share_pool, day_start_price, is_frozen FROM tickers WHERE ticker = ?", (ticker,))
        row = cur.fetchone()
        if not row:
            raise ValueError("Ticker not found")
        return (Decimal(row[0]), Decimal(row[1]), Decimal(row[2]), bool(row[3]))

    def _price(self, G, S):
        return (G / S) if S != 0 else Decimal("0")

    def buy(self, discord_id, ticker, gross_gold):
        with get_conn() as conn:
            conn.execute("BEGIN EXCLUSIVE")
            try:
                G, S, day_start_price, is_frozen = self._fetch_ticker(conn, ticker)
                if is_frozen:
                    return {"ok": False, "reason": "ticker_frozen"}
                gross = Decimal(int(gross_gold))
                # Check user has sufficient gold balance
                cur = conn.execute("SELECT internal_gold FROM users WHERE discord_id = ?", (discord_id,))
                user_row = cur.fetchone()
                if not user_row or user_row[0] < int(gross):
                    return {"ok": False, "reason": "insufficient_gold"}
                per_trade_cap = min((G * PER_TRADE_CAP_PERCENT).quantize(Decimal("1")), PER_TRADE_CAP_ABS)
                if gross > per_trade_cap:
                    return {"ok": False, "reason": "trade_exceeds_cap"}
                pct_of_pool = (gross / G) if G > 0 else Decimal("0")
                fee = BASE_FEE
                if pct_of_pool > Decimal("0.005"):
                    fee += Decimal("0.005")
                if pct_of_pool > Decimal("0.01"):
                    fee += Decimal("0.01")
                # Calculate fee amount with proper rounding (ceiling to ensure fee is captured)
                fee_decimal = (gross * fee)
                fee_int = int(fee_decimal)
                # If there's a fractional part and we're not collecting any fee, round up to 1
                if fee_int == 0 and fee_decimal > 0:
                    fee_int = 1
                net_gold = gross - Decimal(fee_int)
                k = G * S
                G_new = G + net_gold
                S_new = (k / G_new).quantize(Decimal("1"))
                shares_received = (S - S_new).quantize(Decimal("1"))
                price_before = self._price(G, S)
                price_after = self._price(G_new, S_new)
                change = abs((price_after - day_start_price) / day_start_price) if day_start_price > 0 else Decimal("0")
                if change > CIRCUIT_BREAKER_HARD:
                    conn.execute("UPDATE tickers SET is_frozen = 1 WHERE ticker = ?", (ticker,))
                    conn.commit()
                    return {"ok": False, "reason": "circuit_breaker_triggered"}
                # Deduct gold from user balance
                conn.execute("UPDATE users SET internal_gold = internal_gold - ? WHERE discord_id = ?",
                             (int(gross), discord_id))
                conn.execute("UPDATE tickers SET gold_pool = ?, share_pool = ?, last_updated = CURRENT_TIMESTAMP WHERE ticker = ?",
                             (int(G_new), int(S_new), ticker))
                cur = conn.execute("SELECT shares FROM portfolios WHERE discord_id = ? AND ticker = ?", (discord_id, ticker))
                row = cur.fetchone()
                if row:
                    conn.execute("UPDATE portfolios SET shares = shares + ? WHERE discord_id = ? AND ticker = ?",
                                 (int(shares_received), discord_id, ticker))
                else:
                    conn.execute("INSERT INTO portfolios(discord_id, ticker, shares) VALUES (?, ?, ?)",
                                 (discord_id, ticker, int(shares_received)))
                conn.execute("""INSERT INTO trades(discord_id, ticker, type, gross_gold, net_gold, shares, fee, price_before, price_after, maker_flag)
                                VALUES (?, ?, 'BUY', ?, ?, ?, ?, ?, ?, 0)""",
                             (discord_id, ticker, int(gross), int(net_gold), int(shares_received), fee_int, str(price_before), str(price_after)))
                conn.commit()
                return {"ok": True, "shares": int(shares_received), "price_after": float(price_after), "fee": fee_int}
            except ValueError:
                conn.rollback()
                return {"ok": False, "reason": "ticker_not_found"}
            except Exception:
                conn.rollback()
                logger.exception("AMM.buy failed")
                return {"ok": False, "reason": "internal_error"}

    def sell(self, discord_id, ticker, shares_to_sell):
        with get_conn() as conn:
            conn.execute("BEGIN EXCLUSIVE")
            try:
                G, S, day_start_price, is_frozen = self._fetch_ticker(conn, ticker)
                if is_frozen:
                    return {"ok": False, "reason": "ticker_frozen"}
                shares = Decimal(int(shares_to_sell))
                k = G * S
                S_new = (S - shares).quantize(Decimal("1"))
                if S_new <= 0:
                    return {"ok": False, "reason": "would_empty_pool"}
                G_new = (k / S_new).quantize(Decimal("1"))
                gross_gold_out = (G_new - G).quantize(Decimal("1"))
                per_trade_cap = min((G * PER_TRADE_CAP_PERCENT).quantize(Decimal("1")), PER_TRADE_CAP_ABS)
                if gross_gold_out > per_trade_cap:
                    return {"ok": False, "reason": "trade_exceeds_cap"}
                pct_of_pool = (gross_gold_out / G) if G > 0 else Decimal("0")
                fee = BASE_FEE
                if pct_of_pool > Decimal("0.005"):
                    fee += Decimal("0.005")
                if pct_of_pool > Decimal("0.01"):
                    fee += Decimal("0.01")
                # Calculate fee amount with proper rounding (ceiling to ensure fee is captured)
                fee_decimal = (gross_gold_out * fee)
                fee_int = int(fee_decimal)
                # If there's a fractional part and we're not collecting any fee, round up to 1
                if fee_int == 0 and fee_decimal > 0:
                    fee_int = 1
                net_gold = gross_gold_out - Decimal(fee_int)
                price_before = self._price(G, S)
                price_after = self._price(G_new, S_new)
                change = abs((price_after - day_start_price) / day_start_price) if day_start_price > 0 else Decimal("0")
                if change > CIRCUIT_BREAKER_HARD:
                    conn.execute("UPDATE tickers SET is_frozen = 1 WHERE ticker = ?", (ticker,))
                    conn.commit()
                    return {"ok": False, "reason": "circuit_breaker_triggered"}
                cur = conn.execute("SELECT shares FROM portfolios WHERE discord_id = ? AND ticker = ?", (discord_id, ticker))
                row = cur.fetchone()
                if not row or row[0] < int(shares):
                    conn.rollback()
                    return {"ok": False, "reason": "insufficient_shares"}
                conn.execute("UPDATE portfolios SET shares = shares - ? WHERE discord_id = ? AND ticker = ?",
                             (int(shares), discord_id, ticker))
                conn.execute("UPDATE tickers SET gold_pool = ?, share_pool = ?, last_updated = CURRENT_TIMESTAMP WHERE ticker = ?",
                             (int(G_new), int(S_new), ticker))
                conn.execute("""INSERT INTO trades(discord_id, ticker, type, gross_gold, net_gold, shares, fee, price_before, price_after, maker_flag)
                                VALUES (?, ?, 'SELL', ?, ?, ?, ?, ?, ?, 0)""",
                             (discord_id, ticker, int(gross_gold_out), int(net_gold), int(shares), fee_int, str(price_before), str(price_after)))
                conn.execute("UPDATE users SET internal_gold = internal_gold + ? WHERE discord_id = ?", (int(net_gold), discord_id))
                conn.commit()
                return {"ok": True, "gold": int(net_gold), "price_after": float(price_after), "fee": fee_int}
            except ValueError:
                conn.rollback()
                return {"ok": False, "reason": "ticker_not_found"}
            except Exception:
                conn.rollback()
                logger.exception("AMM.sell failed")
                return {"ok": False, "reason": "internal_error"}


# ---- Ledger ingestion: live fetch + mock fallback ----
# ---- Phase 1: Achievements, Dividends, Market Events ----


class Achievements:
    """Track and award player achievements."""

    ACHIEVEMENT_DEFINITIONS = {
        "first_millionaire": {
            "name": "First Millionaire",
            "description": "Reach 1,000,000 gold portfolio value",
            "trigger": "portfolio_value",
            "threshold": 1000000
        },
        "survived_crash": {
            "name": "Crash Survivor",
            "description": "Hold through a 15%+ market crash",
            "trigger": "price_drop",
            "threshold": 0.15
        },
        "perfect_trade_streak": {
            "name": "Perfect Trader",
            "description": "5 consecutive profitable trades",
            "trigger": "trade_streak",
            "threshold": 5
        },
        "day_trader": {
            "name": "Day Trader",
            "description": "Execute 50 trades in a single day",
            "trigger": "daily_trades",
            "threshold": 50
        },
        "hodler": {
            "name": "HODLER",
            "description": "Hold the same ticker for 30 days",
            "trigger": "hold_duration",
            "threshold": 30
        },
        "diversified": {
            "name": "Diversified Portfolio",
            "description": "Own shares in all 9 tickers",
            "trigger": "ticker_count",
            "threshold": 9
        },
        "dividend_collector": {
            "name": "Dividend Collector",
            "description": "Earn 10,000 gold from dividends",
            "trigger": "dividend_income",
            "threshold": 10000
        },
        "price_alert_prophet": {
            "name": "Price Prophet",
            "description": "Hit 10 price alerts successfully",
            "trigger": "alert_hits",
            "threshold": 10
        }
    }

    @staticmethod
    def check_and_award(discord_id, trigger_type, data):
        """Check if user earned an achievement and award it."""
        with get_conn() as conn:
            for ach_key, ach_def in Achievements.ACHIEVEMENT_DEFINITIONS.items():
                if ach_def["trigger"] != trigger_type:
                    continue

                # Check if already earned
                cur = conn.execute(
                    "SELECT achievement_id FROM achievements WHERE discord_id = ? AND achievement_name = ?",
                    (discord_id, ach_key)
                )
                if cur.fetchone():
                    continue  # Already earned

                # Check if threshold met
                earned = False
                if trigger_type == "portfolio_value" and data.get("value", 0) >= ach_def["threshold"]:
                    earned = True
                elif trigger_type == "price_drop" and data.get("drop_percent", 0) >= ach_def["threshold"]:
                    earned = True
                elif trigger_type == "trade_streak" and data.get("streak", 0) >= ach_def["threshold"]:
                    earned = True
                elif trigger_type == "daily_trades" and data.get("count", 0) >= ach_def["threshold"]:
                    earned = True
                elif trigger_type == "hold_duration" and data.get("days", 0) >= ach_def["threshold"]:
                    earned = True
                elif trigger_type == "ticker_count" and data.get("count", 0) >= ach_def["threshold"]:
                    earned = True
                elif trigger_type == "dividend_income" and data.get("total", 0) >= ach_def["threshold"]:
                    earned = True
                elif trigger_type == "alert_hits" and data.get("count", 0) >= ach_def["threshold"]:
                    earned = True

                if earned:
                    conn.execute(
                        "INSERT OR IGNORE INTO achievements(discord_id, achievement_name) VALUES (?, ?)",
                        (discord_id, ach_key)
                    )
                    conn.execute(
                        "INSERT INTO audit_logs(actor, action, details) VALUES (?, ?, ?)",
                        (discord_id, "achievement_earned", f"Earned: {ach_def['name']}")
                    )
                    conn.commit()
                    logger.info(f"Achievement earned: {discord_id} -> {ach_key}")


class Dividends:
    """Handle dividend payouts."""

    @staticmethod
    def init_dividend_config():
        """Initialize dividend yields for tickers."""
        config = {
            "EURO": 0.02,   # 2%
            "STRM": 0.015,  # 1.5%
            "ASIA": 0.018,  # 1.8%
            "CLAN": 0.025,  # 2.5%
            "AMER": 0.016,  # 1.6%
            "MENA": 0.012,  # 1.2%
            "AFRI": 0.010,  # 1.0%
            "PACI": 0.008,  # 0.8%
            "BOTS": 0.005,  # 0.5%
        }
        with get_conn() as conn:
            for ticker, yield_pct in config.items():
                conn.execute(
                    "INSERT OR REPLACE INTO dividend_config(ticker, dividend_yield) VALUES (?, ?)",
                    (ticker, yield_pct)
                )
            conn.commit()

    @staticmethod
    def payout_dividends():
        """Pay quarterly dividends to all shareholders."""
        with get_conn() as conn:
            # Get all tickers and their yields
            cur = conn.execute("SELECT ticker, dividend_yield FROM dividend_config")
            tickers = cur.fetchall()
            
            total_paid = 0
            for ticker, yield_pct in tickers:
                # Get all users holding this ticker
                cur2 = conn.execute(
                    "SELECT discord_id, shares FROM portfolios WHERE ticker = ? AND shares > 0",
                    (ticker,)
                )
                holders = cur2.fetchall()
                
                for discord_id, shares in holders:
                    # Calculate dividend: shares * current_price * yield
                    cur3 = conn.execute(
                        "SELECT gold_pool, share_pool FROM tickers WHERE ticker = ?",
                        (ticker,)
                    )
                    pool_row = cur3.fetchone()
                    if not pool_row:
                        continue
                    
                    gold_pool, share_pool = pool_row
                    current_price = Decimal(gold_pool) / Decimal(share_pool) if share_pool > 0 else Decimal(0)
                    dividend_amount = int(Decimal(shares) * current_price * Decimal(yield_pct))
                    
                    if dividend_amount > 0:
                        conn.execute(
                            "UPDATE users SET internal_gold = internal_gold + ? WHERE discord_id = ?",
                            (dividend_amount, discord_id)
                        )
                        conn.execute(
                            "INSERT INTO dividends_paid(discord_id, ticker, amount) VALUES (?, ?, ?)",
                            (discord_id, ticker, dividend_amount)
                        )
                        total_paid += dividend_amount
            
            conn.commit()
            logger.info(f"Dividend payout complete. Total paid: {total_paid} gold")
            return total_paid


class MarketEvents:
    """Generate and apply random market events."""
    
    EVENTS = [
        {"name": "Tournament Victory", "tickers": ["EURO"], "impact": 0.05, "desc": "🏆 EURO wins regional tournament!"},
        {"name": "Scandal", "tickers": ["ASIA"], "impact": -0.08, "desc": "📉 ASIA scandal breaks news"},
        {"name": "Tech Exploit", "tickers": ["BOTS"], "impact": -0.10, "desc": "🐛 BOTS exploit discovered"},
        {"name": "Streamer Hype", "tickers": ["STRM"], "impact": 0.07, "desc": "🎬 STRM trending on social media"},
        {"name": "Clan War", "tickers": ["CLAN"], "impact": 0.06, "desc": "⚔️ CLAN dominates in warfare"},
        {"name": "Market Crash", "tickers": ["EURO", "ASIA", "AMER"], "impact": -0.15, "desc": "📉 MARKET CRASH: -15% across regions"},
        {"name": "Bull Run", "tickers": ["MENA", "AFRI", "PACI"], "impact": 0.12, "desc": "📈 BULL RUN: Emerging markets surge"},
        {"name": "Economic Boom", "tickers": ["AMER"], "impact": 0.09, "desc": "💰 AMER economy booming"},
    ]
    
    @staticmethod
    def generate_event():
        """Generate a random market event and apply it."""
        event = random.choice(MarketEvents.EVENTS)
        
        with get_conn() as conn:
            # Record event
            conn.execute(
                "INSERT INTO market_events(event_name, event_description, affected_tickers, price_impact_percent) VALUES (?, ?, ?, ?)",
                (event["name"], event["desc"], ",".join(event["tickers"]), event["impact"])
            )
            
            # Apply price impact to affected tickers
            for ticker in event["tickers"]:
                cur = conn.execute(
                    "SELECT gold_pool, share_pool FROM tickers WHERE ticker = ?",
                    (ticker,)
                )
                row = cur.fetchone()
                if not row:
                    continue
                
                gold_pool, share_pool = row
                impact_multiplier = Decimal(1) + Decimal(event["impact"])
                new_gold_pool = int(Decimal(gold_pool) * impact_multiplier)
                
                conn.execute(
                    "UPDATE tickers SET gold_pool = ?, last_updated = CURRENT_TIMESTAMP WHERE ticker = ?",
                    (new_gold_pool, ticker)
                )
            
            conn.commit()
            logger.info(f"Market event: {event['name']} - Impact: {event['impact']*100:+.1f}%")
            return event

# The territorial transactions endpoint returns CSV-like lines: time,sender,receiver,amount,fee
# We parse rows and use timestamp+sender as tx_id. We persist last_seen_time to ledger_state to avoid reprocessing.

async def fetch_live_ledger_rows(since_ts):
    """
    Fetch the live ledger page and parse rows newer than since_ts.
    Returns list of dicts: {tx_id, time, sender, receiver, amount, fee, raw_line}
    Improved error handling with longer timeouts and connection pooling.
    """
    try:
        import aiohttp
    except Exception:
        logger.warning("aiohttp not installed; live ledger fetch unavailable.")
        return []

    rows = []
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Accept": "text/plain, */*",
        "Accept-Encoding": "gzip, deflate",
        "Connection": "keep-alive",
        "Keep-Alive": "timeout=60, max=100",
    }

    max_retries = 5
    text = ""
    
    # Use longer timeouts and better connector settings
    timeout = aiohttp.ClientTimeout(total=120, connect=30, sock_read=60)
    connector = aiohttp.TCPConnector(
        limit=5, 
        limit_per_host=2, 
        ttl_dns_cache=300,
        ssl=False,
        keepalive_timeout=60,
        enable_cleanup_closed=True
    )
    
    # Create session once outside the loop to avoid session closed errors
    async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
        for attempt in range(max_retries):
            try:
                logger.debug(f"Ledger fetch attempt {attempt + 1}/{max_retries}")
                async with session.get(LEDGER_URL, headers=headers, ssl=False) as resp:
                    if resp.status != 200:
                        logger.warning("Ledger fetch returned status %s on attempt %d", resp.status, attempt + 1)
                        if attempt < max_retries - 1:
                            await asyncio.sleep(min(2 ** attempt, 16))  # exponential backoff, max 16s
                            continue
                        return []
                    # Read response in chunks to handle large responses
                    try:
                        text = await resp.text(errors='ignore')
                        logger.debug(f"Successfully fetched ledger ({len(text)} bytes) on attempt {attempt + 1}")
                        break
                    except asyncio.TimeoutError:
                        logger.warning("Timeout reading response on attempt %d", attempt + 1)
                        if attempt < max_retries - 1:
                            await asyncio.sleep(min(2 ** attempt, 16))
                            continue
                        return []
            except aiohttp.ServerDisconnectedError as e:
                logger.warning("Ledger fetch attempt %d/%d failed (server disconnected)", attempt + 1, max_retries)
                if attempt < max_retries - 1:
                    await asyncio.sleep(min(2 ** attempt, 16))
                    continue
                logger.error("Failed to fetch live ledger after %d attempts (server disconnected)", max_retries)
                return []
            except (aiohttp.ClientError, aiohttp.ClientConnectorError) as e:
                logger.warning("Ledger fetch attempt %d/%d failed: %s", attempt + 1, max_retries, type(e).__name__)
                if attempt < max_retries - 1:
                    await asyncio.sleep(min(2 ** attempt, 16))
                    continue
                logger.error("Failed to fetch live ledger after %d attempts", max_retries)
                return []
            except asyncio.TimeoutError:
                logger.warning("Ledger fetch attempt %d/%d timed out", attempt + 1, max_retries)
                if attempt < max_retries - 1:
                    await asyncio.sleep(min(2 ** (attempt + 1), 16))
                    continue
                logger.error("Failed to fetch live ledger after %d attempts (timeout)", max_retries)
                return []
            except Exception as e:
                logger.error("Unexpected error fetching ledger on attempt %d: %s", attempt + 1, str(e))
                if attempt < max_retries - 1:
                    await asyncio.sleep(min(2 ** attempt, 16))
                    continue
                return []

    if not text:
        logger.warning("No ledger text received after %d attempts", max_retries)
        return []

    # The endpoint returns CSV-like content. Parse lines.
    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue
        # Some lines may be header or non-CSV; attempt to parse CSV fields
        parts = [p.strip() for p in line.split(",")]
        if len(parts) < 5:
            continue
        try:
            ts = int(parts[0])
            sender = parts[1]
            receiver = parts[2]
            amount = int(parts[3])
            fee = int(parts[4])
        except Exception:
            continue
        if ts <= since_ts:
            continue
        tx_id = f"{ts}_{sender}_{receiver}_{amount}"
        rows.append({
            "tx_id": tx_id,
            "time": ts,
            "sender": sender,
            "receiver": receiver,
            "amount": amount,
            "fee": fee,
            "raw_line": line
        })
    
    if rows:
        logger.info(f"Parsed {len(rows)} new ledger entries")
    return rows


def load_mock_ledger_entries():
    if not os.path.exists(LEDGER_FILE):
        return []
    try:
        with open(LEDGER_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
            if isinstance(data, list):
                return data
            return []
    except Exception:
        logger.exception("Failed to load mock ledger")
        return []


def process_rows_in_db(rows):
    """
    Process ledger rows (list of dicts) in DB thread.
    - Verifies pending accounts by matching sender account name + receiver == clan bank + exact amount.
    - Credits internal_gold for any transfer sent to the clan bank (deposits).
    - Auto-completes pending withdrawals when exact amount is sent to clan bank.
    Returns (count, newly_verified) where newly_verified is list of (discord_id, account_name).
    """
    if not rows:
        return 0, []
    processed = 0
    newly_verified = []
    completed_withdrawals = []
    with get_conn() as conn:
        conn.execute("PRAGMA foreign_keys = ON")
        cur = conn.execute("SELECT log_id, actor, details FROM audit_logs WHERE action = 'pending_nonce'")
        pending = cur.fetchall()
        for e in rows:
            tx_id = e.get("tx_id")
            sender = e.get("sender", "")
            receiver = e.get("receiver", "")
            amount = int(e.get("amount", 0))
            memo = e.get("memo")
            # Skip if already processed
            cur2 = conn.execute("SELECT tx_id FROM verifications WHERE tx_id = ?", (tx_id,))
            if cur2.fetchone():
                continue
            # Only care about transfers to the clan bank
            if receiver != CLAN_BANK_ACCOUNT:
                continue
            # Credit gold to any existing verified user who sent to the clan bank (deposits)
            cur3 = conn.execute("SELECT discord_id, verified FROM users WHERE game_name = ?", (sender,))
            user_row = cur3.fetchone()
            if user_row and user_row[1]:
                # Check if this matches a pending withdrawal
                cur_withdrawal = conn.execute(
                    "SELECT withdrawal_id, amount FROM withdrawals WHERE discord_id = ? AND status = 'PENDING' ORDER BY requested_at ASC LIMIT 1",
                    (user_row[0],)
                )
                withdrawal_row = cur_withdrawal.fetchone()
                if withdrawal_row and withdrawal_row[1] == amount:
                    # Auto-complete the withdrawal
                    withdrawal_id, _ = withdrawal_row
                    conn.execute("UPDATE withdrawals SET status = 'COMPLETED' WHERE withdrawal_id = ?", (withdrawal_id,))
                    completed_withdrawals.append((user_row[0], sender, amount, withdrawal_id))
                    conn.commit()
                    continue
                # Regular deposit (no matching withdrawal)
                conn.execute("UPDATE users SET internal_gold = internal_gold + ? WHERE discord_id = ?",
                             (amount, user_row[0]))
                conn.commit()
                continue
            # Attempt to match a pending verification
            # Match by: sender account name == pending account name AND amount == VERIFICATION_AMOUNT
            for log_id, actor, details in pending:
                try:
                    details_obj = json.loads(details)
                    discord_id = details_obj.get("discord_id")
                    gname = details_obj.get("game_name")
                    nonce = details_obj.get("nonce")
                except Exception:
                    continue
                sender_matches = (sender == gname)
                memo_matches = (memo is not None and memo == nonce)
                if amount == VERIFICATION_AMOUNT and (sender_matches or memo_matches):
                    conn.execute(
                        "INSERT OR IGNORE INTO verifications(tx_id, discord_id, game_name, nonce, raw_payload, verified_at) "
                        "VALUES (?, ?, ?, ?, ?, ?)",
                        (tx_id, discord_id, gname, nonce, json.dumps(e), datetime.now(datetime.timezone.utc).isoformat())
                    )
                    conn.execute("UPDATE users SET verified = 1, internal_gold = internal_gold + ? WHERE discord_id = ?",
                                 (amount, discord_id))
                    conn.execute("DELETE FROM audit_logs WHERE log_id = ?", (log_id,))
                    conn.commit()
                    newly_verified.append((discord_id, gname))
                    processed += 1
                    break
    
    # Send notifications for auto-completed withdrawals
    if completed_withdrawals:
        try:
            for discord_id, game_name, amount, wid in completed_withdrawals:
                # These will be logged by the background task
                logger.info(f"Withdrawal #{wid} auto-completed: {game_name} sent {amount} gold")
        except Exception:
            pass
    
    return processed, newly_verified


async def ingest_ledger_cycle():
    """
    One cycle: fetch live rows newer than last_seen, process them.
    If live fetch fails or returns nothing, fall back to mock ledger entries.
    """
    last_seen = get_ledger_last_seen()
    rows = []
    try:
        rows = await fetch_live_ledger_rows(last_seen)
    except Exception:
        logger.exception("Live ledger fetch failed")
        rows = []
    # If no live rows, try mock ledger (useful for closed beta)
    if not rows:
        mock = load_mock_ledger_entries()
        # convert mock entries to same shape and filter by time if possible
        for e in mock:
            try:
                ts = int(e.get("timestamp_ts", 0)) if e.get("timestamp_ts") else 0
                # if timestamp not provided, use current time
                if ts == 0:
                    ts = int(datetime.now(datetime.timezone.utc).timestamp())
                tx_id = e.get("tx_id") or f"{ts}_{e.get('game_name')}_{e.get('amount')}"
                rows.append({
                    "tx_id": tx_id,
                    "time": ts,
                    "sender": e.get("sender", ""),
                    "receiver": e.get("game_name", ""),
                    "amount": int(e.get("amount", 0)),
                    "fee": int(e.get("fee", 0)),
                    "memo": e.get("memo"),
                    "raw_line": json.dumps(e)
                })
            except Exception:
                continue
    # Filter rows by last_seen and sort ascending
    rows = [r for r in rows if int(r.get("time", 0)) > last_seen]
    rows.sort(key=lambda x: int(x.get("time", 0)))
    if not rows:
        return 0, []
    # Process rows in DB thread
    processed, newly_verified = await asyncio.to_thread(process_rows_in_db, rows)
    # Update last_seen to max time of processed rows
    max_ts = max(int(r.get("time", 0)) for r in rows)
    set_ledger_last_seen(max_ts)
    return processed, newly_verified


# ---- Simple admin export ----
def export_withdrawals_csv():
    with get_conn() as conn:
        df_rows = []
        cur = conn.execute("SELECT w.withdrawal_id, w.discord_id, u.game_name, w.amount, w.requested_at FROM withdrawals w JOIN users u ON w.discord_id = u.discord_id WHERE w.status = 'PENDING'")
        rows = cur.fetchall()
        for r in rows:
            df_rows.append({
                "withdrawal_id": r[0],
                "discord_id": r[1],
                "game_name": r[2],
                "amount": r[3],
                "requested_at": r[4]
            })
    if not df_rows:
        path = os.path.join("exports", f"withdrawals_empty_{datetime.now(datetime.timezone.utc).strftime('%Y%m%d%H%M%S')}.csv")
        with open(path, "w", encoding="utf-8") as f:
            f.write("empty\n")
        return path
    import csv
    path = os.path.join("exports", f"withdrawals_{datetime.now(datetime.timezone.utc).strftime('%Y%m%d%H%M%S')}.csv")
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["withdrawal_id", "discord_id", "game_name", "amount", "requested_at"])
        writer.writeheader()
        for row in df_rows:
            writer.writerow(row)
    return path


# ---- Clan bank account for verification ----
CLAN_BANK_ACCOUNT = os.getenv("CLAN_BANK_ACCOUNT", "vVtNN")

# ---- Discord bot (slash commands with autocomplete) ----
try:
    import discord
    from discord import app_commands
    from discord.ext import commands
except Exception:
    discord = None

if discord is None:
    logger.warning("discord.py not installed. Install with: pip install discord.py aiohttp")
else:
    intents = discord.Intents.default()
    intents.message_content = True
    bot = commands.Bot(command_prefix="!", intents=intents)
    amm = AMM()

    TICKERS = ["EURO", "STRM", "ASIA", "CLAN", "AMER", "MENA", "AFRI", "PACI", "BOTS"]

    # Volatility per 3-min drift cycle (std-dev fraction of gold pool)
    TICKER_VOLATILITY = {
        "EURO": 0.008,   # 0.8% — large-cap, stable
        "STRM": 0.025,   # 2.5% — influencer-driven, swings
        "ASIA": 0.010,
        "CLAN": 0.015,
        "AMER": 0.012,
        "MENA": 0.016,
        "AFRI": 0.018,
        "PACI": 0.020,
        "BOTS": 0.040,   # 4.0% — highly speculative
    }
    PRICE_DRIFT_INTERVAL = 180   # seconds between automatic price updates
    MOMENTUM_FACTOR    = 0.30   # how much previous drift biases the next one (0 = no memory, 1 = full carry)
    PRICE_HISTORY_DAYS = 7      # days of price history to retain

    async def ticker_autocomplete(interaction: discord.Interaction, current: str):
        return [
            app_commands.Choice(name=t, value=t)
            for t in TICKERS if current.upper() in t
        ]

    def store_pending_nonce(discord_id, game_name, nonce):
        with get_conn() as conn:
            conn.execute("INSERT OR IGNORE INTO users(discord_id, game_name, internal_gold, verified) VALUES (?, ?, 0, 0)",
                         (discord_id, game_name))
            details = json.dumps({"discord_id": discord_id, "game_name": game_name, "nonce": nonce})
            conn.execute("INSERT INTO audit_logs(actor, action, details) VALUES (?, 'pending_nonce', ?)",
                         (discord_id, details))
            conn.commit()

    async def is_verified_async(discord_id):
        def _check(did):
            with get_conn() as conn:
                cur = conn.execute("SELECT verified FROM users WHERE discord_id = ?", (did,))
                row = cur.fetchone()
                return bool(row and row[0])
        return await asyncio.to_thread(_check, discord_id)

    # ---- User slash commands ----

    @bot.tree.command(name="help", description="List all available commands")
    async def help_cmd(interaction: discord.Interaction):
        lines = [
            "**Territorial Market Bot — Commands**",
            "",
        ]
        commands = sorted(
            [cmd for cmd in bot.tree.walk_commands() if cmd.name != "help"],
            key=lambda c: c.name
        )
        for cmd in commands:
            lines.append(f"`/{cmd.name}` — {cmd.description}")
        lines.append("")
        lines.append("Use `/help` to see this list anytime.")
        await interaction.response.send_message("\n".join(lines), ephemeral=True)

    @bot.tree.command(name="link", description="Link your Territorial.io account to verify yourself")
    @app_commands.describe(game_name="Your exact Territorial.io account name")
    async def link(interaction: discord.Interaction, game_name: str):
        nonce = uuid.uuid4().hex[:12]
        await asyncio.to_thread(store_pending_nonce, str(interaction.user.id), game_name, nonce)
        try:
            await interaction.user.send(
                f"**Account Verification**\n\n"
                f"To verify your account **{game_name}**, send exactly **{VERIFICATION_AMOUNT} gold** "
                f"to **{CLAN_BANK_ACCOUNT}** in Territorial.io.\n\n"
                f"The bot checks the ledger every {LEDGER_POLL_INTERVAL} seconds and will verify you automatically. "
                f"Make sure the amount is exactly **{VERIFICATION_AMOUNT} gold** and that you are sending from the account **{game_name}**."
            )
            await interaction.response.send_message("Verification instructions sent to your DMs.", ephemeral=True)
        except Exception:
            await interaction.response.send_message(
                "Could not send you a DM. Please open your DMs and try again, or contact an admin.",
                ephemeral=True
            )

    @bot.tree.command(name="buy", description="Buy shares of a ticker using your in-game gold")
    @app_commands.describe(ticker="Which ticker to buy", gold="Amount of gold to spend")
    @app_commands.autocomplete(ticker=ticker_autocomplete)
    async def buy(interaction: discord.Interaction, ticker: str, gold: int):
        await interaction.response.defer()
        if gold <= 0:
            await interaction.followup.send("Gold must be a positive integer.")
            return
        discord_id = str(interaction.user.id)
        verified = await is_verified_async(discord_id)
        if not verified:
            await interaction.followup.send("Your account is not verified. Use `/link` first.")
            return
        res = await asyncio.to_thread(amm.buy, discord_id, ticker.upper(), gold)
        if not res.get("ok"):
            reason = res.get("reason", "unknown").replace("_", " ")
            await interaction.followup.send(f"Trade rejected: {reason}.")
        else:
            await interaction.followup.send(
                f"Bought **{res['shares']:,} shares** of `{ticker.upper()}` "
                f"at {res['price_after']:.6f} gold/share. Fee: {res['fee']:,} gold."
            )
            log_ch = bot.get_channel(ACTIVITY_LOG_CHANNEL_ID)
            trade_log_ch = bot.get_channel(TRADE_LOG_CHANNEL_ID)
            if log_ch:
                try:
                    name = await asyncio.to_thread(
                        lambda: get_conn().execute("SELECT game_name FROM users WHERE discord_id=?", (discord_id,)).fetchone()
                    )
                    label = name[0] if name else str(interaction.user)
                    await log_ch.send(
                        f"BUY  `{ticker.upper()}`  **{res['shares']:,} shares** — {label} "
                        f"| price {res['price_after']:.4f} | fee {res['fee']:,}"
                    )
                except Exception:
                    logger.exception("Failed to post buy activity log")
            if trade_log_ch:
                try:
                    name = await asyncio.to_thread(
                        lambda: get_conn().execute("SELECT game_name FROM users WHERE discord_id=?", (discord_id,)).fetchone()
                    )
                    label = name[0] if name else str(interaction.user)
                    embed = discord.Embed(
                        title="📈 BUY Trade",
                        color=discord.Color.green(),
                        timestamp=datetime.now(datetime.timezone.utc)
                    )
                    embed.add_field(name="Player", value=label, inline=True)
                    embed.add_field(name="Ticker", value=f"`{ticker.upper()}`", inline=True)
                    embed.add_field(name="Shares", value=f"{res['shares']:,}", inline=True)
                    embed.add_field(name="Price/Share", value=f"{res['price_after']:.6f}", inline=True)
                    embed.add_field(name="Total Spent", value=f"{gold:,}", inline=True)
                    embed.add_field(name="Fee", value=f"{res['fee']:,}", inline=True)
                    await trade_log_ch.send(embed=embed)
                except Exception:
                    logger.exception("Failed to post buy trade log")

    @bot.tree.command(name="sell", description="Sell shares of a ticker to get gold back")
    @app_commands.describe(ticker="Which ticker to sell", shares="Number of shares to sell")
    @app_commands.autocomplete(ticker=ticker_autocomplete)
    async def sell(interaction: discord.Interaction, ticker: str, shares: int):
        await interaction.response.defer()
        if shares <= 0:
            await interaction.followup.send("Shares must be a positive integer.")
            return
        discord_id = str(interaction.user.id)
        verified = await is_verified_async(discord_id)
        if not verified:
            await interaction.followup.send("Your account is not verified. Use `/link` first.")
            return
        res = await asyncio.to_thread(amm.sell, discord_id, ticker.upper(), shares)
        if not res.get("ok"):
            reason = res.get("reason", "unknown").replace("_", " ")
            await interaction.followup.send(f"Trade rejected: {reason}.")
        else:
            await interaction.followup.send(
                f"Sold **{shares:,} shares** of `{ticker.upper()}` for **{res['gold']:,} gold**. "
                f"Fee: {res['fee']:,} gold."
            )
            log_ch = bot.get_channel(ACTIVITY_LOG_CHANNEL_ID)
            trade_log_ch = bot.get_channel(TRADE_LOG_CHANNEL_ID)
            if log_ch:
                try:
                    name = await asyncio.to_thread(
                        lambda: get_conn().execute("SELECT game_name FROM users WHERE discord_id=?", (discord_id,)).fetchone()
                    )
                    label = name[0] if name else str(interaction.user)
                    await log_ch.send(
                        f"SELL `{ticker.upper()}`  **{shares:,} shares** → **{res['gold']:,} gold** — {label} "
                        f"| price {res['price_after']:.4f} | fee {res['fee']:,}"
                    )
                except Exception:
                    logger.exception("Failed to post sell activity log")
            if trade_log_ch:
                try:
                    name = await asyncio.to_thread(
                        lambda: get_conn().execute("SELECT game_name FROM users WHERE discord_id=?", (discord_id,)).fetchone()
                    )
                    label = name[0] if name else str(interaction.user)
                    embed = discord.Embed(
                        title="📉 SELL Trade",
                        color=discord.Color.red(),
                        timestamp=datetime.now(datetime.timezone.utc)
                    )
                    embed.add_field(name="Player", value=label, inline=True)
                    embed.add_field(name="Ticker", value=f"`{ticker.upper()}`", inline=True)
                    embed.add_field(name="Shares", value=f"{shares:,}", inline=True)
                    embed.add_field(name="Price/Share", value=f"{res['price_after']:.6f}", inline=True)
                    embed.add_field(name="Total Received", value=f"{res['gold']:,}", inline=True)
                    embed.add_field(name="Fee", value=f"{res['fee']:,}", inline=True)
                    await trade_log_ch.send(embed=embed)
                except Exception:
                    logger.exception("Failed to post sell trade log")

    @bot.tree.command(name="portfolio", description="View your gold balance and share holdings")
    async def portfolio(interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)
        discord_id = str(interaction.user.id)
        def _portfolio(did):
            with get_conn() as conn:
                cur = conn.execute("SELECT internal_gold, game_name, verified FROM users WHERE discord_id = ?", (did,))
                row = cur.fetchone()
                if not row:
                    return None, []
                gold, game_name, verified = row[0], row[1], row[2]
                cur = conn.execute(
                    "SELECT p.ticker, p.shares, t.gold_pool, t.share_pool, t.is_frozen "
                    "FROM portfolios p JOIN tickers t ON p.ticker = t.ticker "
                    "WHERE p.discord_id = ? AND p.shares > 0",
                    (did,)
                )
                positions = cur.fetchall()
            return (gold, game_name, verified), positions
        info, positions = await asyncio.to_thread(_portfolio, discord_id)
        if info is None:
            await interaction.followup.send("No account found. Use `/link` to get started.")
            return
        gold, game_name, verified = info
        verified_tag = "verified" if verified else "unverified"
        lines = [
            f"**Portfolio — {game_name}** ({verified_tag})",
            f"Gold balance: **{gold:,}**",
        ]
        if not positions:
            lines.append("No share positions held.")
        else:
            lines.append("")
            lines.append("**Holdings**")
            total_value = 0
            for ticker, shares, gold_pool, share_pool, is_frozen in positions:
                price = gold_pool / share_pool if share_pool else 0
                value = int(shares * price)
                total_value += value
                frozen_tag = " [frozen]" if is_frozen else ""
                lines.append(f"  `{ticker}`{frozen_tag}  {shares:,} shares @ {price:.4f}  = {value:,} gold")
            lines.append(f"")
            lines.append(f"Holdings value: **{total_value:,} gold**")
            lines.append(f"Total: **{gold + total_value:,} gold**")
        await interaction.followup.send("\n".join(lines))

    @bot.tree.command(name="market", description="View current prices for all tickers")
    async def market(interaction: discord.Interaction):
        await interaction.response.defer()
        def _market():
            with get_conn() as conn:
                cur = conn.execute("SELECT ticker, gold_pool, share_pool, day_start_price, is_frozen FROM tickers ORDER BY ticker")
                return cur.fetchall()
        rows = await asyncio.to_thread(_market)
        lines = ["**Territorial Market**", ""]
        for ticker, gold_pool, share_pool, day_start_price, is_frozen in rows:
            price = gold_pool / share_pool if share_pool else 0
            change = ((price - day_start_price) / day_start_price * 100) if day_start_price else 0
            direction = "+" if change >= 0 else ""
            frozen = "  [frozen]" if is_frozen else ""
            lines.append(f"`{ticker}`{frozen}   {price:.4f} gold/share   {direction}{change:.2f}%")
        await interaction.followup.send("\n".join(lines))

    @bot.tree.command(name="leaderboard", description="Market leaderboard — ranked by different metrics")
    @app_commands.describe(sort="What to rank players by")
    @app_commands.choices(sort=[
        app_commands.Choice(name="Net worth (gold + holdings)", value="value"),
        app_commands.Choice(name="Gold balance",                value="gold"),
        app_commands.Choice(name="Holdings value",              value="holdings"),
        app_commands.Choice(name="Total shares held",           value="shares"),
    ])
    async def leaderboard(interaction: discord.Interaction, sort: str = "value"):
        await interaction.response.defer()
        def _lb(sort):
            with get_conn() as conn:
                users = conn.execute(
                    "SELECT discord_id, game_name, internal_gold FROM users "
                    "WHERE verified = 1"
                ).fetchall()
                rows = []
                for did, name, gold in users:
                    hval = conn.execute(
                        "SELECT COALESCE(SUM(p.shares*(CAST(t.gold_pool AS REAL)/t.share_pool)),0) "
                        "FROM portfolios p JOIN tickers t ON p.ticker=t.ticker WHERE p.discord_id=?", (did,)
                    ).fetchone()[0]
                    tot_shares = conn.execute(
                        "SELECT COALESCE(SUM(shares),0) FROM portfolios WHERE discord_id=?", (did,)
                    ).fetchone()[0]
                    rows.append((name, gold, int(hval), int(tot_shares), gold + int(hval)))
                if sort == "gold":
                    rows.sort(key=lambda x: x[1], reverse=True)
                elif sort == "holdings":
                    rows.sort(key=lambda x: x[2], reverse=True)
                elif sort == "shares":
                    rows.sort(key=lambda x: x[3], reverse=True)
                else:
                    rows.sort(key=lambda x: x[4], reverse=True)
                return rows[:10]
        rows = await asyncio.to_thread(_lb, sort)
        labels = {"value": "Net Worth", "gold": "Gold Balance",
                  "holdings": "Holdings Value", "shares": "Total Shares"}
        col = labels.get(sort, "Net Worth")
        lines = [f"**Leaderboard — {col}**", ""]
        medals = ["🥇", "🥈", "🥉"]
        for i, (name, gold, hval, shares, networth) in enumerate(rows):
            prefix = medals[i] if i < 3 else f"`{i+1}.`"
            if sort == "gold":
                stat = f"{gold:,} gold"
            elif sort == "holdings":
                stat = f"{hval:,} gold"
            elif sort == "shares":
                stat = f"{shares:,} shares"
            else:
                stat = f"{networth:,} gold"
            lines.append(f"{prefix} **{name}** — {stat}")
        if not rows:
            lines.append("No verified accounts yet.")
        await interaction.followup.send("\n".join(lines))

    @bot.tree.command(name="trend", description="Show a price history graph for a ticker")
    @app_commands.describe(
        ticker="Which ticker to chart",
        hours="How many hours of history to show (1–168, default 24)"
    )
    @app_commands.autocomplete(ticker=ticker_autocomplete)
    async def trend(interaction: discord.Interaction, ticker: str, hours: int = 24):
        await interaction.response.defer()
        hours = max(1, min(168, hours))
        sym = ticker.upper()

        def _fetch_history(sym, hours):
            cutoff = (datetime.now(datetime.timezone.utc) - timedelta(hours=hours)).isoformat()
            with get_conn() as conn:
                # Current ticker info
                trow = conn.execute(
                    "SELECT gold_pool, share_pool, day_start_price, is_frozen FROM tickers WHERE ticker = ?", (sym,)
                ).fetchone()
                if not trow:
                    return None, []
                # Price snapshots in the window
                rows = conn.execute(
                    "SELECT price, recorded_at FROM price_history "
                    "WHERE ticker = ? AND recorded_at >= ? ORDER BY recorded_at ASC",
                    (sym, cutoff)
                ).fetchall()
                return trow, rows

        trow, rows = await asyncio.to_thread(_fetch_history, sym, hours)
        if trow is None:
            await interaction.followup.send(f"Ticker `{sym}` not found.", ephemeral=True)
            return

        gp, sp, day_start, is_frozen = trow
        current_price = gp / sp if sp else 0

        # Build sparkline — sample to ~60 chars max
        SPARK = "▁▂▃▄▅▆▇█"

        def make_sparkline(prices, width=60):
            if len(prices) < 2:
                return None
            # downsample
            step = max(1, len(prices) // width)
            sampled = [prices[i] for i in range(0, len(prices), step)]
            lo, hi = min(sampled), max(sampled)
            span = hi - lo
            if span == 0:
                return SPARK[3] * len(sampled)
            return "".join(SPARK[min(7, int((p - lo) / span * 7.9999))] for p in sampled)

        prices = [r[0] for r in rows]

        # Always include the current price as the last point
        prices.append(current_price)

        lines = []
        frozen_tag = "  [FROZEN]" if is_frozen else ""

        if len(prices) < 2:
            # No history yet — bot just started
            lines.append(f"**{sym}**{frozen_tag} — no history yet (price data accumulates every 5 min)")
            lines.append(f"Current price: **{current_price:.4f}** gold/share")
        else:
            open_price = prices[0]
            high_price = max(prices)
            low_price  = min(prices)
            pct_change = ((current_price - open_price) / open_price * 100) if open_price else 0
            direction  = "+" if pct_change >= 0 else ""
            day_pct    = ((current_price - day_start) / day_start * 100) if day_start else 0
            day_dir    = "+" if day_pct >= 0 else ""

            spark = make_sparkline(prices)
            data_points = len(rows)
            label = f"last {hours}h" if hours > 1 else "last 1h"

            lines.append(f"**{sym}**{frozen_tag}  —  {label}  ({data_points} snapshots)")
            lines.append(f"```")
            lines.append(spark)
            lines.append(f"```")
            lines.append(
                f"Open: **{open_price:.4f}**   "
                f"High: **{high_price:.4f}**   "
                f"Low: **{low_price:.4f}**   "
                f"Now: **{current_price:.4f}**"
            )
            lines.append(
                f"Period change: **{direction}{pct_change:.2f}%**   "
                f"24h baseline: **{day_dir}{day_pct:.2f}%**"
            )

        await interaction.followup.send("\n".join(lines))

    @bot.tree.command(name="status", description="Your net worth at a glance")
    async def status(interaction: discord.Interaction):
        did = str(interaction.user.id)
        def _status(did):
            with get_conn() as conn:
                row = conn.execute(
                    "SELECT game_name, internal_gold, verified FROM users WHERE discord_id = ?", (did,)
                ).fetchone()
                if not row:
                    return None
                name, gold, verified = row
                holdings_val = conn.execute(
                    "SELECT COALESCE(SUM(p.shares * (CAST(t.gold_pool AS REAL)/t.share_pool)),0) "
                    "FROM portfolios p JOIN tickers t ON p.ticker=t.ticker WHERE p.discord_id=?", (did,)
                ).fetchone()[0]
                return name, gold, int(holdings_val), verified
        res = await asyncio.to_thread(_status, did)
        if not res:
            await interaction.response.send_message("No account found. Use `/link` to get started.", ephemeral=True)
            return
        name, gold, holdings, verified = res
        tag = "✓" if verified else "unverified"
        await interaction.response.send_message(
            f"**{name}** {tag}  —  "
            f"Gold: **{gold:,}**  |  Holdings: **{holdings:,}**  |  Net worth: **{gold+holdings:,}**",
            ephemeral=True
        )

    @bot.tree.command(name="deposit", description="Instructions on how to deposit gold into the market")
    async def deposit(interaction: discord.Interaction):
        discord_id = str(interaction.user.id)
        def _get_user(did):
            with get_conn() as conn:
                cur = conn.execute("SELECT game_name, verified FROM users WHERE discord_id = ?", (did,))
                return cur.fetchone()
        row = await asyncio.to_thread(_get_user, discord_id)
        if not row:
            await interaction.response.send_message(
                "You have no account yet. Use `/link` first to link your game account.",
                ephemeral=True
            )
            return
        game_name, verified = row
        if not verified:
            await interaction.response.send_message(
                "Your account is not verified yet. Complete `/link` verification before depositing.",
                ephemeral=True
            )
            return
        await interaction.response.send_message(
            f"**How to deposit gold**\n\n"
            f"Send any amount of gold to **{CLAN_BANK_ACCOUNT}** in-game from your account (**{game_name}**).\n"
            f"The bot detects the transfer automatically within {LEDGER_POLL_INTERVAL} seconds "
            f"and credits your balance here.",
            ephemeral=True
        )

    @bot.tree.command(name="withdraw", description="Request a withdrawal of your gold back to the game")
    @app_commands.describe(amount="Amount of gold to withdraw")
    async def withdraw(interaction: discord.Interaction, amount: int):
        if amount <= 0:
            await interaction.response.send_message("Amount must be a positive integer.", ephemeral=True)
            return
        discord_id = str(interaction.user.id)
        def _withdraw(did, amt):
            with get_conn() as conn:
                cur = conn.execute("SELECT internal_gold, game_name, verified FROM users WHERE discord_id = ?", (did,))
                row = cur.fetchone()
                if not row:
                    return "no_account"
                gold, game_name, verified = row
                if not verified:
                    return "not_verified"
                if gold < amt:
                    return f"insufficient_gold:{gold}"
                conn.execute("UPDATE users SET internal_gold = internal_gold - ? WHERE discord_id = ?", (amt, did))
                conn.execute(
                    "INSERT INTO withdrawals(discord_id, amount, status) VALUES (?, ?, 'PENDING')",
                    (did, amt)
                )
                conn.commit()
                return f"ok:{game_name}"
        result = await asyncio.to_thread(_withdraw, discord_id, amount)
        if result == "no_account":
            await interaction.response.send_message("No account found. Use `/link` to get started.", ephemeral=True)
        elif result == "not_verified":
            await interaction.response.send_message("Your account is not verified. Use `/link` first.", ephemeral=True)
        elif result.startswith("insufficient_gold"):
            current = result.split(":")[1]
            await interaction.response.send_message(
                f"Insufficient balance. You have **{int(current):,} gold** but requested **{amount:,}**.",
                ephemeral=True
            )
        else:
            game_name = result.split(":", 1)[1]
            await interaction.response.send_message(
                f"Withdrawal of **{amount:,} gold** to **{game_name}** submitted.\n"
                f"An admin will process it and send the gold in-game. "
                f"Processing times may vary.",
                ephemeral=True
            )
            log_ch = bot.get_channel(WITHDRAWAL_LOG_CHANNEL_ID)
            if log_ch:
                try:
                    await log_ch.send(
                        f"**Withdrawal request** — `#{interaction.user.id}`\n"
                        f"Player: **{game_name}** | Amount: **{amount:,} gold**\n"
                        f"Use `/admin_list_withdrawals` to view all pending."
                    )
                except Exception:
                    logger.exception("Failed to post withdrawal notification")

    # ---- Admin slash commands ----

    def _check_admin(interaction: discord.Interaction) -> bool:
        return str(interaction.user.id) in ADMIN_IDS

    @bot.tree.command(name="force_verify", description="[Admin] Manually verify a user")
    @app_commands.describe(discord_id="User's Discord ID", tx_id="Transaction ID", game_name="User's game name")
    async def force_verify(interaction: discord.Interaction, discord_id: str, tx_id: str, game_name: str):
        if not _check_admin(interaction):
            await interaction.response.send_message("This command is restricted to the admin channel.", ephemeral=True)
            return
        def _force(did, tx, gname):
            with get_conn() as conn:
                conn.execute("INSERT OR IGNORE INTO users(discord_id, game_name, internal_gold, verified) VALUES (?, ?, 0, 1)", (did, gname))
                conn.execute("INSERT OR IGNORE INTO verifications(tx_id, discord_id, game_name, nonce, raw_payload, verified_at) VALUES (?, ?, ?, ?, ?, ?)",
                             (tx, did, gname, "FORCE", json.dumps({"forced_by": str(interaction.user.id), "tx_id": tx}), datetime.now(datetime.timezone.utc).isoformat()))
                conn.commit()
        await asyncio.to_thread(_force, discord_id, tx_id, game_name)
        await interaction.response.send_message(f"Verification applied: `{discord_id}` registered as **{game_name}**.")

    @bot.tree.command(name="freeze", description="[Admin] Freeze a ticker to halt trading")
    @app_commands.describe(ticker="Ticker to freeze")
    @app_commands.autocomplete(ticker=ticker_autocomplete)
    async def freeze(interaction: discord.Interaction, ticker: str):
        if not _check_admin(interaction):
            await interaction.response.send_message("This command is restricted to the admin channel.", ephemeral=True)
            return
        def _freeze(t):
            with get_conn() as conn:
                conn.execute("UPDATE tickers SET is_frozen = 1 WHERE ticker = ?", (t.upper(),))
                conn.commit()
        await asyncio.to_thread(_freeze, ticker)
        await interaction.response.send_message(f"`{ticker.upper()}` has been frozen. Trading halted.")

    @bot.tree.command(name="unfreeze", description="[Admin] Unfreeze a ticker to resume trading")
    @app_commands.describe(ticker="Ticker to unfreeze")
    @app_commands.autocomplete(ticker=ticker_autocomplete)
    async def unfreeze(interaction: discord.Interaction, ticker: str):
        if not _check_admin(interaction):
            await interaction.response.send_message("This command is restricted to the admin channel.", ephemeral=True)
            return
        def _unfreeze(t):
            with get_conn() as conn:
                conn.execute("UPDATE tickers SET is_frozen = 0 WHERE ticker = ?", (t.upper(),))
                conn.commit()
        await asyncio.to_thread(_unfreeze, ticker)
        await interaction.response.send_message(f"`{ticker.upper()}` has been unfrozen. Trading resumed.")

    @bot.tree.command(name="post_market", description="[Admin] Post the live market embed to this channel")
    async def post_market(interaction: discord.Interaction):
        if not _check_admin(interaction):
            await interaction.response.send_message("This command is restricted to the admin channel.", ephemeral=True)
            return
        await interaction.response.defer(ephemeral=True)
        rows = await asyncio.to_thread(_get_market_data)
        embed = _build_market_embed(rows)
        await interaction.channel.send(embed=embed)
        await interaction.followup.send("Market embed posted.", ephemeral=True)

    @bot.tree.command(name="audit", description="[Admin] Look up a verification by transaction ID")
    @app_commands.describe(tx_id="Transaction ID to audit")
    async def audit(interaction: discord.Interaction, tx_id: str):
        if not _check_admin(interaction):
            await interaction.response.send_message("This command is restricted to the admin channel.", ephemeral=True)
            return
        def _audit(tid):
            with get_conn() as conn:
                cur = conn.execute("SELECT tx_id, discord_id, game_name, nonce, raw_payload, verified_at FROM verifications WHERE tx_id = ?", (tid,))
                return cur.fetchone()
        row = await asyncio.to_thread(_audit, tx_id)
        if not row:
            await interaction.response.send_message(f"No verification record found for `{tx_id}`.")
        else:
            await interaction.response.send_message(
                f"**Verification record**\n"
                f"tx_id: `{row[0]}`\n"
                f"discord_id: `{row[1]}`\n"
                f"game_name: **{row[2]}**\n"
                f"nonce: `{row[3]}`\n"
                f"verified_at: `{row[5]}`"
            )

    @bot.tree.command(name="export_withdrawals", description="[Admin] Export pending withdrawals to CSV")
    async def export_withdrawals(interaction: discord.Interaction):
        if not _check_admin(interaction):
            await interaction.response.send_message("This command is restricted to the admin channel.", ephemeral=True)
            return
        path = await asyncio.to_thread(export_withdrawals_csv)
        await interaction.response.send_message(f"Withdrawals exported to `{path}`.")

    @bot.tree.command(name="admin_credit", description="[Admin] Add gold to a user's balance")
    @app_commands.describe(user="The Discord user to credit", amount="Amount of gold to add", reason="Reason for the credit")
    async def admin_credit(interaction: discord.Interaction, user: discord.Member, amount: int, reason: str = "Admin credit"):
        if not _check_admin(interaction):
            await interaction.response.send_message("This command is restricted to the admin channel.", ephemeral=True)
            return
        if amount <= 0:
            await interaction.response.send_message("Amount must be a positive integer.", ephemeral=True)
            return
        def _credit(did, amt, rsn):
            with get_conn() as conn:
                cur = conn.execute("SELECT game_name FROM users WHERE discord_id = ?", (did,))
                row = cur.fetchone()
                if not row:
                    return None
                conn.execute("UPDATE users SET internal_gold = internal_gold + ? WHERE discord_id = ?", (amt, did))
                conn.execute("INSERT INTO audit_logs(actor, action, details) VALUES (?, 'admin_credit', ?)",
                             (str(interaction.user.id), json.dumps({"discord_id": did, "amount": amt, "reason": rsn})))
                conn.commit()
                return row[0]
        account_name = await asyncio.to_thread(_credit, str(user.id), amount, reason)
        if account_name is None:
            await interaction.response.send_message(
                f"{user.mention} has no registered account. They must use `/link` first.", ephemeral=True
            )
            return
        await interaction.response.send_message(
            f"Credited **{amount:,} gold** to **{account_name}** ({user.mention}).\nReason: {reason}"
        )
        try:
            await user.send(
                f"**Gold credited**\n\n"
                f"An admin has added **{amount:,} gold** to your market balance.\n"
                f"Reason: {reason}"
            )
        except Exception:
            pass

    @bot.tree.command(name="admin_debit", description="[Admin] Remove gold from a user's balance")
    @app_commands.describe(user="The Discord user to debit", amount="Amount of gold to remove", reason="Reason for the debit")
    async def admin_debit(interaction: discord.Interaction, user: discord.Member, amount: int, reason: str = "Admin debit"):
        if not _check_admin(interaction):
            await interaction.response.send_message("This command is restricted to the admin channel.", ephemeral=True)
            return
        if amount <= 0:
            await interaction.response.send_message("Amount must be a positive integer.", ephemeral=True)
            return
        def _debit(did, amt, rsn):
            with get_conn() as conn:
                cur = conn.execute("SELECT game_name, internal_gold FROM users WHERE discord_id = ?", (did,))
                row = cur.fetchone()
                if not row:
                    return None, 0
                if row[1] < amt:
                    return row[0], row[1]
                conn.execute("UPDATE users SET internal_gold = internal_gold - ? WHERE discord_id = ?", (amt, did))
                conn.execute("INSERT INTO audit_logs(actor, action, details) VALUES (?, 'admin_debit', ?)",
                             (str(interaction.user.id), json.dumps({"discord_id": did, "amount": amt, "reason": rsn})))
                conn.commit()
                return row[0], -1
        account_name, balance = await asyncio.to_thread(_debit, str(user.id), amount, reason)
        if account_name is None:
            await interaction.response.send_message(
                f"{user.mention} has no registered account.", ephemeral=True
            )
        elif balance >= 0:
            await interaction.response.send_message(
                f"Insufficient balance. **{account_name}** only has **{balance:,} gold**.", ephemeral=True
            )
        else:
            await interaction.response.send_message(
                f"Debited **{amount:,} gold** from **{account_name}** ({user.mention}).\nReason: {reason}"
            )
            try:
                await user.send(
                    f"**Gold removed**\n\n"
                    f"An admin has removed **{amount:,} gold** from your market balance.\n"
                    f"Reason: {reason}"
                )
            except Exception:
                pass

    # ---- Ticker management ----

    @bot.tree.command(name="admin_add_ticker", description="[Admin] Add a new ticker to the market")
    @app_commands.describe(
        ticker="Ticker symbol (e.g. NORD)",
        gold_pool="Starting gold pool (sets initial liquidity)",
        share_pool="Starting share pool — defaults to gold_pool (price = 1.0)"
    )
    async def admin_add_ticker(interaction: discord.Interaction, ticker: str, gold_pool: int, share_pool: int = 0):
        if not _check_admin(interaction):
            await interaction.response.send_message("This command is restricted to the admin channel.", ephemeral=True)
            return
        sym = ticker.upper().strip()
        if not sym.isalpha() or len(sym) > 8:
            await interaction.response.send_message("Ticker must be letters only, max 8 characters.", ephemeral=True)
            return
        if gold_pool <= 0:
            await interaction.response.send_message("Gold pool must be a positive integer.", ephemeral=True)
            return
        sp = share_pool if share_pool > 0 else gold_pool
        def _add(sym, gp, sp):
            with get_conn() as conn:
                cur = conn.execute("SELECT ticker FROM tickers WHERE ticker = ?", (sym,))
                if cur.fetchone():
                    return False
                conn.execute(
                    "INSERT INTO tickers(ticker, gold_pool, share_pool, day_start_price, is_frozen) VALUES (?, ?, ?, ?, 0)",
                    (sym, gp, sp, gp / sp)
                )
                conn.commit()
                return True
        ok = await asyncio.to_thread(_add, sym, gold_pool, sp)
        if not ok:
            await interaction.response.send_message(f"Ticker `{sym}` already exists.", ephemeral=True)
        else:
            price = gold_pool / sp
            await interaction.response.send_message(
                f"Ticker `{sym}` added — gold pool: **{gold_pool:,}**, share pool: **{sp:,}**, starting price: **{price:.4f}**."
            )

    @bot.tree.command(name="admin_set_price", description="[Admin] Set a ticker's price by adjusting its gold pool")
    @app_commands.describe(ticker="Ticker to reprice", price="Target price in gold per share")
    @app_commands.autocomplete(ticker=ticker_autocomplete)
    async def admin_set_price(interaction: discord.Interaction, ticker: str, price: float):
        if not _check_admin(interaction):
            await interaction.response.send_message("This command is restricted to the admin channel.", ephemeral=True)
            return
        if price <= 0:
            await interaction.response.send_message("Price must be greater than zero.", ephemeral=True)
            return
        def _set_price(sym, p):
            with get_conn() as conn:
                cur = conn.execute("SELECT gold_pool, share_pool FROM tickers WHERE ticker = ?", (sym,))
                row = cur.fetchone()
                if not row:
                    return None, None
                old_gp, sp = row
                new_gp = int(p * sp)
                if new_gp <= 0:
                    new_gp = 1
                conn.execute(
                    "UPDATE tickers SET gold_pool = ?, day_start_price = ?, last_updated = CURRENT_TIMESTAMP WHERE ticker = ?",
                    (new_gp, p, sym)
                )
                conn.execute("INSERT INTO audit_logs(actor, action, details) VALUES (?, 'admin_set_price', ?)",
                             (str(interaction.user.id), json.dumps({"ticker": sym, "old_gold_pool": old_gp, "new_gold_pool": new_gp, "price": p})))
                conn.commit()
                return old_gp / sp, p
        sym = ticker.upper()
        old_price, new_price = await asyncio.to_thread(_set_price, sym, price)
        if old_price is None:
            await interaction.response.send_message(f"Ticker `{sym}` not found.", ephemeral=True)
        else:
            await interaction.response.send_message(
                f"`{sym}` price set: **{old_price:.4f}** → **{new_price:.4f}** gold/share."
            )

    @bot.tree.command(name="admin_set_pool", description="[Admin] Directly set a ticker's AMM pool sizes")
    @app_commands.describe(ticker="Ticker to modify", gold_pool="New gold pool", share_pool="New share pool")
    @app_commands.autocomplete(ticker=ticker_autocomplete)
    async def admin_set_pool(interaction: discord.Interaction, ticker: str, gold_pool: int, share_pool: int):
        if not _check_admin(interaction):
            await interaction.response.send_message("This command is restricted to the admin channel.", ephemeral=True)
            return
        if gold_pool <= 0 or share_pool <= 0:
            await interaction.response.send_message("Pool sizes must be positive integers.", ephemeral=True)
            return
        def _set_pool(sym, gp, sp):
            with get_conn() as conn:
                cur = conn.execute("SELECT gold_pool, share_pool FROM tickers WHERE ticker = ?", (sym,))
                row = cur.fetchone()
                if not row:
                    return None
                old_price = row[0] / row[1] if row[1] else 0
                conn.execute(
                    "UPDATE tickers SET gold_pool = ?, share_pool = ?, day_start_price = ?, last_updated = CURRENT_TIMESTAMP WHERE ticker = ?",
                    (gp, sp, gp / sp, sym)
                )
                conn.execute("INSERT INTO audit_logs(actor, action, details) VALUES (?, 'admin_set_pool', ?)",
                             (str(interaction.user.id), json.dumps({"ticker": sym, "gold_pool": gp, "share_pool": sp})))
                conn.commit()
                return old_price
        sym = ticker.upper()
        old_price = await asyncio.to_thread(_set_pool, sym, gold_pool, share_pool)
        if old_price is None:
            await interaction.response.send_message(f"Ticker `{sym}` not found.", ephemeral=True)
        else:
            new_price = gold_pool / share_pool
            await interaction.response.send_message(
                f"`{sym}` pools updated — gold: **{gold_pool:,}**, shares: **{share_pool:,}**, "
                f"price: **{old_price:.4f}** → **{new_price:.4f}**."
            )

    @bot.tree.command(name="admin_reset_day", description="[Admin] Reset a ticker's day-start price to current price")
    @app_commands.describe(ticker="Ticker to reset (use 'ALL' to reset every ticker)")
    @app_commands.autocomplete(ticker=ticker_autocomplete)
    async def admin_reset_day(interaction: discord.Interaction, ticker: str):
        if not _check_admin(interaction):
            await interaction.response.send_message("This command is restricted to the admin channel.", ephemeral=True)
            return
        def _reset(sym):
            with get_conn() as conn:
                if sym == "ALL":
                    conn.execute("UPDATE tickers SET day_start_price = CAST(gold_pool AS REAL) / share_pool")
                    count = conn.execute("SELECT COUNT(*) FROM tickers").fetchone()[0]
                    conn.commit()
                    return count
                cur = conn.execute("SELECT gold_pool, share_pool FROM tickers WHERE ticker = ?", (sym,))
                row = cur.fetchone()
                if not row:
                    return None
                conn.execute("UPDATE tickers SET day_start_price = ? WHERE ticker = ?", (row[0] / row[1], sym))
                conn.commit()
                return 1
        sym = ticker.upper()
        result = await asyncio.to_thread(_reset, sym)
        if result is None:
            await interaction.response.send_message(f"Ticker `{sym}` not found.", ephemeral=True)
        elif sym == "ALL":
            await interaction.response.send_message(f"Day-start price reset for all **{result}** tickers.")
        else:
            await interaction.response.send_message(f"Day-start price for `{sym}` reset to current price. Change % will now show 0%.")

    @bot.tree.command(name="admin_remove_ticker", description="[Admin] Remove a ticker from the market entirely")
    @app_commands.describe(ticker="Ticker to remove", confirm="Type the ticker symbol again to confirm")
    @app_commands.autocomplete(ticker=ticker_autocomplete)
    async def admin_remove_ticker(interaction: discord.Interaction, ticker: str, confirm: str):
        if not _check_admin(interaction):
            await interaction.response.send_message("This command is restricted to the admin channel.", ephemeral=True)
            return
        sym = ticker.upper()
        if confirm.upper() != sym:
            await interaction.response.send_message(
                f"Confirmation mismatch. You must type `{sym}` exactly in the confirm field.", ephemeral=True
            )
            return
        def _remove(sym):
            with get_conn() as conn:
                cur = conn.execute("SELECT ticker FROM tickers WHERE ticker = ?", (sym,))
                if not cur.fetchone():
                    return False
                conn.execute("DELETE FROM tickers WHERE ticker = ?", (sym,))
                conn.execute("DELETE FROM portfolios WHERE ticker = ?", (sym,))
                conn.execute("DELETE FROM limit_orders WHERE ticker = ?", (sym,))
                conn.execute("INSERT INTO audit_logs(actor, action, details) VALUES (?, 'admin_remove_ticker', ?)",
                             (str(interaction.user.id), json.dumps({"ticker": sym})))
                conn.commit()
                return True
        ok = await asyncio.to_thread(_remove, sym)
        if not ok:
            await interaction.response.send_message(f"Ticker `{sym}` not found.", ephemeral=True)
        else:
            await interaction.response.send_message(
                f"Ticker `{sym}` removed. All portfolios and open orders for this ticker have been cleared."
            )

    # ---- User management ----

    @bot.tree.command(name="admin_view_user", description="[Admin] View full account info for a user")
    @app_commands.describe(user="The Discord user to inspect")
    async def admin_view_user(interaction: discord.Interaction, user: discord.Member):
        if not _check_admin(interaction):
            await interaction.response.send_message("This command is restricted to the admin channel.", ephemeral=True)
            return
        def _fetch(did):
            with get_conn() as conn:
                cur = conn.execute(
                    "SELECT game_name, internal_gold, verified FROM users WHERE discord_id = ?", (did,)
                )
                urow = cur.fetchone()
                if not urow:
                    return None, [], []
                prows = conn.execute(
                    "SELECT p.ticker, p.shares, t.gold_pool, t.share_pool FROM portfolios p "
                    "JOIN tickers t ON p.ticker = t.ticker WHERE p.discord_id = ? AND p.shares > 0", (did,)
                ).fetchall()
                wrows = conn.execute(
                    "SELECT withdrawal_id, amount, status, requested_at FROM withdrawals WHERE discord_id = ? ORDER BY requested_at DESC LIMIT 5", (did,)
                ).fetchall()
                return urow, prows, wrows
        urow, prows, wrows = await asyncio.to_thread(_fetch, str(user.id))
        if not urow:
            await interaction.response.send_message(f"{user.mention} has no registered account.", ephemeral=True)
            return
        name, gold, verified = urow
        lines = [
            f"**Account: {name}** ({user.mention})",
            f"Verified: {'Yes' if verified else 'No'}",
            f"Gold balance: **{gold:,}**",
            "",
            "**Portfolio**"
        ]
        if prows:
            for ticker, shares, gp, sp in prows:
                price = gp / sp if sp else 0
                lines.append(f"  {ticker}: {shares:,} shares @ {price:.4f} = **{int(shares * price):,} gold**")
        else:
            lines.append("  (none)")
        lines.append("")
        lines.append("**Recent withdrawals**")
        if wrows:
            for wid, amt, status, req_at in wrows:
                lines.append(f"  #{wid} — {amt:,} gold — {status} — {req_at[:10]}")
        else:
            lines.append("  (none)")
        await interaction.response.send_message("\n".join(lines), ephemeral=True)

    @bot.tree.command(name="admin_set_gold", description="[Admin] Set a user's gold balance to an exact amount")
    @app_commands.describe(user="The Discord user", amount="New exact gold balance", reason="Reason for adjustment")
    async def admin_set_gold(interaction: discord.Interaction, user: discord.Member, amount: int, reason: str = "Admin adjustment"):
        if not _check_admin(interaction):
            await interaction.response.send_message("This command is restricted to the admin channel.", ephemeral=True)
            return
        if amount < 0:
            await interaction.response.send_message("Amount cannot be negative.", ephemeral=True)
            return
        def _set(did, amt, rsn):
            with get_conn() as conn:
                cur = conn.execute("SELECT game_name, internal_gold FROM users WHERE discord_id = ?", (did,))
                row = cur.fetchone()
                if not row:
                    return None, None
                conn.execute("UPDATE users SET internal_gold = ? WHERE discord_id = ?", (amt, did))
                conn.execute("INSERT INTO audit_logs(actor, action, details) VALUES (?, 'admin_set_gold', ?)",
                             (str(interaction.user.id), json.dumps({"discord_id": did, "old": row[1], "new": amt, "reason": rsn})))
                conn.commit()
                return row[0], row[1]
        name, old = await asyncio.to_thread(_set, str(user.id), amount, reason)
        if name is None:
            await interaction.response.send_message(f"{user.mention} has no registered account.", ephemeral=True)
        else:
            await interaction.response.send_message(
                f"**{name}** ({user.mention}) gold: **{old:,}** → **{amount:,}**.\nReason: {reason}"
            )

    @bot.tree.command(name="admin_unlink", description="[Admin] Remove a user's verification so they can re-link")
    @app_commands.describe(user="The Discord user to unlink")
    async def admin_unlink(interaction: discord.Interaction, user: discord.Member):
        if not _check_admin(interaction):
            await interaction.response.send_message("This command is restricted to the admin channel.", ephemeral=True)
            return
        def _unlink(did):
            with get_conn() as conn:
                cur = conn.execute("SELECT game_name FROM users WHERE discord_id = ?", (did,))
                row = cur.fetchone()
                if not row:
                    return None
                conn.execute("UPDATE users SET verified = 0 WHERE discord_id = ?", (did,))
                conn.execute("DELETE FROM audit_logs WHERE action = 'pending_nonce' AND details LIKE ?", (f'%"discord_id": "{did}"%',))
                conn.execute("INSERT INTO audit_logs(actor, action, details) VALUES (?, 'admin_unlink', ?)",
                             (str(interaction.user.id), json.dumps({"discord_id": did, "game_name": row[0]})))
                conn.commit()
                return row[0]
        name = await asyncio.to_thread(_unlink, str(user.id))
        if name is None:
            await interaction.response.send_message(f"{user.mention} has no registered account.", ephemeral=True)
        else:
            await interaction.response.send_message(
                f"**{name}** ({user.mention}) has been unlinked. They can run `/link` again to re-verify."
            )

    # ---- Withdrawal management ----

    @bot.tree.command(name="admin_list_withdrawals", description="[Admin] List all pending withdrawals")
    async def admin_list_withdrawals(interaction: discord.Interaction):
        if not _check_admin(interaction):
            await interaction.response.send_message("This command is restricted to the admin channel.", ephemeral=True)
            return
        def _list():
            with get_conn() as conn:
                rows = conn.execute(
                    "SELECT w.withdrawal_id, u.game_name, w.amount, w.requested_at "
                    "FROM withdrawals w JOIN users u ON w.discord_id = u.discord_id "
                    "WHERE w.status = 'PENDING' ORDER BY w.requested_at ASC"
                ).fetchall()
                return rows
        rows = await asyncio.to_thread(_list)
        if not rows:
            await interaction.response.send_message("No pending withdrawals.", ephemeral=True)
            return
        total = sum(r[2] for r in rows)
        lines = [f"**Pending withdrawals ({len(rows)}) — {total:,} gold total**", ""]
        for wid, name, amt, req_at in rows:
            lines.append(f"  `#{wid}` — **{name}** — {amt:,} gold — {req_at[:16]}")
        await interaction.response.send_message("\n".join(lines), ephemeral=True)

    @bot.tree.command(name="admin_complete_withdrawal", description="[Admin] Mark a withdrawal as completed")
    @app_commands.describe(withdrawal_id="Withdrawal ID number")
    async def admin_complete_withdrawal(interaction: discord.Interaction, withdrawal_id: int):
        if not _check_admin(interaction):
            await interaction.response.send_message("This command is restricted to the admin channel.", ephemeral=True)
            return
        def _complete(wid):
            with get_conn() as conn:
                cur = conn.execute(
                    "SELECT w.amount, u.game_name FROM withdrawals w JOIN users u ON w.discord_id = u.discord_id "
                    "WHERE w.withdrawal_id = ? AND w.status = 'PENDING'", (wid,)
                )
                row = cur.fetchone()
                if not row:
                    return None
                conn.execute("UPDATE withdrawals SET status = 'COMPLETED' WHERE withdrawal_id = ?", (wid,))
                conn.execute("INSERT INTO audit_logs(actor, action, details) VALUES (?, 'admin_complete_withdrawal', ?)",
                             (str(interaction.user.id), json.dumps({"withdrawal_id": wid})))
                conn.commit()
                return row
        row = await asyncio.to_thread(_complete, withdrawal_id)
        if not row:
            await interaction.response.send_message(
                f"Withdrawal `#{withdrawal_id}` not found or already completed.", ephemeral=True
            )
        else:
            amt, name = row
            await interaction.response.send_message(
                f"Withdrawal `#{withdrawal_id}` marked as **COMPLETED** — **{amt:,} gold** to **{name}**."
            )

    @bot.tree.command(name="admin_cancel_withdrawal", description="[Admin] Cancel a pending withdrawal and refund the gold")
    @app_commands.describe(withdrawal_id="Withdrawal ID number")
    async def admin_cancel_withdrawal(interaction: discord.Interaction, withdrawal_id: int):
        if not _check_admin(interaction):
            await interaction.response.send_message("This command is restricted to the admin channel.", ephemeral=True)
            return
        def _cancel(wid):
            with get_conn() as conn:
                cur = conn.execute(
                    "SELECT w.discord_id, w.amount, u.game_name FROM withdrawals w JOIN users u ON w.discord_id = u.discord_id "
                    "WHERE w.withdrawal_id = ? AND w.status = 'PENDING'", (wid,)
                )
                row = cur.fetchone()
                if not row:
                    return None
                did, amt, name = row
                conn.execute("UPDATE withdrawals SET status = 'CANCELLED' WHERE withdrawal_id = ?", (wid,))
                conn.execute("UPDATE users SET internal_gold = internal_gold + ? WHERE discord_id = ?", (amt, did))
                conn.execute("INSERT INTO audit_logs(actor, action, details) VALUES (?, 'admin_cancel_withdrawal', ?)",
                             (str(interaction.user.id), json.dumps({"withdrawal_id": wid, "refunded": amt})))
                conn.commit()
                return amt, name
        result = await asyncio.to_thread(_cancel, withdrawal_id)
        if not result:
            await interaction.response.send_message(
                f"Withdrawal `#{withdrawal_id}` not found or not in PENDING state.", ephemeral=True
            )
        else:
            amt, name = result
            await interaction.response.send_message(
                f"Withdrawal `#{withdrawal_id}` cancelled. **{amt:,} gold** refunded to **{name}**."
            )

    # ---- Limit order management ----

    @bot.tree.command(name="admin_list_orders", description="[Admin] List open limit orders, optionally filtered by ticker")
    @app_commands.describe(ticker="Filter by ticker (leave blank for all)")
    @app_commands.autocomplete(ticker=ticker_autocomplete)
    async def admin_list_orders(interaction: discord.Interaction, ticker: str = ""):
        if not _check_admin(interaction):
            await interaction.response.send_message("This command is restricted to the admin channel.", ephemeral=True)
            return
        def _list(sym):
            with get_conn() as conn:
                if sym:
                    rows = conn.execute(
                        "SELECT o.order_id, u.game_name, o.ticker, o.side, o.price, o.shares, o.created_at "
                        "FROM limit_orders o JOIN users u ON o.discord_id = u.discord_id "
                        "WHERE o.ticker = ? ORDER BY o.created_at ASC", (sym,)
                    ).fetchall()
                else:
                    rows = conn.execute(
                        "SELECT o.order_id, u.game_name, o.ticker, o.side, o.price, o.shares, o.created_at "
                        "FROM limit_orders o JOIN users u ON o.discord_id = u.discord_id "
                        "ORDER BY o.created_at ASC LIMIT 40"
                    ).fetchall()
                return rows
        sym = ticker.upper() if ticker else ""
        rows = await asyncio.to_thread(_list, sym)
        if not rows:
            label = f" for `{sym}`" if sym else ""
            await interaction.response.send_message(f"No open limit orders{label}.", ephemeral=True)
            return
        lines = [f"**Open limit orders ({len(rows)})**", ""]
        for oid, name, tick, side, price, shares, created_at in rows:
            lines.append(f"  `#{oid}` — **{name}** — {side} {shares:,} {tick} @ {price:.4f} — {created_at[:16]}")
        await interaction.response.send_message("\n".join(lines), ephemeral=True)

    @bot.tree.command(name="admin_cancel_order", description="[Admin] Cancel an open limit order by ID")
    @app_commands.describe(order_id="Order ID number")
    async def admin_cancel_order(interaction: discord.Interaction, order_id: int):
        if not _check_admin(interaction):
            await interaction.response.send_message("This command is restricted to the admin channel.", ephemeral=True)
            return
        def _cancel(oid):
            with get_conn() as conn:
                cur = conn.execute(
                    "SELECT o.discord_id, o.ticker, o.side, o.price, o.shares, u.game_name "
                    "FROM limit_orders o JOIN users u ON o.discord_id = u.discord_id WHERE o.order_id = ?", (oid,)
                )
                row = cur.fetchone()
                if not row:
                    return None
                did, ticker, side, price, shares, name = row
                conn.execute("DELETE FROM limit_orders WHERE order_id = ?", (oid,))
                # Refund reserved gold for BUY orders
                if side == "BUY":
                    refund = int(price * shares)
                    conn.execute("UPDATE users SET internal_gold = internal_gold + ? WHERE discord_id = ?", (refund, did))
                conn.execute("INSERT INTO audit_logs(actor, action, details) VALUES (?, 'admin_cancel_order', ?)",
                             (str(interaction.user.id), json.dumps({"order_id": oid, "ticker": ticker, "side": side})))
                conn.commit()
                return name, ticker, side, price, shares
        result = await asyncio.to_thread(_cancel, order_id)
        if not result:
            await interaction.response.send_message(f"Order `#{order_id}` not found.", ephemeral=True)
        else:
            name, tick, side, price, shares = result
            refund_note = f" Gold reserved for this order has been refunded." if side == "BUY" else ""
            await interaction.response.send_message(
                f"Order `#{order_id}` cancelled — **{name}**'s {side} of {shares:,} `{tick}` @ {price:.4f}.{refund_note}"
            )

    # ---- Market embed helpers ----

    def _get_market_data():
        with get_conn() as conn:
            cur = conn.execute(
                "SELECT ticker, gold_pool, share_pool, day_start_price, is_frozen FROM tickers ORDER BY ticker"
            )
            return cur.fetchall()

    def _get_market_message_id():
        with get_conn() as conn:
            cur = conn.execute("SELECT value FROM ledger_state WHERE key = 'market_message_id'")
            row = cur.fetchone()
            return int(row[0]) if row and row[0] else None

    def _set_market_message_id(msg_id):
        with get_conn() as conn:
            conn.execute(
                "INSERT OR REPLACE INTO ledger_state(key, value) VALUES ('market_message_id', ?)",
                (str(msg_id),)
            )
            conn.commit()

    def _build_market_embed(rows):
        now = datetime.now(datetime.timezone.utc).strftime("%H:%M:%S UTC")
        one_hour_ago = (datetime.now(datetime.timezone.utc) - timedelta(hours=1)).isoformat()
        embed = discord.Embed(
            title="Territorial Market",
            description=f"Live prices updated every {MARKET_UPDATE_INTERVAL}s — change vs 1h ago",
            color=0x2b2d31
        )
        with get_conn() as conn:
            for ticker, gold_pool, share_pool, day_start_price, is_frozen in rows:
                price = gold_pool / share_pool if share_pool else 0
                hist_row = conn.execute(
                    "SELECT price FROM price_history WHERE ticker = ? AND recorded_at <= ? ORDER BY recorded_at DESC LIMIT 1",
                    (ticker, one_hour_ago)
                ).fetchone()
                reference_price = hist_row[0] if hist_row and hist_row[0] else None
                if reference_price is None or reference_price == 0:
                    reference_price = day_start_price if day_start_price else price
                change = ((price - reference_price) / reference_price * 100) if reference_price else 0
                bar_length = 8
                filled = max(0, min(bar_length, round((change + 15) / 30 * bar_length)))
                bar = "█" * filled + "░" * (bar_length - filled)
                direction = "+" if change >= 0 else ""
                status = "FROZEN" if is_frozen else f"{direction}{change:.2f}%"
                embed.add_field(
                    name=f"`{ticker}`",
                    value=f"**{price:.4f}** gold/share\n`{bar}` {status}",
                    inline=True
                )
        embed.set_footer(text=f"Last updated {now}")
        return embed

    # ---- Bot events ----
    @bot.tree.command(name="achievements", description="View your achievements")
    async def cmd_achievements(interaction: discord.Interaction):
        """Show user's achievements."""
        discord_id = str(interaction.user.id)

        with get_conn() as conn:
            cur = conn.execute(
                "SELECT achievement_name, earned_at FROM achievements WHERE discord_id = ? ORDER BY earned_at DESC",
                (discord_id,)
            )
            achievements = cur.fetchall()

        if not achievements:
            await interaction.response.send_message("You haven't earned any achievements yet. Keep trading!", ephemeral=True)
            return

        embed = discord.Embed(title="🏆 Your Achievements", color=discord.Color.gold())
        for ach_name, earned_at in achievements:
            ach_def = Achievements.ACHIEVEMENT_DEFINITIONS.get(ach_name, {})
            embed.add_field(
                name=ach_def.get("name", ach_name),
                value=f"Earned: {earned_at[:10]}",
                inline=False
            )

        await interaction.response.send_message(embed=embed, ephemeral=True)

    @bot.tree.command(name="dividends", description="View dividend schedule")
    async def cmd_dividends(interaction: discord.Interaction):
        """Show dividend yields and next payout."""
        with get_conn() as conn:
            cur = conn.execute("SELECT ticker, dividend_yield FROM dividend_config ORDER BY ticker")
            divs = cur.fetchall()

        embed = discord.Embed(title="💰 Dividend Schedule", color=discord.Color.green())
        embed.description = "Quarterly dividend payouts (every 90 days)"

        for ticker, yield_pct in divs:
            embed.add_field(name=ticker, value=f"{yield_pct*100:.1f}% yield", inline=True)

        await interaction.response.send_message(embed=embed, ephemeral=True)

    @bot.event
    async def on_ready():
            try:
                if GUILD_ID:
                    guild_obj = discord.Object(id=GUILD_ID)
                    bot.tree.copy_global_to(guild=guild_obj)
                    synced = await bot.tree.sync(guild=guild_obj)
                    logger.info(f"Synced {len(synced)} slash command(s) to guild {GUILD_ID} (instant)")
                else:
                    synced = await bot.tree.sync()
                    logger.info(f"Synced {len(synced)} slash command(s) globally (may take up to 1h to appear)")
            except Exception:
                logger.exception("Failed to sync slash commands")
            logger.info(f"Bot ready as {bot.user}")
            logger.info("MARKET_CHANNEL_ID=%s, DB_PATH=%s", MARKET_CHANNEL_ID, DB_PATH)
            # Initialize dividend config and start background tasks
            try:
                Dividends.init_dividend_config()
            except Exception:
                logger.exception("Failed to init dividend config")

            global background_tasks_started
            if not background_tasks_started:
                bot.loop.create_task(background_ledger_poller())
                bot.loop.create_task(background_price_drift())
                bot.loop.create_task(background_day_reset())
                bot.loop.create_task(background_market_events())
                bot.loop.create_task(background_dividend_payouts())
                if MARKET_CHANNEL_ID:
                    bot.loop.create_task(background_market_embed())
                background_tasks_started = True
            else:
                logger.info("Background tasks already started, skipping duplicate startup")

    async def background_day_reset():
        """Reset day_start_price to current price at UTC midnight each day."""
        await bot.wait_until_ready()
        while True:
            now = datetime.now(datetime.timezone.utc)
            next_midnight = (now + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
            await asyncio.sleep((next_midnight - now).total_seconds())
            try:
                def _reset():
                    with get_conn() as conn:
                        conn.execute(
                            "UPDATE tickers SET day_start_price = CAST(gold_pool AS REAL) / share_pool"
                        )
                        conn.commit()
                await asyncio.to_thread(_reset)
                logger.info("Daily day_start_price reset at UTC midnight")
            except Exception:
                logger.exception("Daily reset failed")


async def background_market_events():
    """Periodically generate market events every 6-12 hours."""
    await bot.wait_until_ready()
    while True:
        # sleep random 6-12 hours
        delay_hours = random.uniform(6, 12)
        await asyncio.sleep(delay_hours * 3600)
        try:
            event = await asyncio.to_thread(MarketEvents.generate_event)
            # Post to market channel if configured
            if MARKET_CHANNEL_ID and event:
                try:
                    ch = bot.get_channel(MARKET_CHANNEL_ID)
                    if ch:
                        await ch.send(f"Market Event: **{event['name']}** — {event['desc']}")
                except Exception:
                    logger.exception("Failed to post market event to channel")
        except Exception:
            logger.exception("Market event generation failed")


async def background_dividend_payouts():
    """Pay dividends every 90 days (quarterly)."""
    await bot.wait_until_ready()
    while True:
        # Sleep for 90 days
        await asyncio.sleep(90 * 24 * 3600)
        try:
            total = await asyncio.to_thread(Dividends.payout_dividends)
            if WITHDRAWAL_LOG_CHANNEL_ID and total > 0:
                try:
                    ch = bot.get_channel(WITHDRAWAL_LOG_CHANNEL_ID)
                    if ch:
                        await ch.send(f"Quarterly dividends paid: {total} gold distributed to shareholders.")
                except Exception:
                    logger.exception("Failed to post dividend summary to channel")
        except Exception:
            logger.exception("Dividend payout failed")

async def background_price_drift():
    """Randomly walk every non-frozen ticker's price every PRICE_DRIFT_INTERVAL seconds."""
    await bot.wait_until_ready()
    logger.info("Starting price drift task with interval %s seconds", PRICE_DRIFT_INTERVAL)
    await asyncio.sleep(PRICE_DRIFT_INTERVAL)  # wait one full cycle before first drift
    while True:
        try:
            def _drift():
                with get_conn() as conn:
                    rows = conn.execute(
                        "SELECT ticker, gold_pool, share_pool, is_frozen FROM tickers"
                    ).fetchall()
                    changes = []
                    now_iso = datetime.now(datetime.timezone.utc).isoformat()
                    cutoff  = (datetime.now(datetime.timezone.utc) - timedelta(days=PRICE_HISTORY_DAYS)).isoformat()
                    for ticker, gp, sp, is_frozen in rows:
                        if is_frozen:
                            continue
                        # ── momentum: look at last 2 recorded prices ──────────
                        hist = conn.execute(
                            "SELECT price FROM price_history WHERE ticker = ? "
                            "ORDER BY recorded_at DESC LIMIT 2", (ticker,)
                        ).fetchall()
                        momentum = 0.0
                        if len(hist) == 2:
                            prev2, prev1 = hist[1][0], hist[0][0]
                            if prev2 > 0:
                                momentum = (prev1 - prev2) / prev2
                        # ── random drift + dampened momentum bias ─────────────
                        vol   = TICKER_VOLATILITY.get(ticker, 0.015)
                        noise = random.gauss(0, vol)
                        drift = noise + MOMENTUM_FACTOR * momentum
                        drift = max(-0.12, min(0.12, drift))
                        new_gp = max(1, int(gp * (1 + drift)))
                        conn.execute(
                            "UPDATE tickers SET gold_pool = ?, last_updated = CURRENT_TIMESTAMP WHERE ticker = ?",
                            (new_gp, ticker)
                        )
                        old_price = gp / sp if sp else 0
                        new_price = new_gp / sp if sp else 0
                        # ── record snapshot ───────────────────────────────────
                        conn.execute(
                            "INSERT INTO price_history(ticker, price, recorded_at) VALUES (?, ?, ?)",
                            (ticker, new_price, now_iso)
                        )
                        changes.append((ticker, old_price, new_price, drift))
                    # ── prune old history ─────────────────────────────────────
                    conn.execute(
                        "DELETE FROM price_history WHERE recorded_at < ?", (cutoff,)
                    )
                    conn.commit()
                    return changes
            changes = await asyncio.to_thread(_drift)
            if changes:
                logger.info(
                    "Price drift: %s",
                    "  ".join(f"{t} {op:.4f}→{np:.4f} ({d*100:+.1f}%)" for t, op, np, d in changes)
                )
        except Exception:
            logger.exception("Price drift task failed")
        await asyncio.sleep(PRICE_DRIFT_INTERVAL)


async def background_ledger_poller():
    await bot.wait_until_ready()
    backoff = 1
    while True:
        try:
            processed, newly_verified = await ingest_ledger_cycle()
            if processed > 0:
                # DM each newly verified user
                for discord_id, account_name in newly_verified:
                    try:
                        user = await bot.fetch_user(int(discord_id))
                        if user:
                            await user.send(
                                f"**Verification complete**\n\n"
                                f"Your account **{account_name}** has been verified. "
                                f"You can now deposit gold and start trading on the Territorial Market."
                            )
                    except Exception:
                        logger.warning("Could not DM verified user %s", discord_id)
                # Notify admin channel
                try:
                    if ADMIN_CHANNEL_ID:
                        ch = bot.get_channel(ADMIN_CHANNEL_ID)
                        if ch:
                            names = ", ".join(name for _, name in newly_verified)
                            await ch.send(f"Ledger: verified {processed} account(s) — {names}.")
                except Exception:
                    logger.exception("Failed to notify admin channel")
                # Log deposits / verifications to activity log
                try:
                    act_ch = bot.get_channel(ACTIVITY_LOG_CHANNEL_ID)
                    if act_ch:
                        for did, name in newly_verified:
                            def _gold(d=did):
                                with get_conn() as c:
                                    r = c.execute("SELECT internal_gold FROM users WHERE discord_id=?", (d,)).fetchone()
                                    return r[0] if r else 0
                            bal = await asyncio.to_thread(_gold)
                            await act_ch.send(
                                f"DEPOSIT  **{name}** verified — balance now **{bal:,} gold**"
                            )
                except Exception:
                    logger.exception("Failed to post deposit activity log")
            backoff = 1
        except Exception:
            logger.exception("Ledger poller failed; backing off")
            await asyncio.sleep(min(300, backoff))
            backoff = min(300, backoff * 2)
        await asyncio.sleep(LEDGER_POLL_INTERVAL)


async def background_market_embed():
    await bot.wait_until_ready()
    channel = bot.get_channel(MARKET_CHANNEL_ID)
    if not channel:
        try:
            channel = await bot.fetch_channel(MARKET_CHANNEL_ID)
        except Exception as exc:
            logger.warning("MARKET_CHANNEL_ID set but channel not found. Check bot permissions or channel access: %s", exc)
    if not channel:
        logger.warning("MARKET_CHANNEL_ID set but channel not found. Check bot permissions.")
        return
    logger.info(f"Market embed task started in channel {MARKET_CHANNEL_ID}")
    market_message = None
    # Try to recover previously posted message
    stored_id = await asyncio.to_thread(_get_market_message_id)
    if stored_id:
        try:
            market_message = await channel.fetch_message(stored_id)
        except Exception:
            market_message = None
    while True:
        try:
            rows = await asyncio.to_thread(_get_market_data)
            embed = _build_market_embed(rows)
            if market_message:
                await market_message.edit(embed=embed)
            else:
                market_message = await channel.send(embed=embed)
                await asyncio.to_thread(_set_market_message_id, market_message.id)
        except Exception:
            logger.exception("Market embed update failed")
            market_message = None
        await asyncio.sleep(MARKET_UPDATE_INTERVAL)


# ---- If run as script, initialize DB and optionally start bot ----
async def _run_health_server():
    """Tiny HTTP server so UptimeRobot can ping the Repl to keep it awake."""
    if not AIOHTTP_AVAILABLE:
        logger.warning("aiohttp not available — health server disabled. Bot may sleep on free tier.")
        return
    async def handle(request):
        return aio_web.Response(text="OK — Territorial Market bot is running.")
    app = aio_web.Application()
    app.router.add_get("/", handle)
    app.router.add_get("/health", handle)
    runner = aio_web.AppRunner(app)
    await runner.setup()
    site = aio_web.TCPSite(runner, "0.0.0.0", HEALTH_PORT)
    await site.start()
    logger.info(f"Health server listening on port {HEALTH_PORT} — use this URL with UptimeRobot to stay awake")


async def _main():
    init_db()
    logger.info("Database initialized at %s", DB_PATH)
    if discord is None or not DISCORD_TOKEN:
        logger.info("Discord token not provided or discord.py missing.")
        return
    # Start health server concurrently with the bot
    await _run_health_server()
    async with bot:
        await bot.start(DISCORD_TOKEN)


if __name__ == "__main__":
    try:
        asyncio.run(_main())
    except KeyboardInterrupt:
        pass
    except Exception:
        logger.exception("Fatal error")


