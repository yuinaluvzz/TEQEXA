-- Schema for Territorial market bot (SQLite-compatible)
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

CREATE TABLE IF NOT EXISTS deposit_ledger (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  discord_id TEXT NOT NULL,
  game_name TEXT NOT NULL,
  gross_amount INTEGER NOT NULL,
  fee_amount INTEGER NOT NULL,
  credited_amount INTEGER NOT NULL,
  tx_id TEXT,
  timestamp REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS withdrawal_ledger (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  discord_id TEXT NOT NULL,
  game_name TEXT NOT NULL,
  requested_amount INTEGER NOT NULL,
  fee_amount INTEGER NOT NULL,
  sent_amount INTEGER NOT NULL,
  status TEXT DEFAULT 'PENDING',
  withdrawal_id INTEGER,
  timestamp REAL NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_achievements_discord ON achievements(discord_id);
CREATE INDEX IF NOT EXISTS idx_dividends_discord ON dividends_paid(discord_id);
CREATE INDEX IF NOT EXISTS idx_market_events_time ON market_events(event_time);
CREATE INDEX IF NOT EXISTS idx_deposit_ledger_time ON deposit_ledger(timestamp);
CREATE INDEX IF NOT EXISTS idx_withdrawal_ledger_time ON withdrawal_ledger(timestamp);

-- Seed tickers will be inserted by init_db() if missing.
