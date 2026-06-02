import os
from decimal import Decimal, getcontext
from dotenv import load_dotenv

load_dotenv()

getcontext().prec = 28

DISCORD_TOKEN = os.getenv("DISCORD_TOKEN", "")
DB_PATH = os.getenv("DB_PATH", "./data/market.db")
ADMIN_CHANNEL_ID = int(os.getenv("ADMIN_CHANNEL_ID", "0") or 0)
ADMIN_IDS = {s.strip() for s in os.getenv("ADMIN", "").split(",") if s.strip()}
MARKET_CHANNEL_ID = int(os.getenv("MARKET_CHANNEL_ID", "0") or 0)
GUILD_ID = int(os.getenv("GUILD_ID", "0") or 0)
LEDGER_URL = os.getenv("LEDGER_URL", "https://territorial.io/log/transactions")
LEDGER_POLL_INTERVAL = int(os.getenv("LEDGER_POLL_INTERVAL", "8"))
MARKET_UPDATE_INTERVAL = int(os.getenv("MARKET_UPDATE_INTERVAL", "30"))
CLAN_BANK_ACCOUNT = os.getenv("CLAN_BANK_ACCOUNT", "vVtNN")

BASE_FEE = Decimal(os.getenv("BASE_FEE", "0.015"))
PER_TRADE_CAP_PERCENT = Decimal(os.getenv("PER_TRADE_CAP_PERCENT", "0.01"))
PER_TRADE_CAP_ABS = Decimal(os.getenv("PER_TRADE_CAP_ABS", "300"))
CIRCUIT_BREAKER_HARD = Decimal("0.15")
