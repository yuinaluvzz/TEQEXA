import logging
from database import get_conn

logger = logging.getLogger(__name__)

def freeze_ticker(ticker):
    with get_conn() as conn:
        conn.execute("UPDATE tickers SET is_frozen = 1 WHERE ticker = ?", (ticker,))
        conn.commit()

def unfreeze_ticker(ticker):
    with get_conn() as conn:
        conn.execute("UPDATE tickers SET is_frozen = 0 WHERE ticker = ?", (ticker,))
        conn.commit()

def force_verify(tx_id, discord_id, game_name):
    with get_conn() as conn:
        conn.execute("INSERT OR REPLACE INTO verifications(tx_id, discord_id, game_name, verified_at) VALUES (?, ?, ?, CURRENT_TIMESTAMP)", (tx_id, discord_id, game_name))
        conn.commit()
