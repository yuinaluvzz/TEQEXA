import logging
from database import get_conn
from decimal import Decimal

logger = logging.getLogger(__name__)

class Dividends:
    @staticmethod
    def init_dividend_config():
        config = {
            "EURO": 0.02, "STRM": 0.015, "ASIA": 0.018, "CLAN": 0.025,
            "AMER": 0.016, "MENA": 0.012, "AFRI": 0.010, "PACI": 0.008, "BOTS": 0.005
        }
        with get_conn() as conn:
            for ticker, yield_pct in config.items():
                conn.execute("INSERT OR REPLACE INTO dividend_config(ticker, dividend_yield) VALUES (?, ?)", (ticker, yield_pct))
            conn.commit()

    @staticmethod
    def payout_dividends():
        total_paid = 0
        with get_conn() as conn:
            cur = conn.execute("SELECT ticker, dividend_yield FROM dividend_config")
            tickers = cur.fetchall()
            for ticker, yield_pct in tickers:
                cur2 = conn.execute("SELECT discord_id, shares FROM portfolios WHERE ticker = ? AND shares > 0", (ticker,))
                holders = cur2.fetchall()
                cur3 = conn.execute("SELECT gold_pool, share_pool FROM tickers WHERE ticker = ?", (ticker,))
                pool_row = cur3.fetchone()
                if not pool_row:
                    continue
                gold_pool, share_pool = pool_row
                current_price = Decimal(gold_pool) / Decimal(share_pool) if share_pool > 0 else Decimal(0)
                for discord_id, shares in holders:
                    dividend_amount = int(Decimal(shares) * current_price * Decimal(yield_pct))
                    if dividend_amount > 0:
                        conn.execute("UPDATE users SET internal_gold = internal_gold + ? WHERE discord_id = ?", (dividend_amount, discord_id))
                        conn.execute("INSERT INTO dividends_paid(discord_id, ticker, amount) VALUES (?, ?, ?)", (discord_id, ticker, dividend_amount))
                        total_paid += dividend_amount
            conn.commit()
        logger.info("Dividend payout complete. Total paid: %s gold", total_paid)
        return total_paid
