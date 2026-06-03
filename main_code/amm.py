import logging
from decimal import Decimal
from database import get_conn
from config import BASE_FEE, PER_TRADE_CAP_PERCENT, PER_TRADE_CAP_ABS, CIRCUIT_BREAKER_HARD

logger = logging.getLogger(__name__)

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
                fee_decimal = (gross * fee)
                fee_int = int(fee_decimal)
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
                conn.execute("UPDATE users SET internal_gold = internal_gold - ? WHERE discord_id = ?", (int(gross), discord_id))
                conn.execute("UPDATE tickers SET gold_pool = ?, share_pool = ?, last_updated = CURRENT_TIMESTAMP WHERE ticker = ?", (int(G_new), int(S_new), ticker))
                cur = conn.execute("SELECT shares FROM portfolios WHERE discord_id = ? AND ticker = ?", (discord_id, ticker))
                row = cur.fetchone()
                if row:
                    conn.execute("UPDATE portfolios SET shares = shares + ? WHERE discord_id = ? AND ticker = ?", (int(shares_received), discord_id, ticker))
                else:
                    conn.execute("INSERT INTO portfolios(discord_id, ticker, shares) VALUES (?, ?, ?)", (discord_id, ticker, int(shares_received)))
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
                fee_decimal = (gross_gold_out * fee)
                fee_int = int(fee_decimal)
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
                conn.execute("UPDATE portfolios SET shares = shares - ? WHERE discord_id = ? AND ticker = ?", (int(shares), discord_id, ticker))
                conn.execute("UPDATE tickers SET gold_pool = ?, share_pool = ?, last_updated = CURRENT_TIMESTAMP WHERE ticker = ?", (int(G_new), int(S_new), ticker))
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
