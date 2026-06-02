from database import get_conn

def record_trade(discord_id, ticker, ttype, gross_gold, net_gold, shares, fee, price_before, price_after, maker_flag=0):
    with get_conn() as conn:
        conn.execute("""INSERT INTO trades(discord_id, ticker, type, gross_gold, net_gold, shares, fee, price_before, price_after, maker_flag)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                     (discord_id, ticker, ttype, gross_gold, net_gold, shares, fee, price_before, price_after, maker_flag))
        conn.commit()

def get_trade_history(discord_id):
    with get_conn() as conn:
        cur = conn.execute("SELECT * FROM trades WHERE discord_id = ? ORDER BY timestamp DESC LIMIT 100", (discord_id,))
        return cur.fetchall()
