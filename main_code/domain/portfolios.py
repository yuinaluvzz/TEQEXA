from database import get_conn

def get_shares(discord_id: str, ticker: str):
    with get_conn() as conn:
        cur = conn.execute("SELECT shares FROM portfolios WHERE discord_id = ? AND ticker = ?", (discord_id, ticker))
        row = cur.fetchone()
        return row[0] if row else 0

def update_shares(discord_id: str, ticker: str, delta: int):
    with get_conn() as conn:
        cur = conn.execute("SELECT shares FROM portfolios WHERE discord_id = ? AND ticker = ?", (discord_id, ticker))
        row = cur.fetchone()
        if row:
            conn.execute("UPDATE portfolios SET shares = shares + ? WHERE discord_id = ? AND ticker = ?", (delta, discord_id, ticker))
        else:
            conn.execute("INSERT INTO portfolios(discord_id, ticker, shares) VALUES (?, ?, ?)", (discord_id, ticker, delta))
        conn.commit()

def list_holdings(discord_id: str):
    with get_conn() as conn:
        cur = conn.execute("SELECT ticker, shares FROM portfolios WHERE discord_id = ? AND shares > 0", (discord_id,))
        return cur.fetchall()
