# services/limit_orders.py
import logging
from typing import List, Tuple
from database import get_conn

logger = logging.getLogger(__name__)

def create_limit_order(discord_id: str, ticker: str, side: str, price: float, shares: int) -> int:
    with get_conn() as conn:
        if hasattr(conn, "cursor"):
            cur = conn.cursor()
            cur.execute("INSERT INTO limit_orders(discord_id, ticker, side, price, shares) VALUES (%s, %s, %s, %s, %s) RETURNING order_id", (discord_id, ticker, side, price, shares))
            row = cur.fetchone()
            conn.commit()
            return int(row[0]) if row else 0
        else:
            cur = conn.execute("INSERT INTO limit_orders(discord_id, ticker, side, price, shares) VALUES (?, ?, ?, ?, ?)", (discord_id, ticker, side, price, shares))
            conn.commit()
            return cur.lastrowid if hasattr(cur, "lastrowid") else 0

def cancel_limit_order(order_id: int, discord_id: str) -> bool:
    with get_conn() as conn:
        cur = conn.execute("SELECT discord_id FROM limit_orders WHERE order_id = ?", (order_id,))
        row = cur.fetchone()
        if not row:
            return False
        owner = row[0]
        if owner != discord_id:
            return False
        conn.execute("DELETE FROM limit_orders WHERE order_id = ?", (order_id,))
        conn.commit()
        return True

def list_limit_orders(discord_id: str) -> List[Tuple]:
    with get_conn() as conn:
        cur = conn.execute("SELECT order_id, ticker, side, price, shares, created_at FROM limit_orders WHERE discord_id = ? ORDER BY created_at DESC", (discord_id,))
        return cur.fetchall()
