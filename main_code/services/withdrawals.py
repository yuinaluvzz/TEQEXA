# services/withdrawals.py
import logging
from typing import List, Tuple, Optional
from database import get_conn

logger = logging.getLogger(__name__)

def request_withdrawal(discord_id: str, amount: int) -> int:
    with get_conn() as conn:
        if hasattr(conn, "cursor"):
            cur = conn.cursor()
            cur.execute("INSERT INTO withdrawals(discord_id, amount, status) VALUES (%s, %s, 'PENDING') RETURNING withdrawal_id", (discord_id, amount))
            row = cur.fetchone()
            conn.commit()
            return int(row[0]) if row else 0
        else:
            cur = conn.execute("INSERT INTO withdrawals(discord_id, amount, status) VALUES (?, ?, 'PENDING')", (discord_id, amount))
            conn.commit()
            # SQLite: get lastrowid
            return cur.lastrowid if hasattr(cur, "lastrowid") else 0

def export_withdrawals_pending() -> List[Tuple]:
    with get_conn() as conn:
        cur = conn.execute("SELECT withdrawal_id, discord_id, amount, status, requested_at FROM withdrawals WHERE status = 'PENDING' ORDER BY requested_at ASC")
        return cur.fetchall()

def mark_withdrawal_sent(withdrawal_id: int, sent_amount: int, admin_batch_id: Optional[int] = None):
    with get_conn() as conn:
        if hasattr(conn, "cursor"):
            cur = conn.cursor()
            cur.execute("UPDATE withdrawals SET status = 'SENT', admin_batch_id = %s WHERE withdrawal_id = %s", (admin_batch_id, withdrawal_id))
            conn.commit()
        else:
            conn.execute("UPDATE withdrawals SET status = 'SENT', admin_batch_id = ? WHERE withdrawal_id = ?", (admin_batch_id, withdrawal_id))
            conn.commit()
    logger.info("Marked withdrawal %s as SENT", withdrawal_id)
