from database import get_conn

def request_withdrawal(discord_id, amount):
    with get_conn() as conn:
        conn.execute("INSERT INTO withdrawals(discord_id, amount, status) VALUES (?, ?, 'PENDING')", (discord_id, amount))
        conn.commit()

def export_withdrawals():
    with get_conn() as conn:
        cur = conn.execute("SELECT withdrawal_id, discord_id, amount, status, requested_at FROM withdrawals WHERE status = 'PENDING'")
        rows = cur.fetchall()
        return rows

def mark_withdrawal_sent(withdrawal_id, sent_amount, admin_batch_id=None):
    with get_conn() as conn:
        conn.execute("UPDATE withdrawals SET status = 'SENT', admin_batch_id = ? WHERE withdrawal_id = ?", (admin_batch_id, withdrawal_id))
        conn.commit()
