# exports.py
import csv
import os
from datetime import datetime
from database import get_conn

EXPORT_DIR = "exports"
os.makedirs(EXPORT_DIR, exist_ok=True)

def export_withdrawals_csv(filename: str | None = None):
    if filename is None:
        filename = f"withdrawals_{datetime.utcnow().strftime('%Y%m%dT%H%M%SZ')}.csv"
    path = os.path.join(EXPORT_DIR, filename)
    with get_conn() as conn:
        cur = conn.execute("SELECT withdrawal_id, discord_id, amount, status, requested_at FROM withdrawals ORDER BY requested_at DESC")
        rows = cur.fetchall()
        with open(path, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(["withdrawal_id", "discord_id", "amount", "status", "requested_at"])
            for r in rows:
                writer.writerow(r)
    return path

def export_trades_csv(filename: str | None = None, limit: int = 1000):
    if filename is None:
        filename = f"trades_{datetime.utcnow().strftime('%Y%m%dT%H%M%SZ')}.csv"
    path = os.path.join(EXPORT_DIR, filename)
    with get_conn() as conn:
        cur = conn.execute("SELECT trade_id, timestamp, discord_id, ticker, type, gross_gold, net_gold, shares, fee, price_before, price_after FROM trades ORDER BY timestamp DESC LIMIT ?", (limit,))
        rows = cur.fetchall()
        with open(path, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(["trade_id","timestamp","discord_id","ticker","type","gross_gold","net_gold","shares","fee","price_before","price_after"])
            for r in rows:
                writer.writerow(r)
    return path
