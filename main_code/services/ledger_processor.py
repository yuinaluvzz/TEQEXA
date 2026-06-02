# ledger_processor.py
import json
import logging
from typing import List, Dict, Tuple
from database import get_conn
from utils import now_ts, safe_int
from users import create_user_if_missing, get_user_by_game_name, adjust_internal_gold

logger = logging.getLogger(__name__)

def _insert_audit(conn, actor: str, action: str, details: str):
    """
    Insert an audit log row. conn is a DB connection (sqlite3 or psycopg2).
    """
    try:
        if hasattr(conn, "cursor"):
            cur = conn.cursor()
            cur.execute("INSERT INTO audit_logs(actor, action, details) VALUES (%s, %s, %s)", (actor, action, details))
        else:
            conn.execute("INSERT INTO audit_logs(actor, action, details) VALUES (?, ?, ?)", (actor, action, details))
    except Exception:
        logger.exception("Failed to write audit log")

def _insert_deposit_ledger(conn, discord_id: str, game_name: str, gross: int, fee: int, credited: int, tx_id: str, ts: int):
    try:
        if hasattr(conn, "cursor"):
            cur = conn.cursor()
            cur.execute(
                "INSERT INTO deposit_ledger(discord_id, game_name, gross_amount, fee_amount, credited_amount, tx_id, timestamp) VALUES (%s, %s, %s, %s, %s, %s, %s)",
                (discord_id, game_name, gross, fee, credited, tx_id, ts)
            )
        else:
            conn.execute(
                "INSERT INTO deposit_ledger(discord_id, game_name, gross_amount, fee_amount, credited_amount, tx_id, timestamp) VALUES (?, ?, ?, ?, ?, ?, ?)",
                (discord_id, game_name, gross, fee, credited, tx_id, ts)
            )
    except Exception:
        logger.exception("Failed to insert deposit_ledger")

def _insert_verification(conn, tx_id: str, discord_id: str, game_name: str, nonce: str, raw_payload: str):
    try:
        if hasattr(conn, "cursor"):
            cur = conn.cursor()
            cur.execute(
                "INSERT INTO verifications(tx_id, discord_id, game_name, nonce, raw_payload, verified_at) VALUES (%s, %s, %s, %s, %s, now())",
                (tx_id, discord_id, game_name, nonce, raw_payload)
            )
        else:
            conn.execute(
                "INSERT OR REPLACE INTO verifications(tx_id, discord_id, game_name, nonce, raw_payload, verified_at) VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP)",
                (tx_id, discord_id, game_name, nonce, raw_payload)
            )
    except Exception:
        logger.exception("Failed to insert verification")

def process_rows_in_db(rows: List[Dict]) -> Tuple[int, List[str]]:
    """
    Process parsed ledger rows and credit users when appropriate.
    Returns (processed_count, newly_verified_tx_ids)
    """
    processed = 0
    newly_verified = []

    if not rows:
        return processed, newly_verified

    with get_conn() as conn:
        # For sqlite3, conn.execute is available; for psycopg2 we use cursor()
        is_pg = hasattr(conn, "cursor")
        try:
            for r in rows:
                try:
                    tx_id = r.get("tx_id") or ""
                    ts = int(r.get("time", now_ts()))
                    sender = r.get("sender", "")
                    receiver = r.get("receiver", "")  # often game_name
                    amount = safe_int(r.get("amount", 0))
                    fee = safe_int(r.get("fee", 0))
                    raw_line = r.get("raw_line") or json.dumps(r)

                    # Skip zero or negative amounts
                    if amount <= 0:
                        _insert_audit(conn, sender, "ignored_zero_amount", raw_line)
                        continue

                    # If receiver looks like a game_name, try to find a user
                    # First, check verifications table to avoid double-credit
                    if is_pg:
                        cur = conn.cursor()
                        cur.execute("SELECT tx_id FROM deposit_ledger WHERE tx_id = %s", (tx_id,))
                        if cur.fetchone():
                            logger.debug("Skipping already processed tx %s", tx_id)
                            continue
                    else:
                        cur = conn.execute("SELECT tx_id FROM deposit_ledger WHERE tx_id = ?", (tx_id,))
                        if cur.fetchone():
                            logger.debug("Skipping already processed tx %s", tx_id)
                            continue

                    # Try to find user by game_name
                    if is_pg:
                        cur = conn.cursor()
                        cur.execute("SELECT discord_id, verified FROM users WHERE game_name = %s", (receiver,))
                        user_row = cur.fetchone()
                    else:
                        cur = conn.execute("SELECT discord_id, verified FROM users WHERE game_name = ?", (receiver,))
                        user_row = cur.fetchone()

                    if not user_row:
                        # No matching user: log and continue
                        _insert_audit(conn, sender, "deposit_unmatched", raw_line)
                        # Optionally, create a placeholder user? We will not auto-create here.
                        continue

                    # Extract discord_id and verified flag
                    if isinstance(user_row, dict):
                        discord_id = user_row.get("discord_id")
                        verified_flag = user_row.get("verified", 0)
                    else:
                        discord_id = user_row[0]
                        verified_flag = user_row[1] if len(user_row) > 1 else 0

                    # Compute credited amount (gross - fee)
                    credited = max(0, amount - fee)

                    # Insert deposit_ledger and credit user
                    _insert_deposit_ledger(conn, discord_id, receiver, amount, fee, credited, tx_id, ts)

                    # Update user's internal_gold
                    if is_pg:
                        cur = conn.cursor()
                        cur.execute("UPDATE users SET internal_gold = internal_gold + %s WHERE discord_id = %s", (credited, discord_id))
                    else:
                        conn.execute("UPDATE users SET internal_gold = internal_gold + ? WHERE discord_id = ?", (credited, discord_id))

                    # Insert audit log
                    _insert_audit(conn, discord_id, "deposit_credited", f"{tx_id}:{amount}:{fee}:{credited}")

                    processed += 1

                except Exception as e:
                    logger.exception("Error processing ledger row: %s", e)
                    # continue to next row
                    continue

            # Commit at the end for sqlite; for psycopg2 commit on conn
            try:
                conn.commit()
            except Exception:
                # Some DB wrappers auto-commit; ignore commit errors
                pass

        except Exception:
            logger.exception("Unexpected error in process_rows_in_db")
            try:
                conn.rollback()
            except Exception:
                pass

    return processed, newly_verified
