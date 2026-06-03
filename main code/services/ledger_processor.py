# services/ledger_processor.py
"""
Ledger processor: process parsed ledger rows, credit users, and record audit/deposit rows.

This module is defensive and works with both sqlite3-style connections (conn.execute)
and psycopg2-style connections (conn.cursor()) returned by database.get_conn().
"""

import json
import logging
from typing import List, Dict, Tuple, Optional

from database import get_conn
from utils import now_ts, safe_int

# verification service (expects verify_nonce(nonce, sender_game_name) -> discord_id | None)
from services.verification import verify_nonce

logger = logging.getLogger(__name__)


# -------------------------
# Helper DB writers
# -------------------------
def _insert_audit(conn, actor: str, action: str, details: str):
    """
    Insert an audit log row. Works with sqlite3 (conn.execute) and psycopg2 (conn.cursor()).
    """
    try:
        if hasattr(conn, "cursor"):
            cur = conn.cursor()
            cur.execute(
                "INSERT INTO audit_logs(actor, action, details) VALUES (%s, %s, %s)",
                (actor, action, details),
            )
        else:
            conn.execute(
                "INSERT INTO audit_logs(actor, action, details) VALUES (?, ?, ?)",
                (actor, action, details),
            )
    except Exception:
        logger.exception("Failed to write audit log")


def _insert_deposit_ledger(
    conn, discord_id: str, game_name: str, gross: int, fee: int, credited: int, tx_id: str, ts: int
):
    """
    Insert a deposit_ledger row. Works with sqlite3 and psycopg2.
    """
    try:
        if hasattr(conn, "cursor"):
            cur = conn.cursor()
            cur.execute(
                "INSERT INTO deposit_ledger(discord_id, game_name, gross_amount, fee_amount, credited_amount, tx_id, timestamp) VALUES (%s, %s, %s, %s, %s, %s, %s)",
                (discord_id, game_name, gross, fee, credited, tx_id, ts),
            )
        else:
            conn.execute(
                "INSERT INTO deposit_ledger(discord_id, game_name, gross_amount, fee_amount, credited_amount, tx_id, timestamp) VALUES (?, ?, ?, ?, ?, ?, ?)",
                (discord_id, game_name, gross, fee, credited, tx_id, ts),
            )
    except Exception:
        logger.exception("Failed to insert deposit_ledger")


# -------------------------
# Main processing function
# -------------------------
def process_rows_in_db(rows: List[Dict]) -> Tuple[int, List[str]]:
    """
    Process parsed ledger rows and credit users when appropriate.

    Args:
        rows: list of dicts with keys like tx_id, time, sender, receiver, amount, fee, memo/raw_line

    Returns:
        (processed_count, newly_verified_tx_ids)
    """
    processed = 0
    newly_verified: List[str] = []

    if not rows:
        return processed, newly_verified

    with get_conn() as conn:
        is_pg = hasattr(conn, "cursor")
        try:
            for r in rows:
                try:
                    tx_id = r.get("tx_id") or ""
                    ts = int(r.get("time", now_ts()))
                    sender = r.get("sender", "")
                    receiver = r.get("receiver", "")  # often the in-game name
                    amount = safe_int(r.get("amount", 0))
                    fee = safe_int(r.get("fee", 0))
                    raw_line = r.get("raw_line") or json.dumps(r)

                    # Skip invalid amounts
                    if amount <= 0:
                        _insert_audit(conn, sender, "ignored_zero_amount", raw_line)
                        continue

                    # Skip already processed tx
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

                    # -------------------------
                    # Verification flow
                    # -------------------------
                    # Try to detect a nonce in memo fields or receiver and verify it.
                    verified_discord_id: Optional[str] = None

                    # 1) Try memo-like fields first
                    possible_nonce = r.get("memo") or r.get("nonce") or r.get("memo_text")
                    if possible_nonce:
                        try:
                            verified_discord_id = verify_nonce(possible_nonce, receiver)
                        except Exception:
                            logger.exception("Error while verifying nonce from memo")

                    # 2) If not found, try receiver as nonce (some ledgers put nonce in receiver)
                    if not verified_discord_id:
                        try:
                            verified_discord_id = verify_nonce(receiver, receiver)
                        except Exception:
                            # not a nonce or verification failed; ignore
                            pass

                    if verified_discord_id:
                        # credit the verified discord account
                        credited = max(0, amount - fee)
                        _insert_deposit_ledger(conn, verified_discord_id, receiver, amount, fee, credited, tx_id, ts)
                        if is_pg:
                            cur = conn.cursor()
                            cur.execute("UPDATE users SET internal_gold = internal_gold + %s WHERE discord_id = %s", (credited, verified_discord_id))
                        else:
                            conn.execute("UPDATE users SET internal_gold = internal_gold + ? WHERE discord_id = ?", (credited, verified_discord_id))
                        _insert_audit(conn, verified_discord_id, "deposit_verified_and_credited", f"{tx_id}:{amount}:{fee}:{credited}")
                        newly_verified.append(tx_id)
                        processed += 1
                        continue  # done with this row

                    # -------------------------
                    # Fallback: match by game_name in users table
                    # -------------------------
                    if is_pg:
                        cur = conn.cursor()
                        cur.execute("SELECT discord_id, verified FROM users WHERE game_name = %s", (receiver,))
                        user_row = cur.fetchone()
                    else:
                        cur = conn.execute("SELECT discord_id, verified FROM users WHERE game_name = ?", (receiver,))
                        user_row = cur.fetchone()

                    if not user_row:
                        _insert_audit(conn, sender, "deposit_unmatched", raw_line)
                        continue

                    # Extract discord_id
                    if isinstance(user_row, dict):
                        discord_id = user_row.get("discord_id")
                    else:
                        discord_id = user_row[0]

                    credited = max(0, amount - fee)
                    _insert_deposit_ledger(conn, discord_id, receiver, amount, fee, credited, tx_id, ts)
                    if is_pg:
                        cur = conn.cursor()
                        cur.execute("UPDATE users SET internal_gold = internal_gold + %s WHERE discord_id = %s", (credited, discord_id))
                    else:
                        conn.execute("UPDATE users SET internal_gold = internal_gold + ? WHERE discord_id = ?", (credited, discord_id))
                    _insert_audit(conn, discord_id, "deposit_credited", f"{tx_id}:{amount}:{fee}:{credited}")
                    processed += 1

                except Exception:
                    logger.exception("Error processing ledger row")
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
