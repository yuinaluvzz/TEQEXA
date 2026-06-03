# services/verification.py
import logging
import secrets
from typing import Optional
from database import get_conn
from utils import now_ts

logger = logging.getLogger(__name__)

NONCE_TTL_SECONDS = 60 * 60  # 1 hour

def create_link_nonce(discord_id: str, game_name: str) -> str:
    nonce = secrets.token_hex(8)
    raw = f"{discord_id}:{game_name}:{nonce}:{now_ts()}"
    with get_conn() as conn:
        if hasattr(conn, "cursor"):
            cur = conn.cursor()
            cur.execute(
                "INSERT INTO verifications(tx_id, discord_id, game_name, nonce, raw_payload, verified_at) VALUES (%s, %s, %s, %s, %s, NULL) ON CONFLICT (tx_id) DO UPDATE SET nonce = EXCLUDED.nonce",
                (nonce, discord_id, game_name, nonce, raw)
            )
            conn.commit()
        else:
            conn.execute(
                "INSERT OR REPLACE INTO verifications(tx_id, discord_id, game_name, nonce, raw_payload, verified_at) VALUES (?, ?, ?, ?, ?, NULL)",
                (nonce, discord_id, game_name, nonce, raw)
            )
            conn.commit()
    logger.info("Created link nonce for %s -> %s", discord_id, game_name)
    return nonce

def verify_nonce(nonce: str, sender_game_name: str) -> Optional[str]:
    """
    Called when a ledger row arrives with receiver == sender_game_name.
    If a matching nonce exists for that game_name, return discord_id and mark verified.
    """
    with get_conn() as conn:
        if hasattr(conn, "cursor"):
            cur = conn.cursor()
            cur.execute("SELECT tx_id, discord_id, game_name, nonce FROM verifications WHERE nonce = %s", (nonce,))
            row = cur.fetchone()
            if not row:
                return None
            tx_id = row[0] if isinstance(row, (list, tuple)) else row.get("tx_id")
            discord_id = row[1] if isinstance(row, (list, tuple)) else row.get("discord_id")
            game_name = row[2] if isinstance(row, (list, tuple)) else row.get("game_name")
            if game_name != sender_game_name:
                return None
            cur.execute("UPDATE verifications SET verified_at = now() WHERE tx_id = %s", (tx_id,))
            conn.commit()
            return discord_id
        else:
            cur = conn.execute("SELECT tx_id, discord_id, game_name, nonce FROM verifications WHERE nonce = ?", (nonce,))
            row = cur.fetchone()
            if not row:
                return None
            tx_id, discord_id, game_name, _ = row
            if game_name != sender_game_name:
                return None
            conn.execute("UPDATE verifications SET verified_at = CURRENT_TIMESTAMP WHERE tx_id = ?", (tx_id,))
            conn.commit()
            return discord_id
