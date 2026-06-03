import logging
from database import get_conn

logger = logging.getLogger(__name__)

def create_user_if_missing(discord_id: str, game_name: str, initial_gold: int = 0):
    with get_conn() as conn:
        cur = conn.execute("SELECT discord_id FROM users WHERE discord_id = ?", (discord_id,))
        if cur.fetchone():
            return False
        conn.execute("INSERT INTO users(discord_id, game_name, internal_gold, verified) VALUES (?, ?, ?, ?)", (discord_id, game_name, initial_gold, 0))
        conn.commit()
        return True

def get_user_by_game_name(game_name: str):
    with get_conn() as conn:
        cur = conn.execute("SELECT discord_id, internal_gold, verified FROM users WHERE game_name = ?", (game_name,))
        return cur.fetchone()

def adjust_internal_gold(discord_id: str, delta: int):
    with get_conn() as conn:
        conn.execute("UPDATE users SET internal_gold = internal_gold + ? WHERE discord_id = ?", (delta, discord_id))
        conn.commit()
