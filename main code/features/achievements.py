import logging
from database import get_conn

logger = logging.getLogger(__name__)

class Achievements:
    ACHIEVEMENT_DEFINITIONS = {
        "first_millionaire": {"name": "First Millionaire", "description": "Reach 1,000,000 gold portfolio value", "trigger": "portfolio_value", "threshold": 1000000},
        "survived_crash": {"name": "Crash Survivor", "description": "Hold through a 15%+ market crash", "trigger": "price_drop", "threshold": 0.15},
        "perfect_trade_streak": {"name": "Perfect Trader", "description": "5 consecutive profitable trades", "trigger": "trade_streak", "threshold": 5},
        "day_trader": {"name": "Day Trader", "description": "Execute 50 trades in a single day", "trigger": "daily_trades", "threshold": 50},
        "hodler": {"name": "HODLER", "description": "Hold the same ticker for 30 days", "trigger": "hold_duration", "threshold": 30},
        "diversified": {"name": "Diversified Portfolio", "description": "Own shares in all 9 tickers", "trigger": "ticker_count", "threshold": 9},
        "dividend_collector": {"name": "Dividend Collector", "description": "Earn 10,000 gold from dividends", "trigger": "dividend_income", "threshold": 10000},
        "price_alert_prophet": {"name": "Price Prophet", "description": "Hit 10 price alerts successfully", "trigger": "alert_hits", "threshold": 10}
    }

    @staticmethod
    def check_and_award(discord_id, trigger_type, data):
        with get_conn() as conn:
            for ach_key, ach_def in Achievements.ACHIEVEMENT_DEFINITIONS.items():
                if ach_def["trigger"] != trigger_type:
                    continue
                cur = conn.execute("SELECT achievement_id FROM achievements WHERE discord_id = ? AND achievement_name = ?", (discord_id, ach_key))
                if cur.fetchone():
                    continue
                earned = False
                if trigger_type == "portfolio_value" and data.get("value", 0) >= ach_def["threshold"]:
                    earned = True
                elif trigger_type == "price_drop" and data.get("drop_percent", 0) >= ach_def["threshold"]:
                    earned = True
                elif trigger_type == "trade_streak" and data.get("streak", 0) >= ach_def["threshold"]:
                    earned = True
                elif trigger_type == "daily_trades" and data.get("count", 0) >= ach_def["threshold"]:
                    earned = True
                elif trigger_type == "hold_duration" and data.get("days", 0) >= ach_def["threshold"]:
                    earned = True
                elif trigger_type == "ticker_count" and data.get("count", 0) >= ach_def["threshold"]:
                    earned = True
                elif trigger_type == "dividend_income" and data.get("total", 0) >= ach_def["threshold"]:
                    earned = True
                elif trigger_type == "alert_hits" and data.get("count", 0) >= ach_def["threshold"]:
                    earned = True
                if earned:
                    conn.execute("INSERT OR IGNORE INTO achievements(discord_id, achievement_name) VALUES (?, ?)", (discord_id, ach_key))
                    conn.execute("INSERT INTO audit_logs(actor, action, details) VALUES (?, ?, ?)", (discord_id, "achievement_earned", f"Earned: {ach_def['name']}"))
                    conn.commit()
                    logger.info("Achievement earned: %s -> %s", discord_id, ach_key)
