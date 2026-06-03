import random
import logging
from decimal import Decimal
from database import get_conn
from config import CIRCUIT_BREAKER_HARD

logger = logging.getLogger(__name__)

class MarketEvents:
    EVENTS = [
        {"name": "Tournament Victory", "tickers": ["EURO"], "impact": 0.05, "desc": "🏆 EURO wins regional tournament!"},
        {"name": "Scandal", "tickers": ["ASIA"], "impact": -0.08, "desc": "📉 ASIA scandal breaks news"},
        {"name": "Tech Exploit", "tickers": ["BOTS"], "impact": -0.10, "desc": "🐛 BOTS exploit discovered"},
        {"name": "Streamer Hype", "tickers": ["STRM"], "impact": 0.07, "desc": "🎬 STRM trending on social media"},
        {"name": "Clan War", "tickers": ["CLAN"], "impact": 0.06, "desc": "⚔️ CLAN dominates in warfare"},
        {"name": "Market Crash", "tickers": ["EURO", "ASIA", "AMER"], "impact": -0.15, "desc": "📉 MARKET CRASH: -15% across regions"},
        {"name": "Bull Run", "tickers": ["MENA", "AFRI", "PACI"], "impact": 0.12, "desc": "📈 BULL RUN: Emerging markets surge"},
        {"name": "Economic Boom", "tickers": ["AMER"], "impact": 0.09, "desc": "💰 AMER economy booming"},
    ]

    @staticmethod
    def generate_event():
        event = random.choice(MarketEvents.EVENTS)
        with get_conn() as conn:
            conn.execute("INSERT INTO market_events(event_name, event_description, affected_tickers, price_impact_percent) VALUES (?, ?, ?, ?)",
                         (event["name"], event["desc"], ",".join(event["tickers"]), event["impact"]))
            for ticker in event["tickers"]:
                cur = conn.execute("SELECT gold_pool, share_pool, day_start_price FROM tickers WHERE ticker = ?", (ticker,))
                row = cur.fetchone()
                if not row:
                    continue
                gold_pool, share_pool, day_start_price = row
                impact_multiplier = Decimal(1) + Decimal(event["impact"])
                new_gold_pool = int(Decimal(gold_pool) * impact_multiplier)
                old_price = Decimal(gold_pool) / Decimal(share_pool) if share_pool else Decimal(0)
                new_price = Decimal(new_gold_pool) / Decimal(share_pool) if share_pool else Decimal(0)
                change = abs((new_price - old_price) / old_price) if old_price > 0 else Decimal(0)
                if change > CIRCUIT_BREAKER_HARD:
                    conn.execute("UPDATE tickers SET is_frozen = 1, last_updated = CURRENT_TIMESTAMP WHERE ticker = ?", (ticker,))
                    logger.info("Market event %s on %s would breach circuit breaker — ticker frozen instead", event["name"], ticker)
                else:
                    conn.execute("UPDATE tickers SET gold_pool = ?, last_updated = CURRENT_TIMESTAMP WHERE ticker = ?", (new_gold_pool, ticker))
            conn.commit()
        logger.info("Market event applied: %s", event["name"])
        return event
