import asyncio
import logging
import aiohttp
from database import get_ledger_last_seen, set_ledger_last_seen, init_db
from ledger import fetch_live_ledger_rows, load_mock_ledger_entries
from ledger_processor import process_rows_in_db
from features.market_events import MarketEvents
from features.dividends import Dividends
from config import LEDGER_POLL_INTERVAL, MARKET_UPDATE_INTERVAL, PORT
from health import start_health_server

logger = logging.getLogger(__name__)

_tasks = []

def start_all(bot):
    loop = bot.loop
    _tasks.append(loop.create_task(_ledger_poller()))
    _tasks.append(loop.create_task(_market_event_loop()))
    _tasks.append(loop.create_task(_dividend_loop()))
    _tasks.append(loop.create_task(start_health_server()))
    logger.info("Background tasks started")

async def _ledger_poller():
    while True:
        try:
            last_seen = get_ledger_last_seen()
            rows = await fetch_live_ledger_rows(last_seen)
            if not rows:
                # fallback to mock
                mock = load_mock_ledger_entries()
                rows = []
                for e in mock:
                    ts = int(e.get("timestamp_ts", 0)) or int(__import__("time").time())
                    tx_id = e.get("tx_id") or f"{ts}_{e.get('game_name')}_{e.get('amount')}"
                    rows.append({"tx_id": tx_id, "time": ts, "sender": e.get("sender", ""), "receiver": e.get("game_name", ""), "amount": int(e.get("amount", 0)), "fee": int(e.get("fee", 0)), "raw_line": str(e)})
            rows = [r for r in rows if int(r.get("time", 0)) > last_seen]
            rows.sort(key=lambda x: int(x.get("time", 0)))
            if rows:
                processed, newly_verified = await asyncio.to_thread(process_rows_in_db, rows)
                max_ts = max(int(r.get("time", 0)) for r in rows)
                set_ledger_last_seen(max_ts)
                logger.info("Processed %d ledger rows", processed)
        except Exception:
            logger.exception("Ledger poller error")
        await asyncio.sleep(LEDGER_POLL_INTERVAL)

async def _market_event_loop():
    while True:
        try:
            MarketEvents.generate_event()
        except Exception:
            logger.exception("Market event error")
        await asyncio.sleep(MARKET_UPDATE_INTERVAL)

async def _dividend_loop():
    # Run dividends every 24 hours for demo (adjust as needed)
    while True:
        try:
            Dividends.payout_dividends()
        except Exception:
            logger.exception("Dividend payout error")
        await asyncio.sleep(24 * 3600)
