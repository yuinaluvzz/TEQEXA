import os
import json
import asyncio
import logging
from config import LEDGER_URL
from typing import List, Dict

logger = logging.getLogger(__name__)

LEDGER_FILE = os.path.join("mock_ledger", "ledger.json")

async def fetch_live_ledger_rows(since_ts: int) -> List[Dict]:
    try:
        import aiohttp
    except Exception:
        logger.warning("aiohttp not available")
        return []
    rows = []
    headers = {"User-Agent": "TerritorialBot"}
    timeout = aiohttp.ClientTimeout(total=120, connect=30, sock_read=60)
    connector = aiohttp.TCPConnector(limit=5, limit_per_host=2, ssl=False)
    async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
        try:
            async with session.get(LEDGER_URL, headers=headers, ssl=False) as resp:
                if resp.status != 200:
                    logger.warning("Ledger fetch returned status %s", resp.status)
                    return []
                text = await resp.text(errors="ignore")
        except Exception as e:
            logger.warning("Failed to fetch ledger: %s", e)
            return []
    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue
        parts = [p.strip() for p in line.split(",")]
        if len(parts) < 5:
            continue
        try:
            ts = int(parts[0])
            sender = parts[1]
            receiver = parts[2]
            amount = int(parts[3])
            fee = int(parts[4])
        except Exception:
            continue
        if ts <= since_ts:
            continue
        tx_id = f"{ts}_{sender}_{receiver}_{amount}"
        rows.append({
            "tx_id": tx_id,
            "time": ts,
            "sender": sender,
            "receiver": receiver,
            "amount": amount,
            "fee": fee,
            "raw_line": line
        })
    if rows:
        logger.info("Parsed %d new ledger entries", len(rows))
    return rows

def load_mock_ledger_entries():
    if not os.path.exists(LEDGER_FILE):
        return []
    try:
        with open(LEDGER_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
            return data if isinstance(data, list) else []
    except Exception:
        return []
