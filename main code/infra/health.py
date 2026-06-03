import asyncio
from aiohttp import web
import logging
from config import PORT

logger = logging.getLogger(__name__)

async def _handle(request):
    return web.Response(text="OK")

async def start_health_server():
    app = web.Application()
    app.router.add_get("/health", _handle)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", PORT)
    await site.start()
    logger.info("Health server started on port %s", PORT)
    # keep running
    while True:
        await asyncio.sleep(3600)
