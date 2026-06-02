import asyncio
import logging
import discord
from discord.ext import commands
from logging_config import configure_logging
from database import init_db
from config import DISCORD_TOKEN
import discord_commands
import background_tasks

configure_logging()
logger = logging.getLogger(__name__)

intents = discord.Intents.default()
intents.message_content = True
bot = commands.Bot(command_prefix="!", intents=intents)

@bot.event
async def on_ready():
    logger.info("Bot ready: %s", bot.user)
    # Register commands
    discord_commands.register(bot)
    # Start background tasks
    background_tasks.start_all(bot)

def main():
    init_db()
    if not DISCORD_TOKEN:
        logger.info("DISCORD_TOKEN not set; bot will not connect to Discord. Exiting after DB init.")
        return
    try:
        bot.run(DISCORD_TOKEN)
    except KeyboardInterrupt:
        pass

if __name__ == "__main__":
    main()
