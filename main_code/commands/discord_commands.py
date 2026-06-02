# commands/discord_commands.py
import asyncio
import logging
from decimal import Decimal
import discord
from discord import app_commands
from config import GUILD_ID
from database import get_conn
from domain import get_ticker_price, get_leaderboard, get_price_history

logger = logging.getLogger(__name__)

def register(bot: discord.Client):
    """
    Register all slash commands on the bot's tree.
    Call this from bot.py: commands.register(bot)
    """
    tree = bot.tree

    @tree.command(name="buy", description="Buy shares of a ticker", guild=discord.Object(id=GUILD_ID) if GUILD_ID else None)
    @app_commands.describe(ticker="Ticker symbol", amount="Gold amount to spend")
    async def buy(interaction: discord.Interaction, ticker: str, amount: int):
        await interaction.response.defer()
        from amm import AMM
        amm = AMM()
        result = await asyncio.to_thread(amm.buy, str(interaction.user.id), ticker.upper(), amount)
        if result.get("ok"):
            await interaction.followup.send(f"Bought {result['shares']} shares of {ticker.upper()} at {result['price_after']:.6f}. Fee: {result['fee']}")
        else:
            await interaction.followup.send(f"Buy failed: {result.get('reason')}")

    @tree.command(name="sell", description="Sell shares of a ticker", guild=discord.Object(id=GUILD_ID) if GUILD_ID else None)
    @app_commands.describe(ticker="Ticker symbol", shares="Number of shares to sell")
    async def sell(interaction: discord.Interaction, ticker: str, shares: int):
        await interaction.response.defer()
        from amm import AMM
        amm = AMM()
        result = await asyncio.to_thread(amm.sell, str(interaction.user.id), ticker.upper(), shares)
        if result.get("ok"):
            await interaction.followup.send(f"Sold {shares} shares of {ticker.upper()}. Received {result['gold']} gold. Fee: {result['fee']}")
        else:
            await interaction.followup.send(f"Sell failed: {result.get('reason')}")

    @tree.command(name="status", description="Show your balance and holdings", guild=discord.Object(id=GUILD_ID) if GUILD_ID else None)
    async def status(interaction: discord.Interaction):
        await interaction.response.defer()
        with get_conn() as conn:
            cur = conn.execute("SELECT internal_gold FROM users WHERE discord_id = ?", (str(interaction.user.id),))
            row = cur.fetchone()
            gold = row[0] if row else 0
            cur2 = conn.execute("SELECT ticker, shares FROM portfolios WHERE discord_id = ? AND shares > 0", (str(interaction.user.id),))
            holdings = cur2.fetchall()
        msg = f"Gold: {gold}\nHoldings:\n"
        for t, s in holdings:
            msg += f"- {t}: {s}\n"
        await interaction.followup.send(msg)

    @tree.command(name="leaderboard", description="Show top players by portfolio value", guild=discord.Object(id=GUILD_ID) if GUILD_ID else None)
    async def leaderboard(interaction: discord.Interaction, limit: int = 10):
        await interaction.response.defer()
        lb = await asyncio.to_thread(get_leaderboard, limit)
        if not lb:
            await interaction.followup.send("No players found.")
            return
        lines = []
        for i, row in enumerate(lb, start=1):
            lines.append(f"**{i}. {row['game_name']}** — {row['total_value']} gold")
        await interaction.followup.send("\n".join(lines))

    @tree.command(name="history", description="Show price history for a ticker", guild=discord.Object(id=GUILD_ID) if GUILD_ID else None)
    @app_commands.describe(ticker="Ticker symbol", limit="Number of history points")
    async def history(interaction: discord.Interaction, ticker: str, limit: int = 20):
        await interaction.response.defer()
        hist = await asyncio.to_thread(get_price_history, ticker.upper(), limit)
        if not hist:
            await interaction.followup.send(f"No history for {ticker.upper()}.")
            return
        lines = [f"{h['recorded_at']}: {h['price']}" for h in hist]
        await interaction.followup.send("\n".join(lines))

    @tree.command(name="stocks", description="Show current price for a ticker or all tickers", guild=discord.Object(id=GUILD_ID) if GUILD_ID else None)
    @app_commands.describe(ticker="Optional ticker symbol (leave blank for all)")
    async def stocks(interaction: discord.Interaction, ticker: str | None = None):
        await interaction.response.defer()
        if ticker:
            price = await asyncio.to_thread(get_ticker_price, ticker.upper())
            if price is None:
                await interaction.followup.send(f"Ticker {ticker.upper()} not found.")
            else:
                await interaction.followup.send(f"{ticker.upper()} price: {float(price):.6f}")
            return
        # list all tickers
        with get_conn() as conn:
            cur = conn.execute("SELECT ticker, gold_pool, share_pool FROM tickers")
            rows = cur.fetchall()
        lines = []
        for t, gp, sp in rows:
            price = float(Decimal(gp) / Decimal(sp)) if sp and sp != 0 else 0.0
            lines.append(f"{t}: {price:.6f}")
        await interaction.followup.send("\n".join(lines))
