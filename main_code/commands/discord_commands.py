# commands/discord_commands.py
"""
Central registration of Discord slash commands.

Usage:
    from commands import register
    register(bot)

This file intentionally keeps command handlers thin and delegates logic to
domain/services modules to avoid circular imports and keep handlers testable.
"""

import asyncio
import logging
from decimal import Decimal
from typing import Optional

import discord
from discord import app_commands

from database import get_conn
from config import GUILD_ID, ADMIN_IDS

logger = logging.getLogger(__name__)


def register(bot: discord.Client):
    """
    Register all slash commands on the bot's command tree.
    Call this from bot.py after creating the bot instance:
        from commands.discord_commands import register
        register(bot)
    """
    tree = bot.tree

    def _is_admin(user: discord.User) -> bool:
        """
        Simple admin check. ADMIN_IDS should be a list/iterable of string IDs in config.
        Falls back to guild administrator permission when available.
        """
        try:
            if ADMIN_IDS and str(user.id) in ADMIN_IDS:
                return True
            # If user is a Member in a guild context, check permissions
            if hasattr(user, "guild_permissions") and getattr(user, "guild_permissions", None):
                return user.guild_permissions.administrator
        except Exception:
            logger.exception("Error checking admin status")
        return False

    # -------------------------
    # Trading commands (AMM)
    # -------------------------
    @tree.command(
        name="buy",
        description="Buy shares of a ticker using the AMM",
        guild=discord.Object(id=GUILD_ID) if GUILD_ID else None,
    )
    @app_commands.describe(ticker="Ticker symbol", amount="Gold amount to spend")
    async def buy(interaction: discord.Interaction, ticker: str, amount: int):
        await interaction.response.defer()
        try:
            from amm import AMM

            amm = AMM()
            result = await asyncio.to_thread(amm.buy, str(interaction.user.id), ticker.upper(), amount)
            if result.get("ok"):
                await interaction.followup.send(
                    f"Bought {result['shares']} shares of {ticker.upper()} at {result['price_after']:.6f}. Fee: {result['fee']}"
                )
            else:
                await interaction.followup.send(f"Buy failed: {result.get('reason')}")
        except Exception:
            logger.exception("Buy command error")
            await interaction.followup.send("An error occurred while processing your buy request.")

    @tree.command(
        name="sell",
        description="Sell shares of a ticker using the AMM",
        guild=discord.Object(id=GUILD_ID) if GUILD_ID else None,
    )
    @app_commands.describe(ticker="Ticker symbol", shares="Number of shares to sell")
    async def sell(interaction: discord.Interaction, ticker: str, shares: int):
        await interaction.response.defer()
        try:
            from amm import AMM

            amm = AMM()
            result = await asyncio.to_thread(amm.sell, str(interaction.user.id), ticker.upper(), shares)
            if result.get("ok"):
                await interaction.followup.send(
                    f"Sold {shares} shares of {ticker.upper()}. Received {result['gold']} gold. Fee: {result['fee']}"
                )
            else:
                await interaction.followup.send(f"Sell failed: {result.get('reason')}")
        except Exception:
            logger.exception("Sell command error")
            await interaction.followup.send("An error occurred while processing your sell request.")

    # -------------------------
    # Account / portfolio
    # -------------------------
    @tree.command(
        name="status",
        description="Show your balance and holdings",
        guild=discord.Object(id=GUILD_ID) if GUILD_ID else None,
    )
    async def status(interaction: discord.Interaction):
        await interaction.response.defer()
        try:
            discord_id = str(interaction.user.id)
            with get_conn() as conn:
                cur = conn.execute("SELECT internal_gold FROM users WHERE discord_id = ?", (discord_id,))
                row = cur.fetchone()
                gold = row[0] if row else 0
                cur2 = conn.execute(
                    "SELECT ticker, shares FROM portfolios WHERE discord_id = ? AND shares > 0", (discord_id,)
                )
                holdings = cur2.fetchall()
            msg_lines = [f"**Gold:** {gold}", "**Holdings:**"]
            if holdings:
                for t, s in holdings:
                    msg_lines.append(f"- {t}: {s}")
            else:
                msg_lines.append("No holdings.")
            await interaction.followup.send("\n".join(msg_lines))
        except Exception:
            logger.exception("Status command error")
            await interaction.followup.send("Failed to fetch your status.")

    # -------------------------
    # Leaderboard / Stocks / History
    # -------------------------
    @tree.command(
        name="leaderboard",
        description="Show top players by portfolio value",
        guild=discord.Object(id=GUILD_ID) if GUILD_ID else None,
    )
    async def leaderboard(interaction: discord.Interaction, limit: int = 10):
        await interaction.response.defer()
        try:
            from domain import get_leaderboard

            lb = await asyncio.to_thread(get_leaderboard, limit)
            if not lb:
                await interaction.followup.send("No players found.")
                return
            lines = []
            for i, row in enumerate(lb, start=1):
                lines.append(f"**{i}. {row['game_name']}** — {row['total_value']} gold")
            await interaction.followup.send("\n".join(lines))
        except Exception:
            logger.exception("Leaderboard command error")
            await interaction.followup.send("Failed to fetch leaderboard.")

    @tree.command(
        name="history",
        description="Show price history for a ticker",
        guild=discord.Object(id=GUILD_ID) if GUILD_ID else None,
    )
    @app_commands.describe(ticker="Ticker symbol", limit="Number of history points")
    async def history(interaction: discord.Interaction, ticker: str, limit: int = 20):
        await interaction.response.defer()
        try:
            from domain import get_price_history

            hist = await asyncio.to_thread(get_price_history, ticker.upper(), limit)
            if not hist:
                await interaction.followup.send(f"No history for {ticker.upper()}.")
                return
            lines = [f"{h['recorded_at']}: {h['price']}" for h in hist]
            await interaction.followup.send("\n".join(lines))
        except Exception:
            logger.exception("History command error")
            await interaction.followup.send("Failed to fetch price history.")

    @tree.command(
        name="stocks",
        description="Show current price for a ticker or all tickers",
        guild=discord.Object(id=GUILD_ID) if GUILD_ID else None,
    )
    @app_commands.describe(ticker="Optional ticker symbol (leave blank for all)")
    async def stocks(interaction: discord.Interaction, ticker: Optional[str] = None):
        await interaction.response.defer()
        try:
            from domain import get_ticker_price

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
        except Exception:
            logger.exception("Stocks command error")
            await interaction.followup.send("Failed to fetch stocks.")

    # -------------------------
    # Verification / Linking
    # -------------------------
    @tree.command(
        name="link",
        description="Link your game name to your Discord account",
        guild=discord.Object(id=GUILD_ID) if GUILD_ID else None,
    )
    @app_commands.describe(game_name="Your in-game name")
    async def link(interaction: discord.Interaction, game_name: str):
        await interaction.response.defer(ephemeral=True)
        try:
            from services.verification import create_link_nonce

            discord_id = str(interaction.user.id)
            nonce = await asyncio.to_thread(create_link_nonce, discord_id, game_name)
            await interaction.followup.send(
                f"To verify, send a deposit with memo/nonce: **{nonce}** to your game account. It expires in 1 hour."
            )
        except Exception:
            logger.exception("Link command error")
            await interaction.followup.send("Failed to create verification nonce.")

    # -------------------------
    # Withdrawals
    # -------------------------
    @tree.command(
        name="withdraw",
        description="Request a withdrawal of gold",
        guild=discord.Object(id=GUILD_ID) if GUILD_ID else None,
    )
    @app_commands.describe(amount="Amount of gold to withdraw")
    async def withdraw(interaction: discord.Interaction, amount: int):
        await interaction.response.defer()
        try:
            discord_id = str(interaction.user.id)
            with get_conn() as conn:
                cur = conn.execute("SELECT internal_gold FROM users WHERE discord_id = ?", (discord_id,))
                row = cur.fetchone()
                if not row or row[0] < amount:
                    await interaction.followup.send("Insufficient gold for withdrawal.")
                    return
            from services.withdrawals import request_withdrawal

            wid = await asyncio.to_thread(request_withdrawal, discord_id, amount)
            await interaction.followup.send(f"Withdrawal requested (id: {wid}). An admin will process it.")
        except Exception:
            logger.exception("Withdraw command error")
            await interaction.followup.send("Failed to request withdrawal.")

    # -------------------------
    # Limit orders
    # -------------------------
    @tree.command(
        name="limit_buy",
        description="Place a limit buy order",
        guild=discord.Object(id=GUILD_ID) if GUILD_ID else None,
    )
    @app_commands.describe(ticker="Ticker", price="Price per share", shares="Number of shares")
    async def limit_buy(interaction: discord.Interaction, ticker: str, price: float, shares: int):
        await interaction.response.defer()
        try:
            from services.limit_orders import create_limit_order

            order_id = await asyncio.to_thread(create_limit_order, str(interaction.user.id), ticker.upper(), "BUY", price, shares)
            await interaction.followup.send(f"Limit buy order created: {order_id}")
        except Exception:
            logger.exception("Limit buy command error")
            await interaction.followup.send("Failed to create limit buy order.")

    @tree.command(
        name="limit_sell",
        description="Place a limit sell order",
        guild=discord.Object(id=GUILD_ID) if GUILD_ID else None,
    )
    @app_commands.describe(ticker="Ticker", price="Price per share", shares="Number of shares")
    async def limit_sell(interaction: discord.Interaction, ticker: str, price: float, shares: int):
        await interaction.response.defer()
        try:
            from services.limit_orders import create_limit_order

            order_id = await asyncio.to_thread(create_limit_order, str(interaction.user.id), ticker.upper(), "SELL", price, shares)
            await interaction.followup.send(f"Limit sell order created: {order_id}")
        except Exception:
            logger.exception("Limit sell command error")
            await interaction.followup.send("Failed to create limit sell order.")

    @tree.command(
        name="cancel_order",
        description="Cancel your limit order",
        guild=discord.Object(id=GUILD_ID) if GUILD_ID else None,
    )
    @app_commands.describe(order_id="Order id to cancel")
    async def cancel_order(interaction: discord.Interaction, order_id: int):
        await interaction.response.defer()
        try:
            from services.limit_orders import cancel_limit_order

            ok = await asyncio.to_thread(cancel_limit_order, order_id, str(interaction.user.id))
            await interaction.followup.send("Order cancelled." if ok else "Failed to cancel order.")
        except Exception:
            logger.exception("Cancel order command error")
            await interaction.followup.send("Failed to cancel order.")

    @tree.command(
        name="my_orders",
        description="List your limit orders",
        guild=discord.Object(id=GUILD_ID) if GUILD_ID else None,
    )
    async def my_orders(interaction: discord.Interaction):
        await interaction.response.defer()
        try:
            from services.limit_orders import list_limit_orders

            rows = await asyncio.to_thread(list_limit_orders, str(interaction.user.id))
            if not rows:
                await interaction.followup.send("You have no active limit orders.")
                return
            lines = [f"{r[0]} | {r[1]} | {r[2]} | {r[3]} | {r[4]}" for r in rows]
            await interaction.followup.send("\n".join(lines))
        except Exception:
            logger.exception("My orders command error")
            await interaction.followup.send("Failed to list your orders.")

    # -------------------------
    # Admin commands
    # -------------------------
    @tree.command(
        name="freeze",
        description="Freeze a ticker (admin only)",
        guild=discord.Object(id=GUILD_ID) if GUILD_ID else None,
    )
    @app_commands.describe(ticker="Ticker symbol to freeze")
    async def freeze(interaction: discord.Interaction, ticker: str):
        if not _is_admin(interaction.user):
            await interaction.response.send_message("You are not authorized to run this command.", ephemeral=True)
            return
        await interaction.response.defer()
        try:
            from services.admin import freeze_ticker

            await asyncio.to_thread(freeze_ticker, ticker.upper())
            await interaction.followup.send(f"Ticker {ticker.upper()} frozen.")
        except Exception:
            logger.exception("Freeze command error")
            await interaction.followup.send("Failed to freeze ticker.")

    @tree.command(
        name="unfreeze",
        description="Unfreeze a ticker (admin only)",
        guild=discord.Object(id=GUILD_ID) if GUILD_ID else None,
    )
    @app_commands.describe(ticker="Ticker symbol to unfreeze")
    async def unfreeze(interaction: discord.Interaction, ticker: str):
        if not _is_admin(interaction.user):
            await interaction.response.send_message("You are not authorized to run this command.", ephemeral=True)
            return
        await interaction.response.defer()
        try:
            from services.admin import unfreeze_ticker

            await asyncio.to_thread(unfreeze_ticker, ticker.upper())
            await interaction.followup.send(f"Ticker {ticker.upper()} unfrozen.")
        except Exception:
            logger.exception("Unfreeze command error")
            await interaction.followup.send("Failed to unfreeze ticker.")

    @tree.command(
        name="force_verify",
        description="Force verify a deposit tx (admin only)",
        guild=discord.Object(id=GUILD_ID) if GUILD_ID else None,
    )
    @app_commands.describe(tx_id="Transaction ID", discord_id="Discord user id", game_name="Game name")
    async def force_verify(interaction: discord.Interaction, tx_id: str, discord_id: str, game_name: str):
        if not _is_admin(interaction.user):
            await interaction.response.send_message("You are not authorized to run this command.", ephemeral=True)
            return
        await interaction.response.defer()
        try:
            from services.admin import force_verify

            await asyncio.to_thread(force_verify, tx_id, discord_id, game_name)
            await interaction.followup.send(f"Force-verified tx {tx_id} for {discord_id} / {game_name}.")
        except Exception:
            logger.exception("Force verify command error")
            await interaction.followup.send("Failed to force-verify transaction.")

    @tree.command(
        name="export_withdrawals",
        description="Export pending withdrawals (admin only)",
        guild=discord.Object(id=GUILD_ID) if GUILD_ID else None,
    )
    async def export_withdrawals(interaction: discord.Interaction):
        if not _is_admin(interaction.user):
            await interaction.response.send_message("Not authorized.", ephemeral=True)
            return
        await interaction.response.defer()
        try:
            from services.exports import export_withdrawals_csv

            path = await asyncio.to_thread(export_withdrawals_csv)
            await interaction.followup.send(f"Withdrawals exported to `{path}`")
        except Exception:
            logger.exception("Export withdrawals command error")
            await interaction.followup.send("Failed to export withdrawals.")

    @tree.command(
        name="export_trades",
        description="Export trades CSV (admin only)",
        guild=discord.Object(id=GUILD_ID) if GUILD_ID else None,
    )
    @app_commands.describe(filename="Optional filename for export", limit="Max rows")
    async def export_trades(interaction: discord.Interaction, filename: Optional[str] = None, limit: int = 1000):
        if not _is_admin(interaction.user):
            await interaction.response.send_message("Not authorized.", ephemeral=True)
            return
        await interaction.response.defer()
        try:
            from services.admin import export_trades_admin

            path = await asyncio.to_thread(export_trades_admin, filename, limit)
            await interaction.followup.send(f"Trades exported to `{path}`")
        except Exception:
            logger.exception("Export trades command error")
            await interaction.followup.send("Failed to export trades.")

    @tree.command(
        name="audit",
        description="Query recent audit logs (admin only)",
        guild=discord.Object(id=GUILD_ID) if GUILD_ID else None,
    )
    @app_commands.describe(limit="Number of audit rows to show")
    async def audit(interaction: discord.Interaction, limit: int = 20):
        if not _is_admin(interaction.user):
            await interaction.response.send_message("Not authorized.", ephemeral=True)
            return
        await interaction.response.defer()
        try:
            with get_conn() as conn:
                cur = conn.execute("SELECT timestamp, actor, action, details FROM audit_logs ORDER BY timestamp DESC LIMIT ?", (limit,))
                rows = cur.fetchall()
            lines = [f"{r[0]} | {r[1]} | {r[2]} | {r[3]}" for r in rows]
            await interaction.followup.send("Recent audit logs:\n" + ("\n".join(lines) if lines else "No logs found."))
        except Exception:
            logger.exception("Audit command error")
            await interaction.followup.send("Failed to fetch audit logs.")
