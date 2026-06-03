# inside discord_commands.py register(bot) function (or paste into file and adapt)
from decimal import Decimal
from database import get_conn
from stocks import get_ticker_price, get_leaderboard, get_price_history

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
