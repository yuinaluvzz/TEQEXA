# TEQEXA
# TEQEXA

This bot stores its SQLite database at `./data/market.db` by default.

## Persistent storage

- `DB_PATH` defaults to `./data/market.db`.
- The bot will create the `./data` directory automatically.
- For Docker deployments, mount a host volume to keep the database across container restarts.

Example:

```bash
docker run -v /host/path/teqexa-data:/app/data your-image-name
```

For Railway, use the mounted `/data` volume and ensure `DB_PATH` is set to `/data/market.db` if you override environment variables.

Also make sure `MARKET_CHANNEL_ID` is the numeric Discord channel ID and that the bot has `View Channel`, `Send Messages`, and `Embed Links` permissions in that channel.

## Requirements

- `discord.py`
- `aiohttp`
- `python-dotenv`
