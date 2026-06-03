FROM python:3.11-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy bot entrypoint
COPY bot.py .

# Copy your project folder (with space in name)
COPY "main code/" "/app/main code/"

# Create data directory for SQLite if needed
RUN mkdir -p /app/data

# Start the bot
CMD ["python", "bot.py"]
