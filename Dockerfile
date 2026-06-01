FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
RUN mkdir -p /app/data

COPY bot.py .
COPY mock_ledger ./mock_ledger

VOLUME ["/app/data"]

CMD ["python", "bot.py"]
