FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY bot.py .
COPY main_code/ ./main_code/

RUN mkdir -p /app/data

ENV PYTHONPATH=/app/main_code:$PYTHONPATH

CMD ["python", "bot.py"]
