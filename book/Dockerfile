# Сборщик стакана: Python, asyncpg и aiohttp (вебсокет стакана Binance perp), без ccxt - лёгкий контейнер.
FROM python:3.12-slim
WORKDIR /app
RUN pip install --no-cache-dir asyncpg==0.31.0 aiohttp==3.14.5
COPY book.py bookcheck.py ./
CMD ["python", "-u", "book.py"]
