# Сборщик стакана: только Python и asyncpg, без ccxt - лёгкий контейнер (~40 МБ памяти).
FROM python:3.12-slim
WORKDIR /app
RUN pip install --no-cache-dir asyncpg==0.31.0
COPY book.py bookcheck.py ./
CMD ["python", "-u", "book.py"]
