# --- Stage 1: Build and Test ---
FROM python:3.11-slim AS builder

WORKDIR /app

COPY requirements.txt .
COPY requirements_test.txt .
RUN pip install --no-cache-dir -r requirements.txt -r requirements_test.txt

COPY . .

RUN PYTHONPATH=. pytest tests/ -v && touch .tests_passed

# --- Stage 2: Production ---
FROM python:3.11-slim AS production

ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY api_server.py .

COPY --from=builder /app/.tests_passed /tmp/.tests_passed

CMD ["uvicorn", "api_server:app", "--host", "0.0.0.0", "--port", "8000"]