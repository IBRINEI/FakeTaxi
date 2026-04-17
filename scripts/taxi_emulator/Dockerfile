# --- Stage 1: Build and Test ---
FROM python:3.11-slim AS builder

WORKDIR /app
ENV PYTHONPATH=/app

COPY requirements.txt requirements_test.txt ./
RUN pip install --no-cache-dir -r requirements.txt -r requirements_test.txt

COPY . .

RUN pytest tests/ -v && touch .tests_passed

# --- Stage 2: Production ---
FROM python:3.11-slim AS production

ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONPATH=/app

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY taxi_emulator.py .

COPY --from=builder /app/.tests_passed /tmp/.tests_passed

CMD ["python", "taxi_emulator.py"]