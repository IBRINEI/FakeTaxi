# Stage 1: Build and Test
FROM python:3.11-slim AS builder

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt pytest pytest-mock

COPY . .

RUN pytest tests/ -v && touch .tests_passed

# Stage 2: Production Image (Minimal)
FROM python:3.11-slim AS production

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY minio_lake_writer.py .

COPY --from=builder /app/.tests_passed /tmp/.tests_passed

CMD ["python", "minio_lake_writer.py"]