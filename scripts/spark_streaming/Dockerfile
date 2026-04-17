# --- Stage 1: Build and Test ---
FROM python:3.11-slim AS builder

WORKDIR /app

ENV PYTHONPATH=/app

RUN apt-get update && \
    apt-get install -y --no-install-recommends default-jre-headless && \
    rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/usr/lib/jvm/default-java

COPY requirements.txt requirements_test.txt ./
RUN pip install --no-cache-dir -r requirements.txt -r requirements_test.txt

COPY . .

RUN pytest tests/ -v && touch .tests_passed

# --- Stage 2: Production ---
FROM python:3.11-slim AS production

ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1

WORKDIR /app

RUN apt-get update && \
    apt-get install -y --no-install-recommends default-jre-headless && \
    rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/usr/lib/jvm/default-java

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY spark_streaming.py .

COPY --from=builder /app/.tests_passed /tmp/.tests_passed

CMD ["python", "spark_streaming.py"]