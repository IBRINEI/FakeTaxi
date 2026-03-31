FROM apache/airflow:2.8.1-python3.9

USER root

# Устанавливаем дефолтную Java (OpenJDK 17)
RUN apt-get update && \
    apt-get install -y default-jre-headless && \
    apt-get clean

USER airflow