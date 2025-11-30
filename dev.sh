#!/bin/bash

# Настройки для подключения к сервисам в Docker с хоста (localhost)
export KAFKA_BOOTSTRAP_SERVERS=localhost:9092
export CLICKHOUSE_HOST=localhost
export CLICKHOUSE_PORT=8123
export CLICKHOUSE_DATABASE=events_db

# Запуск FastAPI сервиса
uvicorn app.main:app --reload --host 0.0.0.0 --port 8080