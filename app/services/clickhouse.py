import clickhouse_connect
from app.config.config import settings
import logging
from typing import List, Dict

logger = logging.getLogger(__name__)


class ClickHouseService:
    def __init__(self):
        self.client = None

    def connect(self):
        try:
            tmp_client = clickhouse_connect.get_client(
                host=settings.CLICKHOUSE_HOST,
                port=settings.CLICKHOUSE_PORT
            )
            # Создаем базу данных
            tmp_client.command(f"CREATE DATABASE IF NOT EXISTS {settings.CLICKHOUSE_DATABASE}")
            tmp_client.close()

            self.client = clickhouse_connect.get_client(
                host=settings.CLICKHOUSE_HOST,
                port=settings.CLICKHOUSE_PORT,
                database=settings.CLICKHOUSE_DATABASE,
            )
            
            # Создаем таблицу
            self._create_table()
            logger.info("Connected to ClickHouse")
        except Exception as e:
            logger.error(f"Error connecting to ClickHouse: {e}")
            raise RuntimeError(f"Error connecting to ClickHouse: {e}")


    def _create_table(self):
        schema = """
            CREATE TABLE IF NOT EXISTS events (
                event_id UUID,
                event_type LowCardinality(String),
                user_id Nullable(String),
                timestamp DateTime64(3),
                payload String,
                created_at DateTime DEFAULT now()
            )
            ENGINE = MergeTree()
            PARTITION BY toYYYYMM(timestamp)
            ORDER BY (event_type, timestamp);
        """
        self.client.command(schema)

    
    def insert_events(self, events: List[Dict]):
        """Вставляет пачку событий"""
        if not events:
            return

        try:
            column_names = list(events[0].keys())
            data = []
            for event in events:
                row = [event[col] for col in column_names]
                data.append(row)

            self.client.insert(
                table="events",
                data=data,
                column_names=column_names
            )
            logger.info(f"Inserted {len(events)} events into ClickHouse")
        except Exception as e:
            logger.error(f'Failed to insert events into clickhouse: {e}')
            raise

clickhouse_service = ClickHouseService()
