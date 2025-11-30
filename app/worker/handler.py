# Обработчик сообщений из Kafka
import json
import logging
from typing import List
import uuid
from datetime import datetime
from aiokafka import AIOKafkaProducer
from app.services.clickhouse import clickhouse_service
from app.config.config import settings
logger = logging.getLogger(__name__)

class MessageHandler:
    def __init__(self):
        self.dlq_producer = None

    async def start(self):
        """Инициализация продюсера для DLQ"""
        self.dlq_producer = AIOKafkaProducer(
            bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
        )
        await self.dlq_producer.start()
        logger.info("DLQ Producer started successfully")

    async def stop(self):
        """Остановка продюсера"""
        if self.dlq_producer:
            await self.dlq_producer.stop()
            logger.info("DLQ Producer stopped")

    async def send_to_dlq(self, raw_message: bytes, error: str):
        """Отправка битого сообщения в Dead Letter Queue"""
        try:
            dlq_message = {
                "error": str(error),
                "original_message": raw_message.decode('utf-8', errors='replace'),
            }
            if self.dlq_producer:
                await self.dlq_producer.send_and_wait(
                    "events_dlq",
                    json.dumps(dlq_message).encode('utf-8')
                )
        except Exception as e:
            logger.critical(f'Failed to send to DLQ: {e}')


    async def process_batch(self, messages: List):
        """
        Принимает список сообщений, обрабатывает и пишет в БД.
        Ошибочные отправляет в DLQ.
        """
        if not messages:
            return
        
        clean_events = []

        for msg in messages:
            try:
                raw_value = msg.value
                event_data = json.loads(raw_value.decode('utf-8'))

                # Преобразование payload в строку для ClickHouse
                if isinstance(event_data.get('payload'), dict):
                    event_data['payload'] = json.dumps(event_data['payload'])
                
                # КОНВЕРТАЦИЯ event_id в uuid.UUID
                if 'event_id' in event_data and isinstance(event_data['event_id'], str):
                    try:
                        event_data['event_id'] = uuid.UUID(event_data['event_id'])
                    except ValueError:
                         raise ValueError(f"Invalid UUID: {event_data['event_id']}")
                
                # КОНВЕРТАЦИЯ timestamp в datetime
                if 'timestamp' in event_data and isinstance(event_data['timestamp'], str):
                    try:

                        event_data['timestamp'] = datetime.fromisoformat(event_data['timestamp'])
                    except ValueError:
                        raise ValueError(f"Invalid timestamp format: {event_data['timestamp']}")


                clean_events.append(event_data)
            except Exception as e:
                logger.error(f"Error processing message offset={msg.offset}: {e}")
                await self.send_to_dlq(msg.value, str(e))
                continue

        if clean_events:
            try:
                clickhouse_service.insert_events(clean_events)
            except Exception as e:
                logger.error(f"Failed to insert batch to ClickHouse: {e}")
                raise e
    
handler = MessageHandler()
