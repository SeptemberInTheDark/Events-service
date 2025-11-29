from aiokafka import AIOKafkaProducer
import json
from app.config.config import settings
import logging

logger = logging.getLogger(__name__)

class KafkaService:
    def __init__(self):
        self.producer = None

    
    async def start(self):
        self.producer = AIOKafkaProducer(
            bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )

        await self.producer.start()
        logger.info("Kafka Producer started successfully")

    async def stop(self):
        if self.producer:
            await self.producer.stop()
            logger.info("Kafka Producer stopped")

    async def send_event(self, topic: str, event: dict):
        if not self.producer:
            raise RuntimeError("Kafka producer is not started")
        await self.producer.send_and_wait(topic, event)

kafka_service = KafkaService()