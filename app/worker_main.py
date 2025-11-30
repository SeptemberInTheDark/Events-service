import asyncio
import logging
from aiokafka import AIOKafkaConsumer
from app.config.config import settings
from app.services.clickhouse import clickhouse_service
from app.worker.handler import handler

logging.basicConfig(level=settings.LOG_LEVEL)
logger = logging.getLogger(__name__)

async def consume():
    clickhouse_service.connect()

    await handler.start()

    consumer = AIOKafkaConsumer(
        settings.KAFKA_TOPIC,
        bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
        group_id=settings.GROUP_ID,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
    )

    await consumer.start()
    logger.info("Worker started consuming...")

    try:
        while True:
            result = await consumer.getmany(timeout_ms=1000, max_records=1000)

            for tp, messages in result.items():
                await handler.procces_batch(messages)

                await consumer.commit()

    except Exception as e:
        logger.error(f"Error in consumer: {e}")
    finally:
        await consumer.stop()
        await handler.stop()
        logger.info("Worker stopped consuming...")

