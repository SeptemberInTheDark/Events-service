import asyncio
import logging
from aiokafka import AIOKafkaConsumer
from app.config.config import settings
from app.services.clickhouse import clickhouse_service
from app.worker.handler import handler

logging.basicConfig(level=settings.LOG_LEVEL)
logger = logging.getLogger(__name__)

async def consume():

    try:
        clickhouse_service.connect()
    except Exception as e:
        logger.error(f"Failed to connect to ClickHouse: {e}")
        return # Выходим, если базы нет, Docker перезапустит контейнер

    await handler.start()


    topic_name = settings.KAFKA_TOPIC

    consumer = AIOKafkaConsumer(
        topic_name,
        bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
        group_id=settings.GROUP_ID,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
    )

    await consumer.start()
    logger.info(f"Worker started consuming from {topic_name}...")

    try:
        while True:
            result = await consumer.getmany(timeout_ms=1000, max_records=1000)

            for tp, messages in result.items():
                if messages:
                    logger.info(f"Received {len(messages)} messages")
                    # Исправил опечатку procces -> process
                    await handler.process_batch(messages)
                    await consumer.commit()

    except Exception as e:
        logger.error(f"Error in consumer: {e}")
    finally:
        await consumer.stop()
        await handler.stop()
        logger.info("Worker stopped consuming...")

if __name__ == "__main__":
    asyncio.run(consume())
