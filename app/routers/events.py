from fastapi import APIRouter, HTTPException, status
from app.models.event import Event
import logging
from app.services.kafka import kafka_service
from app.config.config import settings

router = APIRouter()
logger = logging.getLogger(__name__)


@router.post("/", status_code=status.HTTP_202_ACCEPTED)
async def create_event(event: Event):
    """
    Принимает событие и отправляет его в Kafka.
    """

    try:
        await kafka_service.send_event(
            topic=settings.KAFKA_TOPIC,
            event=event.model_dump(mode="json")
        )
        return {"message": "queued", "event_id": event.event_id}
    except Exception as e:
        logger.error(f"Error sending event to Kafka: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error sending event to Kafka: {e}"
        )