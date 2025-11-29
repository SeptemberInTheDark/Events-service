import logging
from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.config.config import settings
from app.routers import events
from app.services.kafka import kafka_service

logging.basicConfig(
    level=settings.LOG_LEVEL,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Starting up Events Service...")

    # Инициализация и старт Kafka Producer
    await kafka_service.start()

    yield

    #завершение работы Kafka Producer
    logger.info("Shutting down Events Service...")
    await kafka_service.stop()

def create_app() -> FastAPI:
    app = FastAPI(
        title=settings.PROJECT_NAME,
        description="API for events service",
        version="1.0.0",
       docs_url="/docs" if settings.DEBUG else None,
       redoc_url="/redoc" if settings.DEBUG else None,
       lifespan=lifespan
    )

    # Настройка CORS
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"], #develop mode
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    app.include_router(events.router, prefix="/api/v1/events", tags=["events"])

    @app.get("/health")
    async def health_check():
        """Проверка работоспособности сервиса"""
        return {"status": "ok"}
    return app


app = create_app()

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "app.main:app",
        host="0.0.0.0",
        port=8000,
        reload=settings.DEBUG
    )