from re import L
from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    #App
    PROJECT_NAME: str = "Events Service"
    DEBUG: bool = True
    LOG_LEVEL: str = "INFO"

    #Kafka
    KAFKA_BOOTSTRAP_SERVERS: str = "localhost:9092"
    KAFKA_TOPIC: str = "events_topic"
    CLICKHOUSE_HOST: str = "localhost"
    CLICKHOUSE_PORT: int = 8123
    CLICKHOUSE_DATABASE: str = "events_db"
    GROUP_ID: str = "events_consumer_group"


    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"

settings = Settings()