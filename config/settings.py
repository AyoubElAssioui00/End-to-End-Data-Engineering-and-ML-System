from pydantic_settings import BaseSettings, SettingsConfigDict
from typing import List


class Settings(BaseSettings):
    """Application settings"""
    
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=True
    )
    
    # Kafka Configuration
    KAFKA_BOOTSTRAP_SERVERS: str = "localhost:9092"
    KAFKA_CONSUMER_GROUP_PREFIX: str = "network-anomaly"
    KAFKA_SEND_TIMEOUT: int = 10  # seconds, adjust as needed
    
    # Data Configuration
    DATA_PATH: str = "./data/network_traffic.csv"
    SIMULATION_DELAY: float = 0.1  # seconds between records
    
    # Streaming Configuration
    BATCH_SIZE: int = 100
    BATCH_TIMEOUT_MS: int = 5000
    
    # MLFlow Configuration
    MLFLOW_TRACKING_URI: str = "http://localhost:5000"
    MLFLOW_MODEL_NAME: str = "network-anomaly-detector"
    MLFLOW_ARTIFACT_ROOT: str = "file://./mlflow/artifacts"
    MLFLOW_MODEL_STAGE: str = "Production"  # Production, Staging, None
    
    # PySpark Configuration
    SPARK_APP_NAME: str = "NetworkAnomalyDetector"
    SPARK_MASTER: str = "local[*]"
    
    # Streamlit Dashboard
    DASHBOARD_REFRESH_INTERVAL: int = 5  # seconds
    DASHBOARD_MAX_RECORDS: int = 1000
    
    # Logging
    LOG_LEVEL: str = "INFO"


settings = Settings()