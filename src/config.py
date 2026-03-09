from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv


load_dotenv()


@dataclass(frozen=True)
class Settings:
    log_level: str = os.getenv("LOG_LEVEL", "INFO")
    kafka_bootstrap_servers: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    kafka_topic: str = os.getenv("KAFKA_TOPIC", "telemetry")
    raw_data_dir: Path = Path(os.getenv("RAW_DATA_DIR", "./data/raw"))
    bronze_dir: Path = Path(os.getenv("BRONZE_DIR", "./data/bronze/telemetry"))
    silver_dir: Path = Path(os.getenv("SILVER_DIR", "./data/silver/telemetry"))
    gold_dir: Path = Path(os.getenv("GOLD_DIR", "./data/gold"))
    checkpoint_dir: Path = Path(os.getenv("CHECKPOINT_DIR", "./checkpoints"))
    postgres_host: str = os.getenv("POSTGRES_HOST", "localhost")
    postgres_port: int = int(os.getenv("POSTGRES_PORT", "5432"))
    postgres_db: str = os.getenv("POSTGRES_DB", "oilgas")
    postgres_user: str = os.getenv("POSTGRES_USER", "oilgas")
    postgres_password: str = os.getenv("POSTGRES_PASSWORD", "oilgas")
    postgres_table: str = os.getenv("POSTGRES_TABLE", "kpis_by_asset_day")
    api_host: str = os.getenv("API_HOST", "0.0.0.0")
    api_port: int = int(os.getenv("API_PORT", "4000"))
    api_base_url: str = os.getenv("API_BASE_URL", "http://localhost:4000")
    telemetry_interval_sec: int = int(os.getenv("TELEMETRY_INTERVAL_SEC", "2"))
    asset_count: int = int(os.getenv("ASSET_COUNT", "5"))

    @property
    def postgres_url(self) -> str:
        return (
            f"postgresql+psycopg2://{self.postgres_user}:{self.postgres_password}"
            f"@{self.postgres_host}:{self.postgres_port}/{self.postgres_db}"
        )


settings = Settings()
