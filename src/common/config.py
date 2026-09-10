"""Application configuration using Pydantic Settings and YAML configuration."""

import os
from functools import lru_cache
from pathlib import Path
from typing import Any

import yaml
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


def get_env_files() -> tuple[str, ...]:
    """Dynamically resolve .env files based on the ENVIRONMENT setting."""
    env = os.getenv("ENVIRONMENT", os.getenv("ENV", "local")).lower()
    env_variants = [env]
    if env in ("dev", "development"):
        env_variants = ["dev", "development"]
    elif env in ("prod", "production"):
        env_variants = ["prod", "production"]
    elif env in ("local", "test", "testing"):
        env_variants = ["local"]

    files = [".env"]
    for variant in env_variants:
        files.append(f".env.{variant}")
        files.append(f".env.{variant}.local")
    if ".env.local" not in files:
        files.append(".env.local")

    return tuple(files)


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=get_env_files(),
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # Environment
    ENVIRONMENT: str = Field(default="development")
    LOG_LEVEL: str = Field(default="INFO")

    # Aiven Kafka
    KAFKA_BOOTSTRAP_SERVERS: str = Field(default="localhost:9092")
    KAFKA_SECURITY_PROTOCOL: str = Field(default="PLAINTEXT")
    KAFKA_SASL_MECHANISM: str = Field(default="PLAIN")
    KAFKA_USER: str | None = Field(default=None)
    KAFKA_PASSWORD: str | None = Field(default=None)
    KAFKA_CA_CERT_PATH: str | None = Field(default=None)
    KAFKA_TOPIC_RAW: str = Field(default="fleet.telemetry.raw")

    # Databricks
    DATABRICKS_HOST: str = Field(default="https://localhost")
    DATABRICKS_TOKEN: str | None = Field(default=None)
    DATABRICKS_HTTP_PATH: str = Field(default="/sql/1.0/warehouses/default")
    DATABRICKS_CATALOG: str = Field(default="fleet_iot")
    DATABRICKS_SCHEMA: str = Field(default="telemetry")

    # SLA & Pipeline
    SLA_MAX_LATENCY_SECONDS: int = Field(default=120)
    WATERMARK_DURATION_MINUTES: int = Field(default=15)
    CHECKPOINT_BASE_PATH: str = Field(default="/Volumes/fleet_iot/telemetry/checkpoints")

    # Local Pipeline & SQLite
    DATABASE_BACKEND: str = Field(default="databricks")
    SQLITE_DB_PATH: str = Field(default="data/fleet_local.db")
    LOCAL_CHECKPOINT_DIR: str = Field(default="data/checkpoints")
    LOCAL_LAKEHOUSE_DIR: str = Field(default="data/lakehouse")
    JAVA_HOME: str | None = Field(default=None)
    HADOOP_HOME: str | None = Field(default=None)


def load_yaml_config(config_path: str | Path = "config/pipeline_config.yaml") -> dict[str, Any]:
    """Load configuration dictionary from YAML file."""
    path = Path(config_path)
    if not path.is_file():
        return {}
    with open(path, encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


@lru_cache
def get_settings() -> Settings:
    """Cached settings singleton."""
    return Settings()
