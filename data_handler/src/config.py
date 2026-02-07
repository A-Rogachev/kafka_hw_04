from functools import lru_cache
from pathlib import Path

from pydantic import BaseModel, Field, PostgresDsn
from pydantic_settings import BaseSettings, SettingsConfigDict

CONFIG_DIR = Path(__file__).resolve().parents[2]


class PostgresSettings(BaseModel):
    class PostgresPoolSettings(BaseModel):
        """
        Настройки пула подключений.
        """

        min_size: int = Field(default=1)
        max_size: int = Field(default=1)
        max_inactive_connection_lifetime: int = Field(default=300)

    pool: PostgresPoolSettings = PostgresPoolSettings()
    dsn: PostgresDsn = Field(...)

    @property
    def pool_config(self) -> dict[str, int]:
        return self.pool.model_dump()


class BrokerSettings(BaseModel):
    bootstrap_servers: str = Field(...)
    topic_names: list[str] = Field(...)
    consumer_group_id: str = Field(default="data-reader")


class Settings(BaseSettings):
    """Настройки приложения."""

    postgres: PostgresSettings
    broker: BrokerSettings

    model_config = SettingsConfigDict(
        case_sensitive=False,
        env_file=str(CONFIG_DIR / "env.example"),
        env_prefix="DATA_APP_",
        env_nested_delimiter="__",
        extra="ignore",
    )

    @property
    def postgres_dsn(self) -> str:
        return str(self.postgres.dsn)


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()  # type: ignore
