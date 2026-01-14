from __future__ import annotations

from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    db_path: Path = Field(
        default=Path("data/situation-monitor.db"), validation_alias="DB_PATH"
    )

    map_tile_url: str = Field(
        default="https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png",
        validation_alias="MAP_TILE_URL",
    )
    user_agent: str = Field(
        default="situation-monitor/0.1", validation_alias="USER_AGENT"
    )

    firms_api_key: str | None = Field(default=None, validation_alias="FIRMS_API_KEY")
    nvd_api_key: str | None = Field(default=None, validation_alias="NVD_API_KEY")

    mastodon_instances: str = Field(default="", validation_alias="MASTODON_INSTANCES")
    mastodon_tags: str = Field(
        default="#earthquake,#wildfire,#flood,#tsunami,#storm,#breaking,#OSINT",
        validation_alias="MASTODON_TAGS",
    )

    bluesky_handle: str | None = Field(default=None, validation_alias="BLUESKY_HANDLE")
    bluesky_app_password: str | None = Field(
        default=None, validation_alias="BLUESKY_APP_PASSWORD"
    )

    items_retention_days: int = Field(
        default=30, validation_alias="ITEMS_RETENTION_DAYS"
    )
    incidents_retention_days: int = Field(
        default=90, validation_alias="INCIDENTS_RETENTION_DAYS"
    )
