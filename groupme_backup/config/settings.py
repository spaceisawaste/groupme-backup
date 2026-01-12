"""Configuration settings using Pydantic."""

from typing import List, Optional
from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Application settings loaded from environment variables."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    # GroupMe API Configuration
    groupme_access_token: str = Field(..., description="GroupMe API access token")
    groupme_api_base_url: str = Field(
        default="https://api.groupme.com/v3", description="GroupMe API base URL"
    )
    groupme_rate_limit_calls: int = Field(
        default=100, description="Maximum API calls per rate limit period"
    )
    groupme_rate_limit_period: int = Field(
        default=60, description="Rate limit period in seconds"
    )

    # Database Configuration
    db_host: str = Field(default="localhost", description="PostgreSQL host")
    db_port: int = Field(default=5432, description="PostgreSQL port")
    db_name: str = Field(default="groupme_backup", description="Database name")
    db_user: str = Field(..., description="Database user")
    db_password: str = Field(..., description="Database password")

    # Sync Settings
    sync_batch_size: int = Field(
        default=100, ge=1, le=100, description="Messages per API request (max 100)"
    )
    sync_max_retries: int = Field(
        default=3, ge=0, description="Maximum retry attempts for failed requests"
    )
    sync_retry_delay: int = Field(
        default=5, ge=1, description="Initial delay between retries in seconds"
    )

    # Optional: Specific groups to backup
    backup_group_ids: List[str] = Field(
        default_factory=list, description="Specific group IDs to backup (empty = all)"
    )

    @field_validator("backup_group_ids", mode="before")
    @classmethod
    def parse_group_ids(cls, v: Optional[str | List[str]]) -> List[str]:
        """Parse comma-separated group IDs from environment variable."""
        if not v:
            return []
        if isinstance(v, list):
            return v
        if isinstance(v, str):
            return [gid.strip() for gid in v.split(",") if gid.strip()]
        return []

    @property
    def database_url(self) -> str:
        """Construct PostgreSQL database URL."""
        return (
            f"postgresql://{self.db_user}:{self.db_password}"
            f"@{self.db_host}:{self.db_port}/{self.db_name}"
        )

    @property
    def database_url_async(self) -> str:
        """Construct async PostgreSQL database URL."""
        return (
            f"postgresql+asyncpg://{self.db_user}:{self.db_password}"
            f"@{self.db_host}:{self.db_port}/{self.db_name}"
        )


# Global settings instance
_settings: Optional[Settings] = None


def get_settings() -> Settings:
    """Get or create the global settings instance."""
    global _settings
    if _settings is None:
        _settings = Settings()  # type: ignore
    return _settings
