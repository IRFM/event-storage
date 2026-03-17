from pathlib import Path
from typing import Any, Dict, Optional

from pydantic import field_validator
from pydantic_settings import BaseSettings
from pydantic import ConfigDict


class Settings(BaseSettings):
    """Application settings."""

    model_config = ConfigDict(
        case_sensitive=True,
        env_file=(str(Path.home() / ".env.default"), ".env"),
        extra="ignore",
    )

    MYSQL_HOST: Optional[str] = None
    MYSQL_DATABASE: Optional[str] = None
    MYSQL_USER: Optional[str] = None
    MYSQL_PASSWORD: Optional[str] = None

    # SQLITE=true enables SQLite mode explicitly. If unset, defaults to True
    # only when no MySQL config is present, to avoid silent fallback.
    SQLITE: bool = False
    SQLITE_DATABASE_FILE: str = "database.db"

    DATABASE_URI: Optional[str] = None

    @field_validator("DATABASE_URI", mode="before")
    def assemble_db_connection(cls, v: Optional[str], values: Dict[str, Any]) -> Any:
        if isinstance(v, str):
            return v
        data = values.data
        if data.get("SQLITE", False):
            return f"sqlite:///{data.get('SQLITE_DATABASE_FILE')}"

        return (
            f"mysql+pymysql://{data['MYSQL_USER']}:{data['MYSQL_PASSWORD']}"
            f"@{data['MYSQL_HOST']}/{data['MYSQL_DATABASE']}"
        )


settings = Settings()
