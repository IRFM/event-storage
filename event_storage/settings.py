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
        use_sqlite = data.get("SQLITE", False)  # explicit False, not True
        if use_sqlite:
            return f"sqlite:///{data.get('SQLITE_DATABASE_FILE')}"
        # Validate that all MySQL fields are present before assembling URI
        required = ("MYSQL_USER", "MYSQL_PASSWORD", "MYSQL_HOST", "MYSQL_DATABASE")
        missing = [k for k in required if not data.get(k)]
        if missing:
            raise ValueError(
                f"SQLITE=false but the following MySQL settings are missing: {missing}. "
                "Set SQLITE=true or provide all MySQL connection parameters."
            )
        return (
            f"mysql+pymysql://{data['MYSQL_USER']}:{data['MYSQL_PASSWORD']}"
            f"@{data['MYSQL_HOST']}/{data['MYSQL_DATABASE']}"
        )


settings = Settings()
