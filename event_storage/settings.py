from pathlib import Path
from typing import Optional

from pydantic import ValidationInfo, field_validator
from pydantic_settings import BaseSettings
from pydantic_settings import SettingsConfigDict


class Settings(BaseSettings):
    """Application settings."""

    model_config = SettingsConfigDict(
        case_sensitive=True,
        env_file=(str(Path.home() / ".env.default"), ".env"),
        extra="ignore",
    )

    MYSQL_HOST: Optional[str] = None
    MYSQL_DATABASE: Optional[str] = None
    MYSQL_USER: Optional[str] = None
    MYSQL_PASSWORD: Optional[str] = None

    # SQLITE=true enables SQLite mode explicitly
    # When SQLITE=false, MYSQL_* settings must be provided unless DATABASE_URI is set
    SQLITE: bool = False
    SQLITE_DATABASE_FILE: str = "database.db"
    DATABASE_URI: Optional[str] = None

    @field_validator("DATABASE_URI", mode="before")
    def assemble_db_connection(
        cls,
        database_uri: Optional[str],
        info: ValidationInfo,
    ) -> str:
        # If DATABASE_URI was provided directly, it is the source of truth
        if database_uri:
            return database_uri

        data = info.data

        # Otherwise, build the connection string from the selected backend settings
        if data.get("SQLITE"):
            sqlite_file = data.get("SQLITE_DATABASE_FILE")
            return f"sqlite:///{sqlite_file}"

        # In MySQL mode, all MYSQL_* settings are required to avoid producing
        # an invalid URI such as mysql+pymysql://None:None@None/None
        required_mysql_settings = [
            "MYSQL_HOST",
            "MYSQL_DATABASE",
            "MYSQL_USER",
            "MYSQL_PASSWORD",
        ]

        missing = [name for name in required_mysql_settings if not data.get(name)]

        if missing:
            raise ValueError(
                "MySQL configuration is required when SQLITE=false. "
                f"Missing settings: {', '.join(missing)}"
            )

        return (
            f"mysql+pymysql://{data['MYSQL_USER']}:{data['MYSQL_PASSWORD']}"
            f"@{data['MYSQL_HOST']}/{data['MYSQL_DATABASE']}"
        )


settings = Settings()
