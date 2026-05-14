from pathlib import Path
from typing import Optional
import warnings

from pydantic import ValidationInfo, model_validator, field_validator
from pydantic_settings import BaseSettings
from pydantic_settings import SettingsConfigDict


class Settings(BaseSettings):
    """Application settings."""

    model_config = SettingsConfigDict(
        case_sensitive=True,
        env_file=(".env.example", str(Path.home() / ".env.default"), ".env"),
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

    @model_validator(mode="before")
    @classmethod
    def warn_if_only_example_env_exists(cls, data):
        env_files = cls.model_config["env_file"]

        existing = [
            Path(path).expanduser()
            for path in env_files
            if Path(path).expanduser().is_file()
        ]

        if [path.name for path in existing] == [".env.example"]:
            warnings.warn(
                f"Only the example dotenv file ({existing[0].resolve()}) was found. "
                "If matching environment variables are set, they take precedence and "
                ".env.example may not affect the active configuration. To avoid "
                "ambiguity, create a real .env or ~/.env.default file, or remove "
                ".env.example.",
                RuntimeWarning,
            )

        return data

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
