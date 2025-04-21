import os
from pydantic_settings import BaseSettings, SettingsConfigDict
from typing import Optional
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
ENV_FILE_PATH = BASE_DIR / ".env"


class Settings(BaseSettings):
    """Application settings using Pydantic BaseSettings."""

    model_config = SettingsConfigDict(
        env_file=str(ENV_FILE_PATH), env_file_encoding="utf-8", extra="ignore"
    )

    APP_NAME: str = "Cypher Backend"

    MONGO_URI: str = "mongodb://localhost:27017/"
    MONGO_DB_NAME: str = "cypher_paas"

    SECRET_KEY: Optional[str] = None

    GITHUB_CLIENT_ID: Optional[str] = None
    GITHUB_CLIENT_SECRET: Optional[str] = None
    GITHUB_CALLBACK_URL: Optional[str] = "http://localhost:8000/auth/callback"
    FRONTEND_URL: Optional[str] = "http://localhost:8080/dashboard"

    GITHUB_WEBHOOK_SECRET: Optional[str] = None

    DOCKER_REGISTRY_URL: Optional[str] = None
    DOCKER_REGISTRY_USERNAME: Optional[str] = None
    DOCKER_REGISTRY_PASSWORD: Optional[str] = None

    DISCORD_WEBHOOK_URL: Optional[str] = None

    ALLOWED_ORIGINS: str = (
        "http://localhost:5173,http://127.0.0.1:5173,http://localhost:8080"
    )

    @property
    def cors_origins_list(self) -> list[str]:
        """Returns a list of allowed origins for CORS."""
        return [origin.strip() for origin in self.ALLOWED_ORIGINS.split(",") if origin]


settings = Settings()

if not settings.SECRET_KEY:
    print("WARNING: SECRET_KEY is not set. JWT authentication will fail.")


if not ENV_FILE_PATH.is_file():
    print(f"WARNING: .env file not found at the expected location: {ENV_FILE_PATH}")
else:
    print(f"INFO: Loading environment variables from: {ENV_FILE_PATH}")
