import os
from pydantic_settings import BaseSettings, SettingsConfigDict
from typing import Optional
from pathlib import Path # Import Path

# Determine the base directory (cypher-backend) relative to this config file
# config.py -> app -> cypher-backend
BASE_DIR = Path(__file__).resolve().parent.parent
ENV_FILE_PATH = BASE_DIR / '.env'

class Settings(BaseSettings):
    """Application settings using Pydantic BaseSettings."""
    # Load environment variables from .env file using the calculated path
    model_config = SettingsConfigDict(
        env_file=str(ENV_FILE_PATH), # Use the calculated path
        env_file_encoding='utf-8',
        extra='ignore'
    )

    # Application Name (NEW)
    APP_NAME: str = "Cypher Backend" # Added default app name

    # MongoDB Settings
    MONGO_URI: str = "mongodb://localhost:27017/"
    MONGO_DB_NAME: str = "cypher_paas"

    # JWT Settings
    SECRET_KEY: Optional[str] = None # Required for JWT, should be set in .env

    # GitHub OAuth Settings
    GITHUB_CLIENT_ID: Optional[str] = None
    GITHUB_CLIENT_SECRET: Optional[str] = None
    GITHUB_CALLBACK_URL: Optional[str] = "http://localhost:8000/auth/callback" # Default callback
    FRONTEND_URL: Optional[str] = "http://localhost:8080/dashboard"

    # GitHub Webhook Settings
    GITHUB_WEBHOOK_SECRET: Optional[str] = None # Required for webhook verification

    # Docker Registry Settings (Optional, if pushing images)
    DOCKER_REGISTRY_URL: Optional[str] = None
    DOCKER_REGISTRY_USERNAME: Optional[str] = None
    DOCKER_REGISTRY_PASSWORD: Optional[str] = None

    # Discord Notification Settings
    DISCORD_WEBHOOK_URL: Optional[str] = None # Add your Discord webhook URL here (e.g., in .env)

    # CORS Settings
    # Updated to include localhost:8080
    ALLOWED_ORIGINS: str = "http://localhost:5173,http://127.0.0.1:5173,http://localhost:8080"

    # Other settings
    # LOG_LEVEL: str = "INFO" # Example

    @property
    def cors_origins_list(self) -> list[str]:
        """Returns a list of allowed origins for CORS."""
        return [origin.strip() for origin in self.ALLOWED_ORIGINS.split(",") if origin]

# Instantiate settings
settings = Settings()

# Basic validation on startup
if not settings.SECRET_KEY:
    print("WARNING: SECRET_KEY is not set. JWT authentication will fail.")
    # In a real app, you might want to raise an exception here or exit.
# if not settings.GITHUB_WEBHOOK_SECRET:
#     print("WARNING: GITHUB_WEBHOOK_SECRET is not set. Webhook verification will fail.")
# if not settings.GITHUB_CLIENT_ID or not settings.GITHUB_CLIENT_SECRET:
#     print("WARNING: GITHUB_CLIENT_ID or GITHUB_CLIENT_SECRET not set. GitHub OAuth will fail.")

# Add a check to see if the .env file was actually found and loaded (optional but helpful)
if not ENV_FILE_PATH.is_file():
    print(f"WARNING: .env file not found at the expected location: {ENV_FILE_PATH}")
else:
    print(f"INFO: Loading environment variables from: {ENV_FILE_PATH}")
