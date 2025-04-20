from pydantic_settings import BaseSettings, SettingsConfigDict
import os

class Settings(BaseSettings):
    # Application settings
    APP_NAME: str = "Cypher Backend"
    DEBUG: bool = False
    FRONTEND_REDIRECT_URL: str | None =  "localhost:8080/dashboard"# URL to redirect to after successful login

    # MongoDB settings
    MONGO_URI: str
    MONGO_DB_NAME: str = "cypher_paas"
    MONGO_ROOT_USER: str | None = None # For docker-compose init
    MONGO_ROOT_PASSWORD: str | None = None # For docker-compose init

    # GitHub OAuth settings
    GITHUB_CLIENT_ID: str
    GITHUB_CLIENT_SECRET: str
    GITHUB_CALLBACK_URL: str = "http://localhost:8000/auth/callback"
    SECRET_KEY: str # Needed for secure session management (TODO)

    # GitHub Webhook settings
    GITHUB_WEBHOOK_SECRET: str | None = None # Optional, but recommended

    # Kubernetes settings (adjust as needed)
    KUBECONFIG_PATH: str | None = None # Path to kubeconfig file if not running in-cluster
    K8S_NAMESPACE: str = "default" # Namespace for build pods

    # Docker Registry settings (e.g., GitHub Container Registry)
    REGISTRY_URL: str # e.g., ghcr.io
    REGISTRY_USER: str # e.g., GitHub username or PAT
    REGISTRY_PASSWORD: str # e.g., GitHub PAT

    # Load .env file if it exists, especially for local development
    model_config = SettingsConfigDict(env_file='.env', extra='ignore')

# Create a single instance of the settings to be imported elsewhere
settings = Settings() # type: ignore # Ignore missing arguments error, loaded from env
