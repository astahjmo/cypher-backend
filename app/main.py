import asyncio
from fastapi import FastAPI
from fastapi.middleware import Middleware
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
import logging

from config import settings
from services.db_service import (
    connect_to_mongo,
    close_mongo_connection,
    get_database,
    db_service,
)
from views import auth as auth_views
from views import github as github_views
from views import webhooks as webhooks_views
from views import build as build_views
from views import containers as containers_views
from controllers.containers import broadcast_status_updates
from repositories.repository_config_repository import RepositoryConfigRepository
from models.auth.db_models import User

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

background_task = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global background_task
    logger.info("Application startup...")
    await connect_to_mongo()

    db = db_service.get_db()
    if db is None:
        logger.critical(
            "Database connection failed on startup. Aborting background task setup."
        )
        yield
        logger.info("Application shutdown...")
        await close_mongo_connection()
        return

    repo_config_repo = RepositoryConfigRepository(db=db)

    logger.info("Starting WebSocket status broadcast task...")
    background_task = asyncio.create_task(
        broadcast_status_updates(repo_config_repo=repo_config_repo)
    )

    yield

    logger.info("Application shutdown...")
    if background_task:
        logger.info("Cancelling WebSocket status broadcast task...")
        background_task.cancel()
        try:
            await background_task
        except asyncio.CancelledError:
            logger.info("WebSocket broadcast task successfully cancelled.")
        except Exception as e:
            logger.error(
                f"Error during background task cancellation: {e}", exc_info=True
            )

    await close_mongo_connection()


middleware = [
    Middleware(
        CORSMiddleware,
        allow_origins=settings.cors_origins_list,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    ),
]

app = FastAPI(
    title=settings.APP_NAME,
    description="Backend API for PaaS platform integrating GitHub, Docker, and Kubernetes.",
    version="0.1.0",
    lifespan=lifespan,
    middleware=middleware,
)

app.include_router(auth_views.router, prefix="/auth", tags=["Authentication"])
app.include_router(
    github_views.router, prefix="/repositories", tags=["GitHub Repositories"]
)
app.include_router(build_views.router, prefix="/build", tags=["Builds"])
app.include_router(webhooks_views.router, prefix="/webhooks", tags=["Webhooks"])
app.include_router(containers_views.router)
