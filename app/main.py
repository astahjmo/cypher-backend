from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware # Import CORS Middleware
from contextlib import asynccontextmanager
from config import settings
from services.db_service import connect_to_mongo, close_mongo_connection
# Import routers from the 'views' directory
from views import auth, github, build, webhooks # Ensure 'build' is imported from views
import logging
# Removed datetime, timezone, jsonable_encoder, JSONResponse, json, Any imports as they are no longer needed here

# Configure basic logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Removed Custom JSON Encoder and CustomJSONResponse class

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Code to run on startup
    logger.info("Application startup...")
    await connect_to_mongo()
    yield
    # Code to run on shutdown
    logger.info("Application shutdown...")
    await close_mongo_connection()

app = FastAPI(
    title=settings.APP_NAME,
    description="Backend API for PaaS platform integrating GitHub, Docker, and Kubernetes.",
    version="0.1.0",
    lifespan=lifespan # Register the lifespan context manager
    # Removed default_response_class=CustomJSONResponse
)

# Define allowed origins
origins = [
    "http://localhost:8080", # Frontend development server
    # "https://your-production-frontend.com", # Example production URL
]

# Add CORS middleware with specific origins
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins, # Use the specific list of allowed origins
    allow_credentials=True, # Allow cookies to be sent
    allow_methods=["*"], # Allows all standard methods
    allow_headers=["*"], # Allows all headers
)

# Include routers from the views layer
app.include_router(auth.router, prefix="/auth", tags=["Authentication"])
app.include_router(github.router, prefix="/repositories", tags=["GitHub Repositories"]) # Updated tag
app.include_router(build.router, prefix="/build", tags=["Builds"]) # Correctly includes build router from views
app.include_router(webhooks.router, prefix="/webhooks", tags=["Webhooks"])

@app.get("/health", tags=["Health Check"])
async def health_check():
    """Basic health check endpoint."""
    # Could add a DB check here later if needed
    return {"status": "OK"}
