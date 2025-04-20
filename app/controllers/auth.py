import os
import logging
import requests
from fastapi import Request, HTTPException, Depends, status, Cookie # Import Cookie
from fastapi.responses import RedirectResponse
from requests_oauthlib import OAuth2Session
from oauthlib.oauth2 import WebApplicationClient, OAuth2Error
from uuid import uuid4
# Import Database type and Depends
from pymongo.database import Database
from fastapi import Depends
from pymongo import DESCENDING
from bson import ObjectId
from datetime import datetime, timedelta, timezone
from pydantic import ValidationError
from typing import Optional # Import Optional

# JWT Handling
from jose import JWTError, jwt

# Import local modules
from config import settings
# Import the UserRepository class AND its dependency function
from repositories.user_repository import UserRepository, get_user_repository
from models import User
# Import the get_database dependency function
from services.db_service import get_database

logger = logging.getLogger(__name__)

oauth_state_store = {}

authorization_base_url = 'https://github.com/login/oauth/authorize'
token_url = 'https://github.com/login/oauth/access_token'
github_api_url = 'https://api.github.com/user'
scope = ['read:user', 'repo']

ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 60 * 24 * 7

# create_access_token function - Keep this accessible
def create_access_token(data: dict, expires_delta: timedelta | None = None):
    """Creates a JWT access token."""
    to_encode = data.copy()
    if expires_delta:
        expire = datetime.now(timezone.utc) + expires_delta
    else:
        expire = datetime.now(timezone.utc) + timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    to_encode.update({"exp": expire})
    if not settings.SECRET_KEY:
         logger.critical("JWT SECRET_KEY is not configured!")
         raise ValueError("JWT Secret Key is missing in configuration.")
    encoded_jwt = jwt.encode(to_encode, settings.SECRET_KEY, algorithm=ALGORITHM)
    return encoded_jwt

# handle_login_redirect remains in controller
async def handle_login_redirect() -> RedirectResponse:
    """
    Generates the GitHub authorization URL and redirects the user.
    Stores the OAuth state temporarily.
    """
    if not settings.GITHUB_CLIENT_ID or not settings.GITHUB_CLIENT_SECRET:
        logger.error("GITHUB_CLIENT_ID or GITHUB_CLIENT_SECRET not configured.")
        raise HTTPException(status_code=500, detail="OAuth credentials not configured.")

    github = OAuth2Session(settings.GITHUB_CLIENT_ID, scope=scope, redirect_uri=settings.GITHUB_CALLBACK_URL)
    authorization_url, state = github.authorization_url(authorization_base_url)

    state_key = str(uuid4())
    oauth_state_store[state_key] = state
    logger.info(f"Generated OAuth state: {state} (key: {state_key})")

    response = RedirectResponse(authorization_url)
    # Use secure=True if served over HTTPS in production
    response.set_cookie(key="oauth_state_key", value=state_key, max_age=300, httponly=True, samesite='lax', secure=False)
    return response

# handle_github_callback function is REMOVED from this file. Its logic is in views/auth.py

# --- Secure Dependency to Get Current User ---
# Modified to use Cookie dependency instead of Request
async def get_current_user_from_token(
    # Inject the access_token cookie directly, make it optional
    access_token: Optional[str] = Cookie(None),
    # Depend on the UserRepository instance
    user_repo: UserRepository = Depends(get_user_repository)
) -> User:
    """
    FastAPI dependency to verify JWT from 'access_token' cookie and return the current user.
    Raises HTTPException 401 if token is invalid, expired, or user not found.
    """
    # Use the injected access_token variable
    if not access_token:
        logger.debug("Access token cookie not found for secure endpoint.")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Not authenticated (token missing)",
            headers={"WWW-Authenticate": "Bearer"},
        )

    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )
    try:
        if not settings.SECRET_KEY:
             logger.critical("JWT SECRET_KEY is not configured!")
             raise ValueError("JWT Secret Key is missing in configuration.")
        # Decode the injected access_token
        payload = jwt.decode(access_token, settings.SECRET_KEY, algorithms=[ALGORITHM])
        user_id_str: str | None = payload.get("sub")
        if user_id_str is None:
            logger.warning("Token payload missing 'sub' (user ID).")
            raise credentials_exception
    except JWTError as e:
        logger.warning(f"JWT validation error: {e}")
        raise credentials_exception from e
    except ValueError as e:
         logger.error(f"JWT configuration error: {e}")
         raise HTTPException(status_code=500, detail="Internal server error: JWT configuration missing.") from e
    except Exception as e:
        logger.error(f"Unexpected error decoding JWT: {e}", exc_info=True)
        raise credentials_exception from e

    try:
        # find_user_by_id is synchronous
        user = user_repo.find_user_by_id(user_id_str)
        if user is None:
            logger.warning(f"User with ID {user_id_str} from token not found in database.")
            raise credentials_exception
        logger.debug(f"Successfully authenticated user ID: {user.id}")
        # Ensure sensitive data like token is not returned unless needed downstream
        user.github_access_token = None
        return user
    except Exception as e:
        logger.error(f"Database error fetching user ID {user_id_str}: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Error retrieving user data."
        ) from e

# --- Removed Insecure Placeholder Functions ---
