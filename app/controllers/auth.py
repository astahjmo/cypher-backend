import os
import logging
import requests
from requests_oauthlib import OAuth2Session
from oauthlib.oauth2 import OAuth2Error
from uuid import uuid4
from fastapi import Depends, HTTPException, status, Cookie, Request, Response, WebSocket
from datetime import datetime, timedelta, timezone
from pydantic import ValidationError
from typing import Optional, Dict, Any, Tuple

from jose import JWTError, jwt

from config import settings
from repositories.user_repository import UserRepository, get_user_repository
from models.auth.db_models import User

logger = logging.getLogger(__name__)

oauth_state_store: Dict[str, str] = {}

authorization_base_url = "https://github.com/login/oauth/authorize"
token_url = "https://github.com/login/oauth/access_token"
github_api_url = "https://api.github.com/user"
scope = ["read:user", "repo"]

ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 60 * 24 * 7


def create_access_token(data: dict, expires_delta: timedelta | None = None):
    """Creates a JWT access token."""
    to_encode = data.copy()
    if expires_delta:
        expire = datetime.now(timezone.utc) + expires_delta
    else:
        expire = datetime.now(timezone.utc) + timedelta(
            minutes=ACCESS_TOKEN_EXPIRE_MINUTES
        )
    to_encode.update({"exp": expire})
    if not settings.SECRET_KEY:
        logger.critical("JWT SECRET_KEY is not configured!")
        raise ValueError("JWT Secret Key is missing in configuration.")
    encoded_jwt = jwt.encode(to_encode, settings.SECRET_KEY, algorithm=ALGORITHM)
    return encoded_jwt


async def _verify_token_and_get_user(
    token: Optional[str], user_repo: UserRepository
) -> User:
    """
    Verifies a JWT token (from cookie or elsewhere) and returns the corresponding user.
    Raises HTTPException 401/500 on failure. Shared logic for HTTP and WebSocket auth.
    """
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )
    if not token:
        logger.debug("Token not provided for verification.")
        raise credentials_exception

    try:
        if not settings.SECRET_KEY:
            logger.critical("JWT SECRET_KEY is not configured!")
            raise ValueError("JWT Secret Key is missing.")

        payload = jwt.decode(token, settings.SECRET_KEY, algorithms=[ALGORITHM])
        user_id_str: str | None = payload.get("sub")
        if user_id_str is None:
            logger.warning("Token payload missing 'sub' (user ID).")
            raise credentials_exception

    except JWTError as e:
        logger.warning(f"JWT validation error: {e}")
        raise credentials_exception from e
    except ValueError as e:
        logger.error(f"JWT configuration error: {e}")
        raise HTTPException(
            status_code=500, detail="Internal server error: JWT configuration."
        ) from e
    except Exception as e:
        logger.error(f"Unexpected error decoding JWT: {e}", exc_info=True)
        raise credentials_exception from e

    try:
        user = user_repo.find_user_by_id(user_id_str)
        if user is None:
            logger.warning(f"User with ID {user_id_str} from token not found.")
            raise credentials_exception
        logger.debug(f"Authenticated user ID: {user.id} via token verification.")
        return user
    except Exception as e:
        logger.error(
            f"Database error fetching user ID {user_id_str}: {e}", exc_info=True
        )
        raise HTTPException(
            status_code=500, detail="Error retrieving user data."
        ) from e


async def get_current_user_from_token(
    access_token: Optional[str] = Cookie(None, alias="access_token"),
    user_repo: UserRepository = Depends(get_user_repository),
) -> User:
    """
    FastAPI dependency to verify JWT from 'access_token' cookie and return the current user.
    Uses the shared verification logic.
    """
    logger.debug(
        f"Attempting to get user from cookie: {access_token[:10] if access_token else 'None'}"
    )
    return await _verify_token_and_get_user(token=access_token, user_repo=user_repo)


def generate_github_auth_url() -> Tuple[str, str]:
    """Generates the GitHub authorization URL and state."""
    if not settings.GITHUB_CLIENT_ID or not settings.GITHUB_CLIENT_SECRET:
        logger.error("GITHUB_CLIENT_ID or GITHUB_CLIENT_SECRET not configured.")
        raise ValueError("OAuth credentials not configured.")

    github = OAuth2Session(
        settings.GITHUB_CLIENT_ID,
        scope=scope,
        redirect_uri=settings.GITHUB_CALLBACK_URL,
    )
    authorization_url, state = github.authorization_url(authorization_base_url)

    state_key = str(uuid4())
    oauth_state_store[state_key] = state
    logger.info(f"Generated OAuth state: {state} (key: {state_key})")
    return authorization_url, state_key


def process_github_callback(
    code: str,
    state_from_gh: str,
    state_key_from_cookie: Optional[str],
    user_repo: UserRepository,
) -> Tuple[User, str]:
    """
    Processes the GitHub callback, exchanges code, upserts user, and creates JWT.
    Returns the User object and the JWT token.
    """
    logger.info(
        f"Processing callback: code={code[:10]}..., state={state_from_gh}, state_key_cookie={state_key_from_cookie}"
    )

    if not state_key_from_cookie:
        raise ValueError("OAuth state key cookie missing.")

    original_state = oauth_state_store.pop(state_key_from_cookie, None)

    if not original_state or original_state != state_from_gh:
        logger.warning(
            f"State mismatch: Original='{original_state}', Received='{state_from_gh}'"
        )
        raise ValueError("OAuth state mismatch.")

    try:
        github = OAuth2Session(
            settings.GITHUB_CLIENT_ID,
            state=original_state,
            redirect_uri=settings.GITHUB_CALLBACK_URL,
        )
        token = github.fetch_token(
            token_url, client_secret=settings.GITHUB_CLIENT_SECRET, code=code
        )
        github_access_token = token.get("access_token")
        if not github_access_token:
            raise OAuth2Error(description="Failed to obtain GitHub access token.")
        logger.info(
            f"GitHub access token obtained successfully (first 5 chars): {github_access_token[:5]}..."
        )

        user_response = github.get(github_api_url)
        user_response.raise_for_status()
        github_user_data = user_response.json()
        logger.info(
            f"Fetched GitHub user data for login: {github_user_data.get('login')}"
        )

        user_data = {
            "github_id": github_user_data.get("id"),
            "login": github_user_data.get("login"),
            "name": github_user_data.get("name"),
            "email": github_user_data.get("email"),
            "avatar_url": github_user_data.get("avatar_url"),
            "github_access_token": github_access_token,
        }

        user = user_repo.upsert_user(user_data)
        if not user or not user.id:
            logger.error("Failed to create or update user in database.")
            raise RuntimeError("Failed to process user information in database.")

        logger.info(f"User upserted/found in DB: {user.login} (ID: {user.id})")

        jwt_data = {"sub": str(user.id)}
        jwt_token = create_access_token(jwt_data)

        return user, jwt_token

    except (
        OAuth2Error,
        requests.exceptions.RequestException,
        ValidationError,
        RuntimeError,
        ValueError,
    ) as e:
        logger.error(f"Error during GitHub callback processing: {e}", exc_info=True)
        raise e
    except Exception as e:
        logger.error(f"Unexpected error during GitHub callback: {e}", exc_info=True)
        raise RuntimeError("An unexpected error occurred during login callback.") from e


async def ctrl_handle_login_redirect() -> Tuple[str, str]:
    """Generates GitHub auth URL and state key."""
    return generate_github_auth_url()


async def ctrl_handle_github_callback(
    request: Request, user_repo: UserRepository
) -> Tuple[User, str]:
    """Processes GitHub callback, returns User and JWT."""
    code = request.query_params.get("code")
    state_from_gh = request.query_params.get("state")
    state_key_from_cookie = request.cookies.get("oauth_state_key")

    if not code or not state_from_gh:
        raise ValueError("Missing code or state in callback.")

    user, jwt_token = process_github_callback(
        code, state_from_gh, state_key_from_cookie, user_repo
    )
    return user, jwt_token


async def ctrl_read_users_me(current_user: User) -> User:
    """Returns the current user (already fetched by dependency)."""
    return current_user


async def ctrl_logout(response: Response):
    """Handles logout logic (clearing cookies is done in the view)."""
    logger.info("Logout processed in controller (currently no specific action).")
    pass


async def get_user_from_websocket_cookie(
    websocket: WebSocket, user_repo: UserRepository
) -> Optional[User]:
    """
    Attempts to get the user from the 'access_token' cookie in a WebSocket connection.
    Returns User or None. Does not raise HTTPException directly, caller should handle None.
    """
    access_token = websocket.cookies.get("access_token")
    if not access_token:
        logger.debug("WebSocket connection missing 'access_token' cookie.")
        return None
    try:
        user = await _verify_token_and_get_user(token=access_token, user_repo=user_repo)
        return user
    except HTTPException as e:
        logger.warning(
            f"WebSocket cookie validation failed: {e.detail} (Status: {e.status_code})"
        )
        return None
    except Exception as e:
        logger.error(
            f"Unexpected error verifying WebSocket cookie token: {e}", exc_info=True
        )
        return None
