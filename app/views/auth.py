import logging
import os # Import os module
from typing import Annotated
import requests # Need requests here now
from fastapi import APIRouter, Request, Depends, HTTPException, Response, status
from fastapi.responses import RedirectResponse, JSONResponse
from requests_oauthlib import OAuth2Session # Need this
from oauthlib.oauth2 import WebApplicationClient, OAuth2Error # Need this
from bson import ObjectId
from datetime import timedelta # Need timedelta
from pymongo.database import Database # Need Database type

from repositories.user_repository import UserRepository, get_user_repository # Import repo and dependency function
from models import User
from controllers import auth as auth_controller
from config import settings # Need settings
from services.db_service import get_database # Need get_database dependency

logger = logging.getLogger(__name__)
router = APIRouter()

# --- Routes ---
@router.get("/login")
async def login():
    """
    Redirects the user to GitHub for authentication by calling the controller.
    """
    logger.info("Received request for /auth/login")
    response = await auth_controller.handle_login_redirect()
    return response

@router.get("/callback")
async def callback(
    request: Request,
    code: str,
    state: str,
    # Inject Database and UserRepository using Annotated and Depends correctly
    db: Annotated[Database, Depends(get_database)],
    user_repo: Annotated[UserRepository, Depends(get_user_repository)]
):
    """
    Handles the callback from GitHub directly within the view route.
    Injects DB and UserRepository using FastAPI dependencies.
    Verifies state, fetches token, fetches user info, saves/updates user,
    creates a JWT, and sets it as an HttpOnly cookie.
    """
    logger.info(f"Received request for /auth/callback with code: {code[:10]}..., state: {state}")

    # user_repo is now injected via Depends

    # --- Logic moved from controller ---

    # --- Verify State ---
    state_key = request.cookies.get("oauth_state_key")
    if not state_key or state_key not in auth_controller.oauth_state_store:
        logger.warning("OAuth state key cookie missing or invalid.")
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid state key or session expired.")

    expected_state = auth_controller.oauth_state_store.pop(state_key, None)
    if not expected_state or state != expected_state:
        logger.warning(f"OAuth state mismatch. Expected: {expected_state}, Received: {state}")
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid OAuth state.")
    logger.info("OAuth state verified successfully.")

    # --- Fetch Token ---
    if not settings.GITHUB_CLIENT_ID or not settings.GITHUB_CLIENT_SECRET:
        logger.error("GITHUB_CLIENT_ID or GITHUB_CLIENT_SECRET not configured.")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="OAuth credentials not configured.")

    # Allow insecure transport for local development (http callback)
    # Check if the callback URL is localhost or if DEBUG is explicitly True
    original_insecure_transport = os.environ.get('OAUTHLIB_INSECURE_TRANSPORT')
    if "http://localhost" in settings.GITHUB_CALLBACK_URL or settings.DEBUG:
        os.environ['OAUTHLIB_INSECURE_TRANSPORT'] = '1'
        logger.warning("OAUTHLIB_INSECURE_TRANSPORT temporarily enabled for development.")

    github_session = OAuth2Session(settings.GITHUB_CLIENT_ID, state=state, redirect_uri=settings.GITHUB_CALLBACK_URL)
    try:
        token_data = github_session.fetch_token(
            auth_controller.token_url,
            client_secret=settings.GITHUB_CLIENT_SECRET,
            authorization_response=str(request.url) # Use the full request URL
        )
        if not token_data or 'access_token' not in token_data:
            logger.error("Failed to fetch access token or token is invalid.")
            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Could not obtain access token from GitHub.")
        access_token = token_data['access_token']
        logger.info(f"Successfully obtained GitHub access token: {access_token[:10]}...")
    except OAuth2Error as e:
        logger.error(f"OAuth2 error during token fetch: {e}", exc_info=True)
        # Check if it's the insecure transport error specifically
        if isinstance(e, OAuth2Error) and 'insecure_transport' in str(e):
             detail_msg = "Authentication failed: OAuth 2 requires HTTPS unless OAUTHLIB_INSECURE_TRANSPORT is set."
        else:
             detail_msg = f"Authentication failed (OAuth2Error): {e}"
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=detail_msg)
    except Exception as e:
        logger.error(f"Error fetching token: {e}", exc_info=True)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Authentication failed: {e}")
    finally:
        # Restore original OAUTHLIB_INSECURE_TRANSPORT value if it was changed
        if "http://localhost" in settings.GITHUB_CALLBACK_URL or settings.DEBUG:
            if original_insecure_transport is None:
                del os.environ['OAUTHLIB_INSECURE_TRANSPORT']
                logger.info("Restored OAUTHLIB_INSECURE_TRANSPORT (removed).")
            else:
                os.environ['OAUTHLIB_INSECURE_TRANSPORT'] = original_insecure_transport
                logger.info(f"Restored OAUTHLIB_INSECURE_TRANSPORT to original value: {original_insecure_transport}")


    # --- Fetch User Info from GitHub ---
    try:
        user_response = requests.get(auth_controller.github_api_url, headers={'Authorization': f'token {access_token}'})
        user_response.raise_for_status()
        github_user_data = user_response.json()
        github_id = github_user_data.get('id')
        if not github_id:
             raise ValueError("GitHub user ID not found in API response.")
        logger.info(f"Fetched GitHub user data for ID: {github_id}")
    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching user info from GitHub API: {e}", exc_info=True)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Failed to fetch user info from GitHub.")
    except ValueError as e:
         logger.error(f"Error parsing GitHub user data: {e}", exc_info=True)
         raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Failed to parse user info from GitHub.")
    except Exception as e:
        logger.error(f"Unexpected error fetching GitHub user info: {e}", exc_info=True)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="An unexpected error occurred while fetching user information.")

    # --- Use Repository to Find/Create User ---
    try:
        user_data_for_repo = {**github_user_data, "access_token": access_token}
        # Use the injected user_repo instance
        user = user_repo.find_or_create_user(github_id=github_id, user_data=user_data_for_repo)
        if not user or not user.id:
             logger.error(f"User repository did not return a valid user object for GitHub ID: {github_id}")
             raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Failed to process user data in repository.")
        logger.info(f"User processed via repository. User ID: {user.id}")

    except Exception as e:
        logger.error(f"Repository error finding or creating user: {e}", exc_info=True)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Database error during user processing.")

    # --- Create JWT and Set Cookie ---
    access_token_expires = timedelta(minutes=auth_controller.ACCESS_TOKEN_EXPIRE_MINUTES)
    jwt_token = auth_controller.create_access_token(
        data={"sub": str(user.id)}, expires_delta=access_token_expires
    )
    logger.info(f"Generated JWT for user ID: {user.id}")

    # --- Redirect User ---
    response = RedirectResponse(url=settings.FRONTEND_REDIRECT_URL or "/")
    response.delete_cookie("oauth_state_key")
    response.set_cookie(
        key="access_token",
        value=jwt_token,
        httponly=True,
        samesite='lax',
        secure=False,
        max_age=int(access_token_expires.total_seconds()),
        path="/"
    )
    logger.info("Set access_token cookie and redirecting to frontend.")
    return response


# Route to get current user details using the secure JWT dependency from the controller
@router.get("/me", response_model=User)
async def read_users_me(
    current_user: User = Depends(auth_controller.get_current_user_from_token) # Use secure dependency
):
    """
    Returns the details of the currently authenticated user using JWT verification.
    """
    logger.info(f"Received request for /auth/me - returning user: {current_user.login}")
    return current_user

# Logout route - Clears the JWT cookie
@router.post("/logout")
async def logout(response: Response):
    """
    Logs the user out by clearing the access_token cookie.
    """
    logger.info("Received request for /auth/logout")
    json_response = JSONResponse(content={"message": "Logout successful"})
    json_response.delete_cookie(
        key="access_token",
        httponly=True,
        samesite='lax',
        secure=False, # Match the setting used when creating the cookie
        path="/"
    )
    logger.info("Cleared access_token cookie.")
    return json_response
