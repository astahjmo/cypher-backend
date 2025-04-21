import logging
import requests # Adicionado import
from fastapi import APIRouter, Depends, Request, Response, HTTPException, status, Cookie
from fastapi.responses import RedirectResponse
from pydantic import BaseModel, Field # Adicionado BaseModel e Field
from typing import Optional

# Import controller functions
from controllers.auth import (
    ctrl_handle_login_redirect,
    ctrl_handle_github_callback,
    # ctrl_read_users_me, # Not needed directly, handled by dependency
    ctrl_logout,
    get_current_user_from_token, # Keep dependency function import here
    ACCESS_TOKEN_EXPIRE_MINUTES # Importar a constante
)
# Import User model from new location (for type hinting dependency)
from models.auth.db_models import User
# Import PyObjectId from base for UserView
from models.base import PyObjectId
# Import UserRepository dependency
from repositories.user_repository import UserRepository, get_user_repository
from config import settings # Import settings for cookie security flag

logger = logging.getLogger(__name__)

# Define the router here
router = APIRouter()

# --- View Models ---
class UserView(BaseModel):
    """API Model for User information (subset of DB model)."""
    id: str = Field(..., description="User's internal ID") # Expose ID as string
    login: str = Field(..., description="GitHub username")
    name: Optional[str] = None
    email: Optional[str] = None
    avatar_url: Optional[str] = None

    # Add model_config for example generation in OpenAPI docs (optional)
    class Config:
        json_schema_extra = {
            "example": {
                "id": "60d5ecf3a3a3a3a3a3a3a3a3",
                "login": "octocat",
                "name": "The Octocat",
                "email": "octocat@github.com",
                "avatar_url": "https://avatars.githubusercontent.com/u/583231?v=4"
            }
        }
        # If converting from DB model with PyObjectId, allow population by field name
        populate_by_name = True


# --- API Endpoints ---

@router.get("/login", summary="Initiate GitHub OAuth Login", tags=["Authentication"])
async def login_redirect() -> RedirectResponse:
    """
    Redirects the user to GitHub for OAuth authorization.
    Handles potential configuration errors.
    """
    try:
        # Call the controller function which contains the core logic
        authorization_url, state_key = await ctrl_handle_login_redirect()
        response = RedirectResponse(authorization_url)
        # Use secure=True if served over HTTPS in production
        # Check if HTTPS_ONLY_COOKIES exists before using it
        secure_cookie = getattr(settings, 'HTTPS_ONLY_COOKIES', False)
        response.set_cookie(key="oauth_state_key", value=state_key, max_age=300, httponly=True, samesite='lax', secure=secure_cookie)
        return response
    except ValueError as e: # Catch config errors from controller
        logger.error(f"Configuration error during login redirect: {e}", exc_info=True) # Log config error
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))
    except Exception as e:
        logger.error(f"Unexpected error during login redirect initiation: {e}", exc_info=True)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Failed to initiate login process.")


@router.get("/callback", summary="GitHub OAuth Callback", tags=["Authentication"])
async def github_callback(request: Request, user_repo: UserRepository = Depends(get_user_repository)):
    """
    Handles the callback from GitHub, exchanges code for token, fetches user info,
    upserts user, creates JWT, and sets cookie. Delegates core logic to controller.
    """
    try:
        # Call the controller function, passing necessary FastAPI objects/dependencies
        user, jwt_token = await ctrl_handle_github_callback(request, user_repo)

        # Redirect user to frontend, setting JWT as a cookie
        redirect_url = settings.FRONTEND_URL or "/" # Use FRONTEND_URL from settings
        response = RedirectResponse(redirect_url)
        # Check if HTTPS_ONLY_COOKIES exists before using it
        secure_cookie = getattr(settings, 'HTTPS_ONLY_COOKIES', False)
        response.set_cookie(
            key="access_token",
            value=jwt_token,
            httponly=True, # Important for security
            max_age=ACCESS_TOKEN_EXPIRE_MINUTES * 60, # In seconds
            expires=ACCESS_TOKEN_EXPIRE_MINUTES * 60, # Redundant with max_age, but some browsers might prefer it
            path="/",
            samesite='lax', # Consider 'strict' if applicable
            secure=secure_cookie, # Set based on environment
        )
        # Clean up state cookie
        response.delete_cookie("oauth_state_key")
        logger.info(f"Redirecting user {user.login} to {redirect_url} with JWT cookie set.")
        return response

    except ValueError as e: # Catch state mismatch or token errors
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))
    except requests.exceptions.RequestException as e: # Catch GitHub API errors
        logger.error(f"HTTP error fetching user data from GitHub: {e}", exc_info=True)
        raise HTTPException(status_code=status.HTTP_502_BAD_GATEWAY, detail="Failed to fetch user data from GitHub.")
    except RuntimeError as e: # Catch DB or other internal errors from controller
        logger.error(f"Error processing GitHub callback: {e}", exc_info=True)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))
    except Exception as e:
        # Log unexpected errors during callback processing
        logger.error(f"Unexpected error during GitHub callback processing: {e}", exc_info=True)
        # Redirect to frontend login with an error parameter (optional)
        error_redirect_url = f"{settings.FRONTEND_URL or '/login'}?error=callback_failed" # Use FRONTEND_URL
        # Or raise a generic 500 error
        # raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Login callback failed.")
        return RedirectResponse(error_redirect_url)


@router.get("/me", response_model=UserView, summary="Get Current User", tags=["Authentication"]) # Corrigido response_model
async def get_current_user(current_user: User = Depends(get_current_user_from_token)):
    """
    Returns the authenticated user's information based on the access token cookie.
    Uses the get_current_user_from_token dependency.
    """
    # The dependency handles authentication and fetching.
    # Convert the DB User model to the UserView API model for the response.
    # Ensure the User model has the necessary fields or handle potential missing fields.
    user_data = current_user.model_dump(by_alias=True) # Use model_dump for Pydantic v2
    # Corrigido: Acessar '_id' do dump e converter para string no campo 'id' do view model
    user_data['id'] = str(user_data['_id'])
    return UserView.model_validate(user_data) # Use model_validate


@router.post("/logout", summary="Log Out User", tags=["Authentication"])
async def logout_user_view(response: Response): # Renamed function to avoid conflict
    """
    Clears the authentication cookie. Delegates logic to controller.
    """
    # Call controller function to handle any backend logout logic (if any)
    await ctrl_logout(response) # Pass response for potential future use in controller
    # Clear cookies in the view layer, as it controls the HTTP response
    # Check if HTTPS_ONLY_COOKIES exists before using it
    secure_cookie = getattr(settings, 'HTTPS_ONLY_COOKIES', False)
    response.delete_cookie(key="access_token", path="/", httponly=True, samesite='lax', secure=secure_cookie)
    response.delete_cookie(key="oauth_state_key", path="/", httponly=True, samesite='lax', secure=secure_cookie) # Also clear state cookie on logout
    return {"message": "Successfully logged out"}
