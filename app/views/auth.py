import logging
import requests
from fastapi import APIRouter, Depends, Request, Response, HTTPException, status, Cookie
from fastapi.responses import RedirectResponse
from pydantic import BaseModel, Field
from typing import Optional

from controllers.auth import (
    ctrl_handle_login_redirect,
    ctrl_handle_github_callback,
    ctrl_logout,
    get_current_user_from_token,
    ACCESS_TOKEN_EXPIRE_MINUTES,
)
from models.auth.db_models import User
from models.base import PyObjectId
from repositories.user_repository import UserRepository, get_user_repository
from config import settings

logger = logging.getLogger(__name__)

router = APIRouter()


class UserView(BaseModel):
    """API Model for User information (subset of DB model)."""

    id: str = Field(..., description="User's internal ID")
    login: str = Field(..., description="GitHub username")
    name: Optional[str] = None
    email: Optional[str] = None
    avatar_url: Optional[str] = None

    model_config = {
        "json_schema_extra": {
            "example": {
                "id": "60d5ecf3a3a3a3a3a3a3a3a3",
                "login": "octocat",
                "name": "The Octocat",
                "email": "octocat@github.com",
                "avatar_url": "https://avatars.githubusercontent.com/u/583231?v=4",
            }
        },
        "populate_by_name": True,
    }


@router.get("/login", summary="Initiate GitHub OAuth Login", tags=["Authentication"])
async def login_redirect() -> RedirectResponse:
    """Redirects the user to GitHub for OAuth authorization.

    Generates the authorization URL and state, sets a state cookie,
    and returns a redirect response. Handles potential configuration errors.

    Returns:
        RedirectResponse: A response that redirects the user's browser to GitHub.

    Raises:
        HTTPException(500): If OAuth credentials are not configured or an unexpected error occurs.
    """
    try:
        authorization_url, state_key = await ctrl_handle_login_redirect()
        response = RedirectResponse(authorization_url)
        secure_cookie = getattr(settings, "HTTPS_ONLY_COOKIES", False)
        response.set_cookie(
            key="oauth_state_key",
            value=state_key,
            max_age=300,
            httponly=True,
            samesite="lax",
            secure=secure_cookie,
        )
        return response
    except ValueError as e:
        logger.error(f"Configuration error during login redirect: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e)
        )
    except Exception as e:
        logger.error(
            f"Unexpected error during login redirect initiation: {e}", exc_info=True
        )
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to initiate login process.",
        )


@router.get("/callback", summary="GitHub OAuth Callback", tags=["Authentication"])
async def github_callback(
    request: Request, user_repo: UserRepository = Depends(get_user_repository)
):
    """Handles the callback from GitHub after user authorization.

    Exchanges the authorization code for an access token, fetches user information
    from GitHub, upserts the user in the local database, creates a JWT access token,
    sets the token in a secure HTTPOnly cookie, and redirects the user to the frontend.

    Args:
        request (Request): The incoming request object containing query parameters and cookies.
        user_repo (UserRepository): Dependency injection for the user repository.

    Returns:
        RedirectResponse: A response redirecting the user to the frontend application,
                          with the access token set as a cookie.

    Raises:
        HTTPException(400): If the state parameter is missing or invalid, or if the code exchange fails.
        HTTPException(502): If fetching user data from GitHub fails.
        HTTPException(500): If there's an error upserting the user or creating the JWT.
    """
    try:
        user, jwt_token = await ctrl_handle_github_callback(request, user_repo)

        redirect_url = settings.FRONTEND_URL or "/"
        response = RedirectResponse(redirect_url)
        secure_cookie = getattr(settings, "HTTPS_ONLY_COOKIES", False)
        response.set_cookie(
            key="access_token",
            value=jwt_token,
            httponly=True,
            max_age=ACCESS_TOKEN_EXPIRE_MINUTES * 60,
            expires=ACCESS_TOKEN_EXPIRE_MINUTES * 60,
            path="/",
            samesite="lax",
            secure=secure_cookie,
        )
        response.delete_cookie("oauth_state_key")
        logger.info(
            f"Redirecting user {user.login} to {redirect_url} with JWT cookie set."
        )
        return response

    except ValueError as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))
    except requests.exceptions.RequestException as e:
        logger.error(f"HTTP error fetching user data from GitHub: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail="Failed to fetch user data from GitHub.",
        )
    except RuntimeError as e:
        logger.error(f"Error processing GitHub callback: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e)
        )
    except Exception as e:
        logger.error(
            f"Unexpected error during GitHub callback processing: {e}", exc_info=True
        )
        error_redirect_url = (
            f"{settings.FRONTEND_URL or '/login'}?error=callback_failed"
        )
        return RedirectResponse(error_redirect_url)


@router.get(
    "/me", response_model=UserView, summary="Get Current User", tags=["Authentication"]
)
async def get_current_user(current_user: User = Depends(get_current_user_from_token)):
    """Retrieves the information of the currently authenticated user.

    Relies on the `get_current_user_from_token` dependency to validate the
    access token cookie and fetch the user data from the database. Converts the
    database model (`User`) to the API response model (`UserView`).

    Args:
        current_user (User): The authenticated user object injected by the dependency.

    Returns:
        UserView: The user's information suitable for API response.
    """
    user_data = current_user.model_dump(by_alias=True)
    user_data["id"] = str(user_data["_id"])  # Ensure ID is string for the view model
    return UserView.model_validate(user_data)


@router.post("/logout", summary="Log Out User", tags=["Authentication"])
async def logout_user_view(response: Response):
    """Logs out the current user by clearing the authentication cookie.

    Calls the controller function for any backend logout logic (currently none)
    and then deletes the `access_token` and `oauth_state_key` cookies from the user's browser.

    Args:
        response (Response): The FastAPI response object used to delete cookies.

    Returns:
        dict: A confirmation message indicating successful logout.
    """
    await ctrl_logout(response)
    secure_cookie = getattr(settings, "HTTPS_ONLY_COOKIES", False)
    response.delete_cookie(
        key="access_token",
        path="/",
        httponly=True,
        samesite="lax",
        secure=secure_cookie,
    )
    response.delete_cookie(
        key="oauth_state_key",
        path="/",
        httponly=True,
        samesite="lax",
        secure=secure_cookie,
    )
    return {"message": "Successfully logged out"}
