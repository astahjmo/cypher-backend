import logging
from fastapi import APIRouter, Depends, HTTPException, status, Path # Import necessary FastAPI components
from fastapi.responses import JSONResponse
from fastapi.encoders import jsonable_encoder
# Removed Pydantic imports from here
from typing import List, Dict, Any
from bson import ObjectId
from github import GithubException # Import GithubException for specific error handling
import requests # Import requests for exception handling in callback

# Import local modules
# Import DB Models used by controller functions or dependencies
from models.github.db_models import RepositoryConfig
from models.auth.db_models import User
# Import API Models (Views) from the new location
from models.github.api_models import RepositoryView, BranchView, BranchConfigPayload, RepositoryConfigView
from repositories.repository_config_repository import RepositoryConfigRepository, get_repo_config_repository
from repositories.user_repository import UserRepository, get_user_repository
from controllers import github as github_controller # Controller functions
from controllers.auth import get_current_user_from_token # Auth dependency

logger = logging.getLogger(__name__)
router = APIRouter() # Define router here

# --- Pydantic Models / Schemas for this View ---
# Removed model definitions - they are now in models/github/api_models.py

# --- API Endpoints ---

@router.get(
    "",
    response_model=List[RepositoryView], # Use imported view model
    summary="List User Repositories",
    description="Lists all repositories accessible by the authenticated user via GitHub."
)
async def list_repositories(
    current_user: User = Depends(get_current_user_from_token),
    user_repo: UserRepository = Depends(get_user_repository)
):
    """Endpoint to list user's GitHub repositories."""
    logger.info(f"View: Received request for GET /repositories for user {current_user.login}")
    try:
        repo_list_data = await github_controller.get_user_repositories(user_id=current_user.id, user_repo=user_repo)
        # Controller returns list[dict] matching RepositoryView structure
        return JSONResponse(content=jsonable_encoder(repo_list_data))
    except ValueError as e: # Catch token errors from controller
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail=str(e))
    except GithubException as e:
        logger.error(f"View: GitHub API error listing repositories for user {current_user.id}: {e.status} - {e.data}", exc_info=False)
        status_code = e.status if e.status >= 400 else status.HTTP_502_BAD_GATEWAY # Use 502 for upstream errors
        detail = f"GitHub API error: {e.data.get('message', 'Failed to fetch repositories')}"
        if e.status == 401:
             detail = "GitHub API error: Bad credentials. Please re-authenticate."
             status_code = status.HTTP_401_UNAUTHORIZED # Ensure 401 for bad credentials
        raise HTTPException(status_code=status_code, detail=detail)
    except Exception as e:
        logger.error(f"View: Unexpected error listing repositories for user {current_user.id}: {e}", exc_info=True)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="An unexpected error occurred.")


@router.get(
    "/configs",
    response_model=List[RepositoryConfigView], # Use imported view model
    summary="List Repository Configurations",
    description="Lists all repository configurations saved for the authenticated user."
)
async def list_repository_configs(
    current_user: User = Depends(get_current_user_from_token),
    repo_config_repo: RepositoryConfigRepository = Depends(get_repo_config_repository)
):
    """Endpoint to list saved repository configurations."""
    logger.info(f"View: Received request for GET /repositories/configs for user {current_user.login}")
    try:
        configs: List[RepositoryConfig] = await github_controller.get_user_repo_configs(
            user_id=current_user.id,
            repo_config_repo=repo_config_repo
        )
        # Convert RepositoryConfig models to RepositoryConfigView dictionaries for response
        # jsonable_encoder handles ObjectId to str conversion based on model Config
        return JSONResponse(content=jsonable_encoder(configs, by_alias=True))
    except (TypeError, RuntimeError) as e: # Catch errors from controller
         logger.error(f"View: Error fetching configurations for user {current_user.id}: {e}", exc_info=True)
         raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))
    except Exception as e:
        logger.error(f"View: Unexpected error fetching configurations for user {current_user.id}: {e}", exc_info=True)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Failed to retrieve configurations.")


@router.get(
    "/{owner}/{repo_name}/branches",
    response_model=List[BranchView], # Use imported view model
    summary="List Repository Branches",
    description="Lists branches for a specific repository accessible by the user."
)
async def list_branches(
    owner: str = Path(..., description="Repository owner"),
    repo_name: str = Path(..., description="Repository name"),
    current_user: User = Depends(get_current_user_from_token),
    user_repo: UserRepository = Depends(get_user_repository)
):
    """Endpoint to list branches of a specific repository."""
    logger.info(f"View: Received request for GET /repositories/{owner}/{repo_name}/branches")
    try:
        branch_list_data = await github_controller.get_repository_branches(
            owner=owner,
            repo_name=repo_name,
            user_id=current_user.id,
            user_repo=user_repo
        )
        # Controller returns list[dict] matching BranchView structure
        return JSONResponse(content=jsonable_encoder(branch_list_data))
    except ValueError as e: # Catch token errors or repo not found
        # Distinguish based on message if possible, otherwise assume 401/404 might be appropriate
        if "token not found" in str(e) or "Bad credentials" in str(e):
             raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail=str(e))
        elif "not found or not accessible" in str(e):
             raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(e))
        else: # Generic ValueError
             raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))
    except GithubException as e: # Catch other specific GitHub errors
        logger.error(f"View: GitHub API error fetching branches for {owner}/{repo_name}: {e.status} - {e.data}", exc_info=False)
        status_code = e.status if e.status >= 400 else status.HTTP_502_BAD_GATEWAY
        detail = f"GitHub API error: {e.data.get('message', 'Failed to fetch branches')}"
        raise HTTPException(status_code=status_code, detail=detail)
    except Exception as e:
        logger.error(f"View: Unexpected error fetching branches for {owner}/{repo_name}: {e}", exc_info=True)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="An unexpected error occurred.")


@router.post(
    "/{owner}/{repo_name}/branches/config",
    response_model=RepositoryConfigView, # Use the view model
    summary="Configure Repository Branches",
    description="Sets the list of branches to be automatically built for a repository."
)
async def configure_branches(
    # Corrected Order: Path parameters and Body first
    config_payload: BranchConfigPayload, # Request body
    owner: str = Path(..., description="Repository owner"),
    repo_name: str = Path(..., description="Repository name"),
    # Dependencies last
    repo_config_repo: RepositoryConfigRepository = Depends(get_repo_config_repository),
    user_repo: UserRepository = Depends(get_user_repository),
    current_user: User = Depends(get_current_user_from_token),
):
    """Endpoint to save branch configuration for auto-builds."""
    logger.info(f"View: Received request for POST /repositories/{owner}/{repo_name}/branches/config")
    try:
        saved_config: RepositoryConfig = await github_controller.set_branch_configuration(
            owner=owner,
            repo_name=repo_name,
            branches_to_configure=config_payload.branches,
            repo_config_repo=repo_config_repo,
            user_repo=user_repo,
            user_id=current_user.id
        )
        # Convert RepositoryConfig (DB model) to RepositoryConfigView for response
        return JSONResponse(content=jsonable_encoder(saved_config, by_alias=True))
    except ValueError as e: # Catch token errors or repo not found/accessible
        if "token not found" in str(e) or "Bad credentials" in str(e):
             raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail=str(e))
        elif "not found or not accessible" in str(e):
             raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(e))
        else:
             raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))
    except GithubException as e: # Catch other GitHub errors
        logger.error(f"View: GitHub API error configuring branches for {owner}/{repo_name}: {e.status} - {e.data}", exc_info=False)
        status_code = e.status if e.status >= 400 else status.HTTP_502_BAD_GATEWAY
        detail = f"GitHub API error: {e.data.get('message', 'Failed to verify repository access')}"
        raise HTTPException(status_code=status_code, detail=detail)
    except (TypeError, RuntimeError) as e: # Catch DB or internal errors
         logger.error(f"View: Error saving configuration for {owner}/{repo_name}: {e}", exc_info=True)
         raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))
    except Exception as e:
        logger.error(f"View: Unexpected error configuring branches for {owner}/{repo_name}: {e}", exc_info=True)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="An unexpected error occurred.")
