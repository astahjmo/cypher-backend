import logging
from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import JSONResponse # Import JSONResponse
from fastapi.encoders import jsonable_encoder # Import jsonable_encoder
from pydantic import BaseModel, Field
from typing import List, Dict, Any
from bson import ObjectId

# Import local modules
from models import RepositoryConfig, User
from repositories.repository_config_repository import RepositoryConfigRepository, get_repo_config_repository
from repositories.user_repository import UserRepository, get_user_repository
from controllers import github as github_controller
from controllers import auth as auth_controller

logger = logging.getLogger(__name__)
router = APIRouter() # Prefix is applied in main.py

# --- Pydantic Models ---
class BranchConfigPayload(BaseModel):
    branches: List[str] = Field(..., description="List of branch names to configure for automatic builds.")

# --- Routes ---
@router.get("")
async def list_repositories(
    current_user: User = Depends(auth_controller.get_current_user_from_token),
    user_repo: UserRepository = Depends(get_user_repository)
):
    """Lists all repositories accessible by the authenticated user."""
    logger.info("Received request for GET /repositories")
    # Returns list[dict] directly from controller, no model involved here
    repo_list = await github_controller.get_user_repositories(user_id=current_user.id, user_repo=user_repo)
    # Use JSONResponse for consistency, though list[dict] might serialize okay by default
    return JSONResponse(content=jsonable_encoder(repo_list))

# Use JSONResponse with jsonable_encoder
@router.get("/configs", response_class=JSONResponse)
async def list_repository_configs(
    current_user: User = Depends(auth_controller.get_current_user_from_token),
    repo_config_repo: RepositoryConfigRepository = Depends(get_repo_config_repository)
):
    """Lists all repository configurations for the authenticated user."""
    logger.info(f"Received request for GET /repositories/configs for user {current_user.login}")
    configs: List[RepositoryConfig] = await github_controller.get_user_repo_configs(
        user_id=current_user.id,
        repo_config_repo=repo_config_repo
    )
    # Use jsonable_encoder which should respect model's json_encoders and by_alias
    content = jsonable_encoder(configs, by_alias=True)
    return JSONResponse(content=content)

@router.get("/{owner}/{repo_name}/branches")
async def list_branches(
    owner: str,
    repo_name: str,
    current_user: User = Depends(auth_controller.get_current_user_from_token),
    user_repo: UserRepository = Depends(get_user_repository)
):
    """Lists branches for a specific repository."""
    logger.info(f"Received request for GET /repositories/{owner}/{repo_name}/branches")
    # Returns list[dict] directly from controller
    branch_list = await github_controller.get_repository_branches(
        owner=owner,
        repo_name=repo_name,
        user_id=current_user.id,
        user_repo=user_repo
    )
    # Use JSONResponse for consistency
    return JSONResponse(content=jsonable_encoder(branch_list))

# Use JSONResponse with jsonable_encoder
@router.post("/{owner}/{repo_name}/branches/config", response_class=JSONResponse)
async def configure_branches(
    owner: str,
    repo_name: str,
    config_payload: BranchConfigPayload,
    repo_config_repo: RepositoryConfigRepository = Depends(get_repo_config_repository),
    user_repo: UserRepository = Depends(get_user_repository),
    current_user: User = Depends(auth_controller.get_current_user_from_token)
):
    """
    Configures specific branches of a repository for automatic builds.
    """
    logger.info(f"Received request for POST /repositories/{owner}/{repo_name}/branches/config")
    saved_config: RepositoryConfig = await github_controller.set_branch_configuration(
        owner=owner,
        repo_name=repo_name,
        branches_to_configure=config_payload.branches,
        repo_config_repo=repo_config_repo,
        user_repo=user_repo,
        user_id=current_user.id
    )
    # Use jsonable_encoder which should respect model's json_encoders and by_alias
    content = jsonable_encoder(saved_config, by_alias=True)
    return JSONResponse(content=content)
