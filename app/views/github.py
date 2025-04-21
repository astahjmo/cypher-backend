import logging
from fastapi import APIRouter, Depends, HTTPException, status, Path
from fastapi.responses import JSONResponse
from fastapi.encoders import jsonable_encoder
from typing import List, Dict, Any
from bson import ObjectId
from github import GithubException
import requests

from models.github.db_models import RepositoryConfig
from models.auth.db_models import User
from models.github.api_models import (
    RepositoryView,
    BranchView,
    BranchConfigPayload,
    RepositoryConfigView,
)
from repositories.repository_config_repository import (
    RepositoryConfigRepository,
    get_repo_config_repository,
)
from repositories.user_repository import UserRepository, get_user_repository
from controllers import github as github_controller
from controllers.auth import get_current_user_from_token

logger = logging.getLogger(__name__)
router = APIRouter()


@router.get(
    "",
    response_model=List[RepositoryView],
    summary="List User Repositories",
    description="Lists all repositories accessible by the authenticated user via GitHub.",
)
async def list_repositories(
    current_user: User = Depends(get_current_user_from_token),
    user_repo: UserRepository = Depends(get_user_repository),
):
    """API endpoint to list the authenticated user's accessible GitHub repositories.

    Fetches repositories using the user's stored GitHub token via the controller.

    Args:
        current_user (User): The authenticated user object (dependency).
        user_repo (UserRepository): Repository for fetching user data (dependency).

    Returns:
        JSONResponse: A list of repository details conforming to the RepositoryView schema.

    Raises:
        HTTPException(401): If the user's GitHub token is missing or invalid.
        HTTPException(502): If there's an error communicating with the GitHub API.
        HTTPException(500): For unexpected internal errors.
    """
    logger.info(
        f"View: Received request for GET /repositories for user {current_user.login}"
    )
    try:
        repo_list_data = await github_controller.get_user_repositories(
            user_id=current_user.id, user_repo=user_repo
        )
        return JSONResponse(content=jsonable_encoder(repo_list_data))
    except ValueError as e:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail=str(e))
    except GithubException as e:
        logger.error(
            f"View: GitHub API error listing repositories for user {current_user.id}: {e.status} - {e.data}",
            exc_info=False,
        )
        status_code = e.status if e.status >= 400 else status.HTTP_502_BAD_GATEWAY
        detail = (
            f"GitHub API error: {e.data.get('message', 'Failed to fetch repositories')}"
        )
        if e.status == 401:
            detail = "GitHub API error: Bad credentials. Please re-authenticate."
            status_code = status.HTTP_401_UNAUTHORIZED
        raise HTTPException(status_code=status_code, detail=detail)
    except Exception as e:
        logger.error(
            f"View: Unexpected error listing repositories for user {current_user.id}: {e}",
            exc_info=True,
        )
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="An unexpected error occurred.",
        )


@router.get(
    "/configs",
    response_model=List[RepositoryConfigView],
    summary="List Repository Configurations",
    description="Lists all repository configurations saved for the authenticated user.",
)
async def list_repository_configs(
    current_user: User = Depends(get_current_user_from_token),
    repo_config_repo: RepositoryConfigRepository = Depends(get_repo_config_repository),
):
    """API endpoint to list saved repository configurations for auto-builds.

    Args:
        current_user (User): The authenticated user object (dependency).
        repo_config_repo (RepositoryConfigRepository): Repository for config data (dependency).

    Returns:
        JSONResponse: A list of repository configurations conforming to the RepositoryConfigView schema.

    Raises:
        HTTPException(500): If an error occurs fetching configurations from the database.
    """
    logger.info(
        f"View: Received request for GET /repositories/configs for user {current_user.login}"
    )
    try:
        configs: List[RepositoryConfig] = await github_controller.get_user_repo_configs(
            user_id=current_user.id, repo_config_repo=repo_config_repo
        )
        return JSONResponse(content=jsonable_encoder(configs, by_alias=True))
    except (TypeError, RuntimeError) as e:
        logger.error(
            f"View: Error fetching configurations for user {current_user.id}: {e}",
            exc_info=True,
        )
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e)
        )
    except Exception as e:
        logger.error(
            f"View: Unexpected error fetching configurations for user {current_user.id}: {e}",
            exc_info=True,
        )
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to retrieve configurations.",
        )


@router.get(
    "/{owner}/{repo_name}/branches",
    response_model=List[BranchView],
    summary="List Repository Branches",
    description="Lists branches for a specific repository accessible by the user.",
)
async def list_branches(
    owner: str = Path(..., description="Repository owner"),
    repo_name: str = Path(..., description="Repository name"),
    current_user: User = Depends(get_current_user_from_token),
    user_repo: UserRepository = Depends(get_user_repository),
):
    """API endpoint to list branches of a specific GitHub repository.

    Args:
        owner (str): The owner of the GitHub repository.
        repo_name (str): The name of the GitHub repository.
        current_user (User): The authenticated user object (dependency).
        user_repo (UserRepository): Repository for fetching user data (dependency).

    Returns:
        JSONResponse: A list of branch details conforming to the BranchView schema.

    Raises:
        HTTPException(401): If the user's GitHub token is missing or invalid.
        HTTPException(404): If the repository is not found or not accessible by the user.
        HTTPException(400): For other validation errors.
        HTTPException(502): If there's an error communicating with the GitHub API.
        HTTPException(500): For unexpected internal errors.
    """
    logger.info(
        f"View: Received request for GET /repositories/{owner}/{repo_name}/branches"
    )
    try:
        branch_list_data = await github_controller.get_repository_branches(
            owner=owner,
            repo_name=repo_name,
            user_id=current_user.id,
            user_repo=user_repo,
        )
        return JSONResponse(content=jsonable_encoder(branch_list_data))
    except ValueError as e:
        if "token not found" in str(e) or "Bad credentials" in str(e):
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail=str(e))
        elif "not found or not accessible" in str(e):
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(e))
        else:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))
    except GithubException as e:
        logger.error(
            f"View: GitHub API error fetching branches for {owner}/{repo_name}: {e.status} - {e.data}",
            exc_info=False,
        )
        status_code = e.status if e.status >= 400 else status.HTTP_502_BAD_GATEWAY
        detail = (
            f"GitHub API error: {e.data.get('message', 'Failed to fetch branches')}"
        )
        raise HTTPException(status_code=status_code, detail=detail)
    except Exception as e:
        logger.error(
            f"View: Unexpected error fetching branches for {owner}/{repo_name}: {e}",
            exc_info=True,
        )
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="An unexpected error occurred.",
        )


@router.post(
    "/{owner}/{repo_name}/branches/config",
    response_model=RepositoryConfigView,
    summary="Configure Repository Branches",
    description="Sets the list of branches to be automatically built for a repository.",
)
async def configure_branches(
    config_payload: BranchConfigPayload,
    owner: str = Path(..., description="Repository owner"),
    repo_name: str = Path(..., description="Repository name"),
    repo_config_repo: RepositoryConfigRepository = Depends(get_repo_config_repository),
    user_repo: UserRepository = Depends(get_user_repository),
    current_user: User = Depends(get_current_user_from_token),
):
    """API endpoint to save the configuration for which branches trigger automatic builds.

    Verifies user access to the repository via GitHub API before saving the configuration.

    Args:
        config_payload (BranchConfigPayload): Request body containing the list of branches.
        owner (str): The owner of the GitHub repository.
        repo_name (str): The name of the GitHub repository.
        repo_config_repo (RepositoryConfigRepository): Repository for config data (dependency).
        user_repo (UserRepository): Repository for user data (dependency).
        current_user (User): The authenticated user object (dependency).

    Returns:
        JSONResponse: The saved repository configuration conforming to RepositoryConfigView schema.

    Raises:
        HTTPException(401): If the user's GitHub token is missing or invalid.
        HTTPException(404): If the repository is not found or not accessible by the user.
        HTTPException(400): For other validation errors.
        HTTPException(502): If there's an error communicating with the GitHub API during verification.
        HTTPException(500): If there's an error saving the configuration or an unexpected internal error.
    """
    logger.info(
        f"View: Received request for POST /repositories/{owner}/{repo_name}/branches/config"
    )
    try:
        saved_config: RepositoryConfig = (
            await github_controller.set_branch_configuration(
                owner=owner,
                repo_name=repo_name,
                branches_to_configure=config_payload.branches,
                repo_config_repo=repo_config_repo,
                user_repo=user_repo,
                user_id=current_user.id,
            )
        )
        return JSONResponse(content=jsonable_encoder(saved_config, by_alias=True))
    except ValueError as e:
        if "token not found" in str(e) or "Bad credentials" in str(e):
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail=str(e))
        elif "not found or not accessible" in str(e):
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(e))
        else:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))
    except GithubException as e:
        logger.error(
            f"View: GitHub API error configuring branches for {owner}/{repo_name}: {e.status} - {e.data}",
            exc_info=False,
        )
        status_code = e.status if e.status >= 400 else status.HTTP_502_BAD_GATEWAY
        detail = f"GitHub API error: {e.data.get('message', 'Failed to verify repository access')}"
        raise HTTPException(status_code=status_code, detail=detail)
    except (TypeError, RuntimeError) as e:
        logger.error(
            f"View: Error saving configuration for {owner}/{repo_name}: {e}",
            exc_info=True,
        )
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e)
        )
    except Exception as e:
        logger.error(
            f"View: Unexpected error configuring branches for {owner}/{repo_name}: {e}",
            exc_info=True,
        )
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="An unexpected error occurred.",
        )
