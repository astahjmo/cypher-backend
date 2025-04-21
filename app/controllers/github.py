import logging
from github import Github, GithubException
# Removed FastAPI HTTPException import
from typing import List, Dict, Any, Optional, Tuple # Added Tuple
from bson import ObjectId # Import ObjectId
import asyncio # Import asyncio
import functools # Import functools for partial

# Import local modules
from repositories.repository_config_repository import RepositoryConfigRepository
from repositories.user_repository import UserRepository # Import UserRepository
# Import models from new locations
from models.github.db_models import RepositoryConfig
from models.auth.db_models import User
# Import async LRU cache decorator
from async_lru import alru_cache # Import the decorator

logger = logging.getLogger(__name__)

# --- Controller Logic Functions ---

# Apply cache decorator
@alru_cache(maxsize=128, ttl=300) # Cache for 5 minutes
async def get_user_repositories(user_id: ObjectId, user_repo: UserRepository) -> list[dict]:
    """
    Fetches a list of repositories accessible by the authenticated user.
    Raises ValueError if token is missing, GithubException for API errors.
    """
    logger.info(f"Controller: Fetching repositories for user ID: {user_id}")
    token = user_repo.get_user_token_by_id(user_id)
    if not token:
        logger.error(f"Controller: GitHub token not found for user ID: {user_id}")
        raise ValueError("GitHub token not found for user.") # Raise standard error

    token_preview = f"{token[:5]}...{token[-5:]}" if len(token) > 10 else token
    logger.info(f"Controller: Attempting to use token preview: {token_preview} for user ID: {user_id}")

    try:
        # Run blocking GitHub API call in thread
        def fetch_repos_sync():
             logger.debug(f"Controller: Executing blocking GitHub repo fetch in thread for user {user_id}")
             g = Github(token)
             github_user = g.get_user()
             repos = github_user.get_repos(type='all', sort='updated', direction='desc')
             repo_list_sync = [{"name": repo.name, "full_name": repo.full_name, "private": repo.private, "url": repo.html_url} for repo in repos]
             logger.debug(f"Controller: Finished blocking GitHub repo fetch for user {user_id}")
             return repo_list_sync

        repo_list = await asyncio.to_thread(fetch_repos_sync)
        logger.info(f"Controller: Fetched {len(repo_list)} repositories for user ID: {user_id}")
        return repo_list
    except GithubException as e:
        # Let the view handle converting GithubException to HTTPException
        logger.error(f"Controller: GitHub API error listing repositories for user ID {user_id} (token preview: {token_preview}): {e.status} - {e.data}", exc_info=False) # Log less verbosely
        raise e # Re-raise original exception
    except Exception as e:
        logger.error(f"Controller: Unexpected error listing repositories for user ID {user_id} (token preview: {token_preview}): {e}", exc_info=True)
        # Raise a generic exception for unexpected errors
        raise RuntimeError("An unexpected error occurred while fetching repositories.") from e


async def get_repository_branches(owner: str, repo_name: str, user_id: ObjectId, user_repo: UserRepository) -> list[dict]:
    """
    Fetches a list of branches for a specific repository.
    Raises ValueError if token is missing, GithubException for API errors.
    """
    repo_full_name = f"{owner}/{repo_name}"
    logger.info(f"Controller: Fetching branches for repository: {repo_full_name} for user ID: {user_id}")

    token = user_repo.get_user_token_by_id(user_id)
    if not token:
        logger.error(f"Controller: GitHub token not found for user ID: {user_id} when fetching branches for {repo_full_name}")
        raise ValueError("GitHub token not found for user.")

    token_preview = f"{token[:5]}...{token[-5:]}" if len(token) > 10 else token
    logger.info(f"Attempting to use token preview: {token_preview} for user ID: {user_id} to fetch branches for {repo_full_name}")

    try:
        # Run blocking GitHub API call in thread
        def fetch_branches_sync():
            logger.debug(f"Executing blocking GitHub branch fetch in thread for {repo_full_name}")
            g = Github(token)
            repo = g.get_repo(repo_full_name)
            branches = repo.get_branches()
            branch_list_sync = [{"name": branch.name} for branch in branches]
            logger.debug(f"Finished blocking GitHub branch fetch for {repo_full_name}")
            return branch_list_sync

        branch_list = await asyncio.to_thread(fetch_branches_sync)
        logger.info(f"Fetched {len(branch_list)} branches for {repo_full_name}")
        return branch_list
    except GithubException as e:
        logger.error(f"GitHub API error fetching branches for {repo_full_name} by user ID {user_id} (token preview: {token_preview}): {e.status} - {e.data}", exc_info=True)
        # Raise specific exceptions for the view to handle
        if e.status == 404: raise ValueError(f"Repository '{repo_full_name}' not found or not accessible.")
        if e.status == 401: raise ValueError(f"GitHub API error: Bad credentials accessing {repo_full_name}. Please re-authenticate.")
        raise RuntimeError(f"GitHub API error: {e.data.get('message', 'Unknown error')}") from e
    except Exception as e:
        logger.error(f"Unexpected error fetching branches for {repo_full_name} by user ID {user_id} (token preview: {token_preview}): {e}", exc_info=True)
        raise RuntimeError(f"An unexpected error occurred while fetching branches for {repo_full_name}.") from e

async def set_branch_configuration(
    owner: str,
    repo_name: str,
    branches_to_configure: list[str],
    repo_config_repo: RepositoryConfigRepository,
    user_repo: UserRepository,
    user_id: ObjectId
) -> RepositoryConfig:
    """
    Verifies repository access using the user's token and saves the branch configuration.
    Raises ValueError, GithubException, TypeError, or RuntimeError on errors.
    """
    repo_full_name = f"{owner}/{repo_name}"
    logger.info(f"User ID: {user_id} request to configure branches for {repo_full_name}: {branches_to_configure}")

    token = user_repo.get_user_token_by_id(user_id)
    if not token:
        logger.error(f"GitHub token not found for user ID: {user_id} when configuring {repo_full_name}")
        raise ValueError("GitHub token not found for user. Please re-authenticate.")

    token_preview = f"{token[:5]}...{token[-5:]}" if len(token) > 10 else token
    logger.info(f"Attempting to use token preview: {token_preview} for user ID: {user_id} to verify access for {repo_full_name}")

    # 1. Verify repository access (in thread)
    try:
        def verify_repo_access_sync():
            logger.debug(f"Executing blocking GitHub repo access check in thread for {repo_full_name}")
            g = Github(token)
            repo = g.get_repo(repo_full_name)
            logger.debug(f"Finished blocking GitHub repo access check for {repo_full_name}")
            return repo.full_name

        verified_repo_name = await asyncio.to_thread(verify_repo_access_sync)
        logger.info(f"Verified access to repository: {verified_repo_name} for user ID: {user_id}")
    except GithubException as e:
        logger.error(f"GitHub API error verifying repository {repo_full_name} for user ID {user_id} (token preview: {token_preview}): {e.status} - {e.data}", exc_info=True)
        # Raise specific exceptions for the view
        if e.status == 404: raise ValueError(f"Repository '{repo_full_name}' not found or not accessible.")
        if e.status == 401: raise ValueError(f"GitHub API error: Bad credentials verifying {repo_full_name}. Please re-authenticate.")
        raise RuntimeError(f"GitHub API error: {e.data.get('message', 'Unknown error')}") from e
    except Exception as e:
        logger.error(f"Unexpected error verifying repository {repo_full_name} for user ID {user_id} (token preview: {token_preview}): {e}", exc_info=True)
        raise RuntimeError(f"An unexpected error occurred while verifying the repository.") from e

    # 2. Use repository to save configuration (in thread for DB access)
    try:
        if not isinstance(user_id, ObjectId):
             logger.error(f"User ID {user_id} is not ObjectId type when saving config for {repo_full_name}. Type: {type(user_id)}")
             raise TypeError("Internal server error: Invalid user ID format.")

        upsert_func = functools.partial(repo_config_repo.upsert_config, user_id, repo_full_name, branches_to_configure)
        saved_config = await asyncio.to_thread(upsert_func)

        logger.info(f"Successfully saved branch configuration for {repo_full_name} by user ID: {user_id}")
        # Invalidate repo list cache for this user
        try:
            # Use await with cache_invalidate as it's likely async now with async-lru
            await get_user_repositories.cache_invalidate(user_id, user_repo)
            logger.info(f"Invalidated repo list cache for user {user_id}")
        except Exception as cache_err:
             logger.warning(f"Could not invalidate cache for user {user_id}: {cache_err}")

        return saved_config
    except Exception as e:
        logger.error(f"Error saving configuration for {repo_full_name} by user ID {user_id}: {e}", exc_info=True)
        raise RuntimeError("Database error saving configuration.") from e

async def get_user_repo_configs(
    user_id: ObjectId,
    repo_config_repo: RepositoryConfigRepository
) -> List[RepositoryConfig]:
    """
    Retrieves all repository configurations for the authenticated user identified by user_id.
    Raises TypeError or RuntimeError on errors.
    """
    logger.info(f"Fetching repository configurations for user ID: {user_id}")
    try:
        if not isinstance(user_id, ObjectId):
             logger.error(f"User ID {user_id} is not ObjectId type when fetching configs. Type: {type(user_id)}")
             raise TypeError("Internal server error: Invalid user ID format.")

        # Run DB query in thread
        find_func = functools.partial(repo_config_repo.find_by_user, user_id)
        configs = await asyncio.to_thread(find_func)
        return configs
    except Exception as e:
        logger.error(f"Controller error fetching configurations for user ID {user_id}: {e}", exc_info=True)
        raise RuntimeError("Database error retrieving configurations.") from e
