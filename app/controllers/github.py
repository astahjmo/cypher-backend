import logging
from github import Github, GithubException
from fastapi import HTTPException
from typing import List, Dict, Any # Import List, Dict, Any
from bson import ObjectId # Import ObjectId
import time # Import time (can be removed if not used elsewhere)
# from cachetools import cached, TTLCache # Removed cachetools imports
from async_lru import alru_cache # Import async-lru cache decorator
import functools # Import functools
import asyncio # Import asyncio

# Import local modules
from repositories.repository_config_repository import RepositoryConfigRepository
from repositories.user_repository import UserRepository # Import UserRepository
from models import RepositoryConfig, User # Import the data models

logger = logging.getLogger(__name__)

# --- Cache Configuration ---
# Cache repository lists for 5 minutes (ttl=300 seconds)
# maxsize=100 means we cache results for up to 100 different users
# Note: @alru_cache handles async results correctly.
# The arguments to the cached function (user_id, user_repo) are used for the cache key by default.
# We don't need a custom key function here as the default behavior is fine.

# Apply the async-lru cache decorator
@alru_cache(maxsize=100, ttl=300)
async def get_user_repositories(user_id: ObjectId, user_repo: UserRepository) -> list[dict]:
    """
    Fetches a list of repositories accessible by the authenticated user, identified by user_id.
    Results are cached per user for 5 minutes using async-lru.
    """
    # Note: This log will only appear on cache misses or expirations
    logger.info(f"Fetching repositories for user ID: {user_id} (Cache miss or expired)")

    # Fetch token synchronously first (assuming this is fast)
    # If get_user_token_by_id becomes async or slow, run it in thread too
    token = user_repo.get_user_token_by_id(user_id)
    if not token:
        logger.error(f"GitHub token not found for user ID: {user_id}")
        # Don't cache errors related to missing tokens
        raise HTTPException(status_code=401, detail="GitHub token not found for user. Please re-authenticate.")

    token_preview = f"{token[:5]}...{token[-5:]}" if len(token) > 10 else token
    logger.info(f"Attempting to use token preview: {token_preview} for user ID: {user_id}")

    try:
        # Run the blocking GitHub API call in a separate thread
        def fetch_repos_sync():
             logger.debug(f"Executing blocking GitHub repo fetch in thread for user {user_id}")
             g = Github(token)
             github_user = g.get_user() # This might also be blocking
             repos = github_user.get_repos(type='all', sort='updated', direction='desc')
             # Convert PaginatedList to a simple list immediately within the thread
             repo_list_sync = [{"name": repo.name, "full_name": repo.full_name, "private": repo.private, "url": repo.html_url} for repo in repos]
             logger.debug(f"Finished blocking GitHub repo fetch for user {user_id}")
             return repo_list_sync

        repo_list = await asyncio.to_thread(fetch_repos_sync)

        logger.info(f"Fetched {len(repo_list)} repositories for GitHub user (associated with internal user ID: {user_id})")
        return repo_list
    except GithubException as e:
        logger.error(f"GitHub API error while listing repositories for user ID {user_id} (token preview: {token_preview}): {e.status} - {e.data}", exc_info=True)
        # Don't cache GitHub errors
        if e.status == 401:
             raise HTTPException(status_code=401, detail=f"GitHub API error: Bad credentials. Please re-authenticate.")
        raise HTTPException(status_code=e.status, detail=f"GitHub API error: {e.data.get('message', 'Unknown error')}")
    except Exception as e:
        logger.error(f"Unexpected error listing repositories for user ID {user_id} (token preview: {token_preview}): {e}", exc_info=True)
        # Don't cache unexpected errors
        raise HTTPException(status_code=500, detail="An unexpected error occurred while fetching repositories.")


# --- Other functions ---
# Apply similar async/threading patterns if they involve blocking I/O

async def get_repository_branches(owner: str, repo_name: str, user_id: ObjectId, user_repo: UserRepository) -> list[dict]:
    """
    Fetches a list of branches for a specific repository using the user's token identified by user_id.
    """
    repo_full_name = f"{owner}/{repo_name}"
    logger.info(f"Fetching branches for repository: {repo_full_name} for user ID: {user_id}")

    token = user_repo.get_user_token_by_id(user_id)
    if not token:
        logger.error(f"GitHub token not found for user ID: {user_id} when fetching branches for {repo_full_name}")
        raise HTTPException(status_code=401, detail="GitHub token not found for user. Please re-authenticate.")

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
        if e.status == 404: raise HTTPException(status_code=404, detail=f"Repository '{repo_full_name}' not found or not accessible.")
        if e.status == 401: raise HTTPException(status_code=401, detail=f"GitHub API error: Bad credentials accessing {repo_full_name}. Please re-authenticate.")
        raise HTTPException(status_code=e.status, detail=f"GitHub API error: {e.data.get('message', 'Unknown error')}")
    except Exception as e:
        logger.error(f"Unexpected error fetching branches for {repo_full_name} by user ID {user_id} (token preview: {token_preview}): {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"An unexpected error occurred while fetching branches for {repo_full_name}.")

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
    """
    repo_full_name = f"{owner}/{repo_name}"
    logger.info(f"User ID: {user_id} request to configure branches for {repo_full_name}: {branches_to_configure}")

    token = user_repo.get_user_token_by_id(user_id)
    if not token:
        logger.error(f"GitHub token not found for user ID: {user_id} when configuring {repo_full_name}")
        raise HTTPException(status_code=401, detail="GitHub token not found for user. Please re-authenticate.")

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
        if e.status == 404: raise HTTPException(status_code=404, detail=f"Repository '{repo_full_name}' not found or not accessible.")
        if e.status == 401: raise HTTPException(status_code=401, detail=f"GitHub API error: Bad credentials verifying {repo_full_name}. Please re-authenticate.")
        raise HTTPException(status_code=e.status, detail=f"GitHub API error: {e.data.get('message', 'Unknown error')}")
    except Exception as e:
        logger.error(f"Unexpected error verifying repository {repo_full_name} for user ID {user_id} (token preview: {token_preview}): {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"An unexpected error occurred while verifying the repository.")

    # 2. Use repository to save configuration (in thread for DB access)
    try:
        if not isinstance(user_id, ObjectId):
             logger.error(f"User ID {user_id} is not ObjectId type when saving config for {repo_full_name}. Type: {type(user_id)}")
             raise HTTPException(status_code=500, detail="Internal server error: Invalid user ID format.")

        upsert_func = functools.partial(repo_config_repo.upsert_config, user_id, repo_full_name, branches_to_configure)
        saved_config = await asyncio.to_thread(upsert_func)

        logger.info(f"Successfully saved branch configuration for {repo_full_name} by user ID: {user_id}")
        # Invalidate repo list cache for this user
        # Use try-except as pop might fail if key doesn't exist (though it shouldn't hurt)
        try:
            get_user_repositories.cache_invalidate(user_id, user_repo) # Use invalidate function from async-lru
            logger.info(f"Invalidated repo list cache for user {user_id}")
        except Exception as cache_err:
             logger.warning(f"Could not invalidate cache for user {user_id}: {cache_err}")

        return saved_config
    except Exception as e:
        logger.error(f"Error saving configuration for {repo_full_name} by user ID {user_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Database error saving configuration.")

async def get_user_repo_configs(
    user_id: ObjectId,
    repo_config_repo: RepositoryConfigRepository
) -> List[RepositoryConfig]:
    """
    Retrieves all repository configurations for the authenticated user identified by user_id.
    """
    logger.info(f"Fetching repository configurations for user ID: {user_id}")
    try:
        if not isinstance(user_id, ObjectId):
             logger.error(f"User ID {user_id} is not ObjectId type when fetching configs. Type: {type(user_id)}")
             raise HTTPException(status_code=500, detail="Internal server error: Invalid user ID format.")

        # Run DB query in thread
        find_func = functools.partial(repo_config_repo.find_by_user, user_id)
        configs = await asyncio.to_thread(find_func)
        return configs
    except Exception as e:
        logger.error(f"Controller error fetching configurations for user ID {user_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Database error retrieving configurations.")
