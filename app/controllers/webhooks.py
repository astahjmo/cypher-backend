import logging
from fastapi import Request, HTTPException, BackgroundTasks, Depends # Added Depends
from bson import ObjectId # Import ObjectId

# Import local modules
from config import settings
from services.docker_service import docker_service # Import docker_service
# Import necessary repositories and their dependency functions
from repositories.repository_config_repository import RepositoryConfigRepository, get_repo_config_repository
# Removed UserRepository import as it's no longer needed here
# from repositories.user_repository import UserRepository, get_user_repository
from models import PyObjectId # Import PyObjectId if needed for type hints, though ObjectId might suffice

logger = logging.getLogger(__name__)

async def handle_github_push_event(
    request: Request,
    background_tasks: BackgroundTasks,
    # Inject only the repo config repository
    repo_config_repo: RepositoryConfigRepository = Depends(get_repo_config_repository)
    # Removed user_repo dependency
    # user_repo: UserRepository = Depends(get_user_repository)
) -> dict:
    """
    Processes a validated GitHub push event payload.
    Checks if the repo/branch is configured (by anyone) and triggers a Docker build job if necessary.
    """
    try:
        payload = await request.json()
    except Exception as e:
        logger.error(f"Failed to parse webhook payload: {e}", exc_info=True)
        raise HTTPException(status_code=400, detail="Invalid JSON payload.")

    # Extract relevant information
    try:
        repo_info = payload.get('repository', {})
        repo_full_name = repo_info.get('full_name')
        repo_url = repo_info.get('clone_url')
        # owner_info = repo_info.get('owner', {}) # No longer need owner info here
        # owner_login = owner_info.get('login')
        ref = payload.get('ref')
        commit_sha = payload.get('after') # Get the commit SHA

        # Removed owner_login from validation
        if not repo_full_name or not ref or not ref.startswith('refs/heads/') or not repo_url or not commit_sha:
            logger.warning(f"Webhook payload missing required fields (repo full_name/clone_url, ref, after): {payload}")
            return {"message": "Payload missing required fields for build trigger."}

        branch = ref.split('/')[-1]
        # owner = repo_full_name.split('/')[0]
        repo_name = repo_full_name.split('/')[1]

        # Removed owner from log message
        logger.info(f"Processing push event for repository: {repo_full_name}, branch: {branch}, commit: {commit_sha[:7]}")

    except Exception as e:
        logger.error(f"Error parsing webhook payload details: {e}", exc_info=True)
        raise HTTPException(status_code=400, detail="Error parsing payload details.")

    # Removed user lookup logic

    # Check database if this repo/branch is configured for auto-builds (by anyone)
    is_configured_for_build = False
    try:
        # Call the simplified function without user_id
        is_configured_for_build = repo_config_repo.is_branch_configured_for_build(
            repo_full_name=repo_full_name,
            branch=branch
        )
        if is_configured_for_build:
             logger.info(f"Branch '{branch}' is configured for auto-build for {repo_full_name}.")
        else:
             logger.info(f"Branch '{branch}' not configured for auto-build for {repo_full_name}.")
             return {"message": "Branch not configured for auto-build."}

    except Exception as e:
        logger.error(f"Database error checking build config for {repo_full_name}/{branch}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Database error checking build configuration.")

    logger.info(f"Triggering Docker build job for {repo_full_name}, branch {branch}, commit {commit_sha[:7]}.")

    try:
        if settings.REGISTRY_URL:
            logger.error("REGISTRY_URL is not configured in settings. using LOCALHOST registry")
            registry_host = settings.REGISTRY_URL.replace("https://", "").replace("http://", "")
            sanitized_branch = branch.replace('/', '-')
            build_image_tag = f"{registry_host}/{repo_full_name}:{sanitized_branch}-{commit_sha[:7]}".lower()
        else:
            sanitized_branch = branch.replace('/', '-')
            build_image_tag = f"{repo_full_name}:{sanitized_branch}-{commit_sha[:7]}".lower()
            logger.info(f"Generated image tag: {build_image_tag}")
    except Exception as e:
        logger.error(f"Error generating image tag: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Error generating image tag.")

    # TODO: Revisit how BuildStatus will be created/associated without a specific user ID here.
    background_tasks.add_task(
        docker_service.build_and_push_image,
        repo_url=repo_url,
        branch=branch,
        commit_sha=commit_sha, # Pass actual commit SHA from webhook
        build_image_tag=build_image_tag,
        repo_full_name=repo_full_name,
    )

    return {"message": f"Docker build initiated in background for {repo_full_name} branch {branch}."}
