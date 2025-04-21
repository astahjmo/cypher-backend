import logging
import asyncio
# Import BackgroundTasks
from fastapi import Request, Header, HTTPException, status, Depends, BackgroundTasks
from typing import Dict, Any, Optional, List # Added List
import hmac
import hashlib
from datetime import datetime
import json # Import json module

# Import models
from models.auth.db_models import User
from models.github.db_models import RepositoryConfig

# Import components
from config import settings
from repositories.repository_config_repository import RepositoryConfigRepository # Keep direct import for type hint
# Import dependency functions for repos needed in background task
from repositories.repository_config_repository import get_repo_config_repository
from repositories.build_status_repository import get_build_status_repository, BuildStatusRepository
from repositories.build_log_repository import get_build_log_repository, BuildLogRepository
# Removed ContainerRuntimeConfigRepository import
# from repositories.container_runtime_config_repository import ContainerRuntimeConfigRepository, get_container_runtime_config_repo
from controllers.build import handle_docker_build_trigger # Keep this import
from services.notification_service import send_discord_build_notification
# Import db_service to get db connection in background task
from services.db_service import db_service


logger = logging.getLogger(__name__)

# --- Background Processing Function ---

async def _process_webhook_background(
    payload: Dict[str, Any], # Pass parsed payload
    # Pass repo instances created by the view/main handler
    repo_config_repo: RepositoryConfigRepository,
    build_status_repo: BuildStatusRepository,
    build_log_repo: BuildLogRepository,
    # Removed runtime_config_repo
    # runtime_config_repo: ContainerRuntimeConfigRepository,
):
    """Processes the webhook payload in the background."""
    logger.info("Background Task: Starting webhook processing.")
    repo_full_name = None
    branch_name = None
    commit_sha = None

    try:
        # 1. Extract data
        repo_data = payload.get("repository", {})
        repo_full_name = repo_data.get("full_name")
        ref = payload.get("ref", "")
        commit_sha = payload.get("after")
        pusher_name = payload.get("pusher", {}).get("name", "Unknown Pusher")
        head_commit = payload.get("head_commit", {})
        commit_message = head_commit.get("message", "No commit message") if head_commit else "No commit message"
        branch_name = ref.split("refs/heads/")[-1]

        logger.info(f"Background Task: Processing push for {repo_full_name}, branch {branch_name}, commit {commit_sha[:7]}")

        # 2. Find Matching Repository Configurations
        matching_configs: List[RepositoryConfig] = []
        try:
            # Use the passed repo instance
            all_configs = await asyncio.to_thread(repo_config_repo.find_by_name, repo_full_name=repo_full_name)
            if isinstance(all_configs, list):
                 matching_configs = [
                     config for config in all_configs
                     if config.repo_full_name == repo_full_name and branch_name in config.auto_build_branches
                 ]
            elif all_configs and all_configs.repo_full_name == repo_full_name and branch_name in all_configs.auto_build_branches:
                 matching_configs.append(all_configs)

            if not matching_configs:
                logger.info(f"Background Task: No matching auto-build configuration found for {repo_full_name} branch {branch_name}.")
                return # Exit background task
            logger.info(f"Background Task: Found {len(matching_configs)} matching configurations for {repo_full_name} branch {branch_name}.")

        except Exception as e:
            logger.error(f"Background Task: Database error finding repository configurations for {repo_full_name}: {e}", exc_info=True)
            return # Exit background task

        # 3. Trigger Builds and Send Trigger Notifications
        final_notification_tasks = []

        for config in matching_configs:
            build_id_for_notification = None
            try:
                logger.info(f"Background Task: Checking auto-build for config {config.id}, user {config.user_id}")
                user_for_build = User(id=config.user_id, github_id=0, login="webhook_trigger")

                build_background_tasks = BackgroundTasks()

                # Call handle_docker_build_trigger without runtime_config_repo
                build_result = await handle_docker_build_trigger(
                    repo_owner=repo_full_name.split('/')[0],
                    repo_name=repo_full_name.split('/')[1],
                    branch=branch_name,
                    background_tasks=build_background_tasks,
                    payload=None,
                    current_user=user_for_build,
                    repo_config_repo=repo_config_repo,
                    build_status_repo=build_status_repo,
                    build_log_repo=build_log_repo,
                    # runtime_config_repo=runtime_config_repo # Removed
                )
                build_id_for_notification = build_result.get("build_id", "unknown")
                logger.info(f"Background Task: Successfully triggered build {build_id_for_notification} for user {config.user_id} config {config.id}")

                await build_background_tasks()

                triggered_notification_task = asyncio.to_thread(
                    send_discord_build_notification,
                    repo_full_name=repo_full_name,
                    branch_name=branch_name,
                    commit_sha=commit_sha,
                    commit_message=commit_message,
                    pusher_name=pusher_name,
                    build_id=build_id_for_notification,
                    status="Build Triggered"
                )
                final_notification_tasks.append(triggered_notification_task)

            except Exception as e:
                logger.error(f"Background Task: Failed to trigger build for user {config.user_id} config {config.id}: {e}", exc_info=True)

        # Wait for "Build Triggered" notifications
        if final_notification_tasks:
            try:
                await asyncio.gather(*final_notification_tasks)
                logger.info(f"Background Task: Finished processing {len(final_notification_tasks)} 'Build Triggered' Discord notifications.")
            except Exception as gather_exc:
                logger.error(f"Background Task: Error waiting for 'Build Triggered' Discord notification tasks: {gather_exc}", exc_info=True)

        logger.info(f"Background Task: Finished processing webhook for {repo_full_name} branch {branch_name}.")

    except Exception as e:
        logger.error(f"Background Task: Unhandled error during webhook processing: {e}", exc_info=True)


# --- Main Webhook Handler (called by view) ---

async def handle_github_push_webhook(
    request: Request,
    background_tasks: BackgroundTasks, # Accept from view
    x_hub_signature_256: Optional[str],
    # Get repo instances via Depends here to pass to background task
    repo_config_repo: RepositoryConfigRepository = Depends(get_repo_config_repository),
    build_status_repo: BuildStatusRepository = Depends(get_build_status_repository),
    build_log_repo: BuildLogRepository = Depends(get_build_log_repository),
    # Removed runtime_config_repo dependency
    # runtime_config_repo: ContainerRuntimeConfigRepository = Depends(get_container_runtime_config_repo),
):
    """
    Validates the GitHub webhook request and schedules background processing.
    Raises ValueError on validation errors.
    """
    logger.info("Controller: Received GitHub push webhook request.")

    # 1. Verify Signature
    if not settings.GITHUB_WEBHOOK_SECRET:
        logger.error("Controller: GITHUB_WEBHOOK_SECRET not configured.")
        raise ValueError("Webhook secret not configured.")
    if not x_hub_signature_256:
        logger.warning("Controller: Missing X-Hub-Signature-256 header.")
        raise ValueError("Missing signature header.")

    try:
        body = await request.body()
        signature = hmac.new(settings.GITHUB_WEBHOOK_SECRET.encode('utf-8'), body, hashlib.sha256).hexdigest()
        expected_signature = f"sha256={signature}"
        if not hmac.compare_digest(expected_signature, x_hub_signature_256):
            logger.error("Controller: Invalid webhook signature.")
            raise ValueError("Invalid signature.")
        logger.info("Controller: Webhook signature verified successfully.")
    except ValueError as e:
        raise e
    except Exception as e:
        logger.error(f"Controller: Error during webhook signature verification: {e}", exc_info=True)
        raise RuntimeError("Webhook signature verification failed.") from e

    # 2. Parse Webhook Payload
    try:
        payload = json.loads(body.decode('utf-8'))
    except Exception as e:
        logger.error(f"Controller: Failed to parse webhook JSON payload: {e}", exc_info=True)
        raise ValueError("Invalid JSON payload.") from e

    # 3. Basic Event Check (Push to Branch)
    try:
        event_type = request.headers.get("X-GitHub-Event")
        if event_type != "push":
            logger.info(f"Controller: Ignoring non-push event: {event_type}")
            return

        ref = payload.get("ref", "")
        commit_sha = payload.get("after")

        if not ref.startswith("refs/heads/") or not commit_sha or commit_sha == '0000000000000000000000000000000000000000':
            logger.info(f"Controller: Ignoring event (not branch push or branch deletion): ref={ref}, commit={commit_sha}")
            return
    except Exception as e:
         logger.error(f"Controller: Error during basic payload check: {e}", exc_info=True)
         raise ValueError("Could not parse basic webhook payload details.") from e


    # 4. Schedule Background Processing, passing only necessary repo instances
    logger.info("Controller: Scheduling background processing for valid push event.")
    # Need to get runtime_config_repo instance here to pass to background task
    # This requires access to the DB connection, which might be tricky here.
    # Alternative: Pass DB connection or get it inside the background task.
    # Let's get it inside the background task for now.
    db = db_service.get_db() # Assuming db_service is accessible globally or passed differently
    if not db:
         logger.error("Controller: Cannot get DB connection to create runtime_config_repo for background task.")
         # Handle error appropriately, maybe raise 500?
         raise HTTPException(status_code=500, detail="Internal server error: DB connection unavailable.")

    runtime_config_repo_instance = ContainerRuntimeConfigRepository(db=db)


    background_tasks.add_task(
        _process_webhook_background,
        payload=payload,
        repo_config_repo=repo_config_repo,
        build_status_repo=build_status_repo,
        build_log_repo=build_log_repo,
        runtime_config_repo=runtime_config_repo_instance # Pass the instance
    )

    logger.info("Controller: Background task scheduled. Returning acceptance to view.")
