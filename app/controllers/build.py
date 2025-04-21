import logging
import asyncio
import json
import os
import tempfile
import shutil
# Keep Request for state access, add BackgroundTasks for type hint and parameter
from fastapi import Request, BackgroundTasks
from sse_starlette.sse import EventSourceResponse
from typing import List, Dict, Any, Optional, AsyncGenerator
from bson import ObjectId
import git # Import git library

# Import models
from models.build.db_models import BuildStatus, BuildLog
from models.auth.db_models import User
from models.github.db_models import RepositoryConfig
from models.base import PyObjectId

# Import View Models
from models.build.api_models import TriggerBuildRequestView

# Import services and repositories
from services.docker_build_service import docker_build_service
from services.registry_service import registry_service, are_registry_credentials_set
from repositories.build_status_repository import BuildStatusRepository
from repositories.build_log_repository import BuildLogRepository
from repositories.repository_config_repository import RepositoryConfigRepository
# Import notification service
from services.notification_service import send_discord_build_notification


logger = logging.getLogger(__name__)

TEMP_LOG_DIR = tempfile.mkdtemp(prefix="cypher_build_logs_")
logger.info(f"Temporary build log directory created at: {TEMP_LOG_DIR}")

# --- SSE Connection Management ---
sse_connections: Dict[str, List[asyncio.Queue]] = {}
sse_connections_lock = asyncio.Lock()

async def send_log_update(build_id: str, message: str):
    """Sends a log message to all connected SSE clients for a specific build."""
    # Log entry into the function
    logger.debug(f"SSE Log Callback: Received message for build {build_id}: '{message[:100]}...'")
    async with sse_connections_lock:
        if build_id in sse_connections:
            queues = sse_connections[build_id]
            if queues: # Check if there are actually clients connected
                 logger.debug(f"SSE Log Callback: Found {len(queues)} active queue(s) for build {build_id}. Putting message...")
                 tasks = [queue.put(message) for queue in queues]
                 try:
                     await asyncio.gather(*tasks)
                     logger.debug(f"SSE Log Callback: Successfully put message onto queue(s) for build {build_id}.")
                 except Exception as e:
                      logger.error(f"SSE Log Callback: Error putting message onto queue for build {build_id}: {e}", exc_info=True)
            else:
                 # This might happen if client disconnects just before log arrives
                 logger.warning(f"SSE Log Callback: No active queues/clients found for build {build_id} when trying to send log: {message[:100]}...")
        else:
           # This might happen if the build finishes very quickly before client connects or after disconnect
           logger.warning(f"SSE Log Callback: build_id {build_id} not found in sse_connections dict.")


async def send_build_complete(build_id: str, final_status: str):
    """Sends a build completion event to all connected SSE clients."""
    logger.debug(f"SSE Complete Callback: Sending BUILD_COMPLETE:{final_status} for build {build_id}")
    async with sse_connections_lock:
        if build_id in sse_connections:
            queues = sse_connections[build_id]
            if queues:
                logger.debug(f"SSE Complete Callback: Found {len(queues)} active queue(s) for build {build_id}. Putting message...")
                tasks = [queue.put(f"BUILD_COMPLETE:{final_status}") for queue in queues]
                try:
                    await asyncio.gather(*tasks)
                    logger.debug(f"SSE Complete Callback: Successfully put BUILD_COMPLETE onto queue(s) for build {build_id}.")
                except Exception as e:
                    logger.error(f"SSE Complete Callback: Error putting BUILD_COMPLETE onto queue for build {build_id}: {e}", exc_info=True)

            else:
                 logger.warning(f"SSE Complete Callback: No active queues/clients found for build {build_id} when trying to send BUILD_COMPLETE.")
        else:
           logger.warning(f"SSE Complete Callback: build_id {build_id} not found in sse_connections dict.")


async def cleanup_sse_connection(build_id: str, queue: asyncio.Queue):
    """Removes a queue from the connection list when a client disconnects."""
    async with sse_connections_lock:
        if build_id in sse_connections:
            try:
                sse_connections[build_id].remove(queue)
                logger.info(f"SSE Cleanup: Client disconnected for build {build_id}. Remaining clients: {len(sse_connections[build_id])}")
                if not sse_connections[build_id]:
                    del sse_connections[build_id]
                    logger.info(f"SSE Cleanup: Removed build ID {build_id} from SSE connections (no clients left).")
            except ValueError:
                logger.debug(f"SSE Cleanup: Queue for build {build_id} already removed.")
                pass # Queue already removed

# --- Background Build Task ---
async def run_build_and_log(
    build_id: str,
    repo_url: str,
    branch: str,
    tag_version: str,
    repo_full_name: str,
    build_status_repo: BuildStatusRepository,
    build_log_repo: BuildLogRepository,
    user_id: PyObjectId # Changed back to PyObjectId based on model
):
    """Runs the Docker build process in the background, logs output, and updates status."""
    log_file_path = os.path.join(TEMP_LOG_DIR, f"{build_id}.log")
    final_status = "pending" # Start as pending
    final_image_tag = None
    commit_sha = None
    commit_message = None
    temp_dir = None # Define temp_dir here for cleanup in finally block

    try:
        # Ensure user_id is ObjectId before using it with repository
        if not isinstance(user_id, ObjectId):
             logger.warning(f"run_build_and_log received user_id as {type(user_id)}, converting to ObjectId.")
             user_id = PyObjectId(user_id) # Convert if necessary

        # Send initial message
        await send_log_update(build_id, f"Build process initiated for {repo_full_name} branch {branch}...")

        # --- Git Clone and Commit Info ---
        temp_dir = tempfile.mkdtemp(prefix=f"cypher_build_{build_id}_")
        logger.info(f"Created temporary directory for cloning: {temp_dir}")
        await send_log_update(build_id, f"Cloning repository {repo_url} branch {branch}...")
        try:
            # Clone the repository
            cloned_repo = await asyncio.to_thread(git.Repo.clone_from, repo_url, temp_dir, branch=branch, depth=1)
            logger.info(f"Successfully cloned repository for build {build_id}")
            await send_log_update(build_id, "Repository cloned successfully.")

            # Get commit info from the cloned repo
            head_commit = cloned_repo.head.commit
            commit_sha = head_commit.hexsha
            commit_message = head_commit.message.strip()
            logger.info(f"Fetched commit info: SHA={commit_sha[:7]}, Message='{commit_message[:50]}...'")
            await send_log_update(build_id, f"Using commit: {commit_sha[:7]} ('{commit_message[:50]}...')")

        except git.GitCommandError as e:
            logger.error(f"Git clone failed for {repo_url} branch {branch}: {e}", exc_info=True)
            error_msg = f"ERROR: Git clone failed: {e.stderr}"
            await send_log_update(build_id, error_msg)
            # Write git error to log file
            with open(log_file_path, 'a') as log_file:
                 log_file.write(error_msg + '\n')
            final_status = "failed"
            # Update status immediately to failed because clone failed
            build_status_repo.update_status(build_id, final_status, message="Git clone failed.")
            # Skip the rest of the build process
            raise # Re-raise to go to the finally block for cleanup

        # --- Update Status to Running with Commit Info ---
        logger.info(f"Updating build status to 'running' with commit info for build {build_id}")
        build_status_repo.update_status(build_id, "running", commit_sha=commit_sha, commit_message=commit_message)
        await send_log_update(build_id, "Starting Docker build...")

        # --- Execute Docker Build ---
        success, _, image_id, final_image_tag = await docker_build_service.build_image_from_git(
            repo_url=repo_url, # Although cloned, might be useful for context? Or remove? Let's keep for now.
            branch=branch,
            tag_version=tag_version,
            repo_full_name=repo_full_name,
            build_id=build_id,
            log_file_path=log_file_path,
            log_callback=lambda log_line: send_log_update(build_id, log_line) # Pass send_log_update directly
        )

        # --- Handle Build Result and Push ---
        if success and final_image_tag:
            await send_log_update(build_id, f"Build successful. Image created: {final_image_tag}")
            push_successful = True # Assume success if no push needed
            if are_registry_credentials_set():
                await send_log_update(build_id, f"Pushing image {final_image_tag} to registry...")
                push_success, push_logs = await asyncio.to_thread(registry_service.push_image, final_image_tag)
                for line in push_logs.splitlines():
                     await send_log_update(build_id, f"[Push] {line}")
                if push_success:
                    await send_log_update(build_id, "Image push successful.")
                    final_status = "success"
                else:
                    await send_log_update(build_id, "ERROR: Image push failed.")
                    final_status = "failed" # Keep failed status if push fails
            else:
                await send_log_update(build_id, "Registry not configured, skipping push.")
                final_status = "success" # Mark as success if build worked but no push needed
        else:
            # Build itself failed
            await send_log_update(build_id, "ERROR: Build process failed.")
            final_status = "failed"

    except Exception as e:
        logger.error(f"Error during build process for build {build_id}: {e}", exc_info=True)
        # Try sending error via SSE as well
        try:
            await send_log_update(build_id, f"ERROR: An unexpected error occurred during the build: {e}")
        except Exception as sse_e:
             logger.error(f"Failed to send build error via SSE for build {build_id}: {sse_e}")
        final_status = "failed"
    finally:
        # --- Final Status Update and Cleanup ---
        logger.info(f"Build {build_id} final status: {final_status}")
        # Update final status in DB (might re-update if clone failed, which is ok)
        build_status_repo.update_status(build_id, final_status, image_tag=final_image_tag if final_status == 'success' else None)
        # Send final status message via SSE
        await send_log_update(build_id, f"--- Build finished (Status: {final_status}) ---")
        # Send explicit completion event for frontend logic
        await send_build_complete(build_id, final_status)

        # --- Send Final Discord Notification ---
        logger.info(f"Sending final Discord notification for build {build_id} with status {final_status}")
        # Run in thread to avoid blocking completion if notification fails/slows
        asyncio.create_task(asyncio.to_thread(
            send_discord_build_notification,
            repo_full_name=repo_full_name,
            branch_name=branch,
            commit_sha=commit_sha, # Use fetched commit sha
            commit_message=commit_message, # Use fetched commit message
            pusher_name=None, # Pusher not relevant for final status
            build_id=build_id,
            status=final_status.capitalize() # Capitalize status (Success/Failed)
        ))

        # Persist logs from file to DB
        try:
            if os.path.exists(log_file_path):
                with open(log_file_path, 'r') as f:
                    log_entries = []
                    for line in f:
                        log_type = 'log'
                        if "ERROR:" in line.upper(): log_type = 'error'
                        elif "WARNING:" in line.upper(): log_type = 'warning'
                        log_entries.append(BuildLog(build_id=ObjectId(build_id), message=line.strip(), type=log_type))
                    if log_entries:
                        # Run insert_many in thread to avoid blocking
                        await asyncio.to_thread(build_log_repo.insert_many, log_entries)
                        logger.info(f"Persisted {len(log_entries)} log entries for build {build_id}")
            else:
                 logger.warning(f"Temporary log file not found for build {build_id}: {log_file_path}")
        except Exception as e:
            logger.error(f"Error persisting logs for build {build_id}: {e}", exc_info=True)

        # Clean up the temporary directory if it was created
        if temp_dir and os.path.exists(temp_dir):
            try:
                await asyncio.to_thread(shutil.rmtree, temp_dir)
                logger.info(f"Removed temporary directory: {temp_dir}")
            except Exception as e:
                logger.error(f"Failed to remove temporary directory {temp_dir}: {e}", exc_info=True)


# --- Controller Logic Functions ---

async def handle_docker_build_trigger(
    repo_owner: str,
    repo_name: str,
    branch: str,
    background_tasks: BackgroundTasks,
    payload: Optional[TriggerBuildRequestView],
    current_user: User,
    repo_config_repo: RepositoryConfigRepository,
    build_status_repo: BuildStatusRepository,
    build_log_repo: BuildLogRepository,
    # Removed runtime_config_repo parameter
    # runtime_config_repo: ContainerRuntimeConfigRepository,
) -> Dict[str, str]:
    """Handles the logic to trigger a Docker build."""
    repo_full_name = f"{repo_owner}/{repo_name}"
    tag_version = payload.tag_version if payload and payload.tag_version else "latest" # Use provided tag or default

    logger.info(f"Controller: Build trigger request for {repo_full_name}, branch '{branch}', tag '{tag_version}' by user {current_user.login}")

    if not isinstance(current_user.id, ObjectId):
         logger.error(f"CRITICAL: current_user.id is not ObjectId in handle_docker_build_trigger. Type: {type(current_user.id)}")
         # Attempt conversion, but this indicates a potential upstream issue
         try:
             user_object_id = PyObjectId(current_user.id)
         except Exception:
              raise TypeError("Invalid user ID format received.")
    else:
         user_object_id = current_user.id


    # Run synchronous DB call in thread
    repo_config = await asyncio.to_thread(repo_config_repo.find_by_repo_and_user, repo_full_name, user_object_id)
    if not repo_config:
        logger.warning(f"Controller: Repository config not found for {repo_full_name} and user {current_user.login}")
        raise ValueError("Repository configuration not found.")

    repo_url = f"https://github.com/{repo_full_name}.git"
    logger.debug(f"Controller: Using repository URL: {repo_url}")

    # Commit info will be fetched in the background task now
    build_status = BuildStatus(
        user_id=str(user_object_id), # Store as string, matching the updated model
        repo_full_name=repo_full_name,
        branch=branch,
        status="pending", # Start as pending, commit info added later
        commit_sha=None,
        commit_message=None,
        image_tag=None
    )
    # Run synchronous DB call in thread
    created_build = await asyncio.to_thread(build_status_repo.create, build_status)
    build_id = str(created_build.id)
    logger.info(f"Controller: Created pending build record with ID: {build_id}")

    # Use the passed background_tasks object directly
    background_tasks.add_task(
        run_build_and_log,
        build_id=build_id,
        repo_url=repo_url,
        branch=branch,
        tag_version=tag_version,
        repo_full_name=repo_full_name,
        # Don't pass commit info here anymore
        build_status_repo=build_status_repo,
        build_log_repo=build_log_repo,
        # runtime_config_repo=runtime_config_repo, # Removed
        user_id=user_object_id # Pass ObjectId
    )
    logger.info(f"Controller: Scheduled background build task for build ID: {build_id}")

    return {"message": "Build triggered successfully.", "build_id": build_id}


async def get_build_statuses(
    user_id: Any, # Accept Any, convert inside
    repo: BuildStatusRepository,
    limit: int
) -> List[BuildStatus]: # Return the DB model
    """Controller logic to get recent build statuses."""
    try:
        statuses = await asyncio.to_thread(repo.find_by_user, user_id=user_id, limit=limit)
        return statuses
    except Exception as e:
        logger.error(f"Controller: Error fetching build statuses for user {user_id}: {e}", exc_info=True)
        raise RuntimeError("Failed to retrieve build statuses.") from e

async def get_builds_list(
    user_id: Any, # Accept Any, convert inside
    repo: BuildStatusRepository
) -> List[BuildStatus]: # Return the DB model
    """Controller logic to get all builds for a user."""
    try:
        builds = await asyncio.to_thread(repo.find_by_user, user_id=user_id, limit=0)
        return builds
    except Exception as e:
        logger.error(f"Controller: Error fetching builds list for user {user_id}: {e}", exc_info=True)
        raise RuntimeError("Failed to retrieve builds list.") from e

async def get_build_detail(
    build_id: str,
    user_id: Any, # Accept Any, convert inside
    repo: BuildStatusRepository
) -> Optional[BuildStatus]: # Return the DB model or None
    """Controller logic to get specific build details."""
    try:
        build = await asyncio.to_thread(repo.get_by_id_and_user, build_id, user_id)
        # Removed print(build)
        return build # Returns None if not found/accessible
    except ValueError:
        raise ValueError("Invalid Build ID format.") # Propagate specific error
    except Exception as e:
        logger.error(f"Controller: Error fetching build detail for ID {build_id}, user {user_id}: {e}", exc_info=True)
        raise RuntimeError("Failed to retrieve build details.") from e

async def get_historical_build_logs(
    build_id: str,
    user_id: Any, # Accept Any, convert inside
    log_repo: BuildLogRepository,
    status_repo: BuildStatusRepository
) -> List[BuildLog]: # Return the DB model
    """Controller logic to get historical logs."""
    try:
        # First check access using status repo (which now handles string user_id comparison)
        build_status = await asyncio.to_thread(status_repo.get_by_id_and_user, build_id, user_id)
        if not build_status:
             raise ValueError("Build not found or not accessible.") # Raise error for view

        # If accessible, fetch logs using build_id (which should be ObjectId internally for BuildLog)
        logs = await asyncio.to_thread(log_repo.find_by_build_id, build_id)
        return logs
    except ValueError as e: # Catch invalid ID or not found
         raise e # Propagate
    except Exception as e:
        logger.error(f"Controller: Error fetching historical logs for build {build_id}, user {user_id}: {e}", exc_info=True)
        raise RuntimeError("Failed to retrieve build logs.") from e

# --- SSE Event Generator Logic (called by view) ---
# ... (stream_build_logs remains the same) ...
async def stream_build_logs(build_id: str, request: Request) -> AsyncGenerator[Dict[str, str], None]:
    """Generator function to yield build log events for SSE."""
    queue = asyncio.Queue()
    async with sse_connections_lock:
        if build_id not in sse_connections:
            sse_connections[build_id] = []
        sse_connections[build_id].append(queue)
        logger.info(f"SSE Stream: Client connected for build {build_id}. Total clients for build: {len(sse_connections[build_id])}")

    try:
        while True:
            # Check connection status first
            disconnected = await request.is_disconnected()
            if disconnected:
                 logger.info(f"SSE Stream: Client disconnected (detected by request) for build {build_id}.")
                 break

            # Wait for a message with a timeout
            try:
                message = await asyncio.wait_for(queue.get(), timeout=1.0) # Check queue every second
                logger.debug(f"SSE Stream: Got message from queue for build {build_id}: {message[:100]}...")
                if message.startswith("BUILD_COMPLETE:"):
                    status = message.split(":", 1)[1]
                    logger.info(f"SSE Stream: Yielding BUILD_COMPLETE event for build {build_id}, status: {status}")
                    yield {"event": "BUILD_COMPLETE", "data": status}
                    break # End stream after completion event
                else:
                    # logger.debug(f"SSE Stream: Yielding message event for build {build_id}")
                    yield {"event": "message", "data": message}
                queue.task_done()
            except asyncio.TimeoutError:
                # logger.debug(f"SSE Stream: Queue timeout for build {build_id}, checking disconnect again.")
                # No message, loop continues to check disconnect status
                continue
            except Exception as e:
                 logger.error(f"SSE Stream: Error getting message from queue for build {build_id}: {e}", exc_info=True)
                 # Decide if we should break or continue on queue error
                 break


    except asyncio.CancelledError:
        logger.info(f"SSE Stream: Generator cancelled for build {build_id}.")
        # Ensure cleanup happens even if cancelled externally
    except Exception as e:
         logger.error(f"SSE Stream: Unexpected error in generator for build {build_id}: {e}", exc_info=True)
    finally:
        logger.debug(f"SSE Stream: Cleaning up connection for build {build_id}")
        await cleanup_sse_connection(build_id, queue)
        logger.info(f"SSE Stream: Finished for build {build_id}")
