import logging
import asyncio
import os
import aiofiles
from fastapi import APIRouter, Depends, HTTPException, Request, BackgroundTasks
from fastapi.responses import StreamingResponse
from pydantic import BaseModel # Import BaseModel
from typing import List, Dict, Optional, Any
from bson import ObjectId
import time

# Import models and dependencies
from models import User, BuildStatus, PyObjectId, BuildLog
from controllers.auth import get_current_user_from_token
from controllers.build import handle_docker_build_trigger, sse_connections, TEMP_LOG_DIR
from repositories.build_status_repository import BuildStatusRepository, get_build_status_repository
from repositories.build_log_repository import BuildLogRepository, get_build_log_repository
from repositories.repository_config_repository import RepositoryConfigRepository, get_repo_config_repository

logger = logging.getLogger(__name__)
router = APIRouter(tags=["Builds"]) # Prefix is applied in main.py

# --- Pydantic Model for Request Body ---
# Changed 'tag' to 'tag_version'
class TriggerBuildRequest(BaseModel):
    tag_version: str

# --- Helper Function for SSE ---
# (log_stream_generator remains the same)
async def log_stream_generator(build_id_str: str, build_status_repo: BuildStatusRepository):
    """Async generator to watch the log file and yield SSE events."""
    log_file_path = os.path.join(TEMP_LOG_DIR, f"build_{build_id_str}.log")
    build_id_obj = ObjectId(build_id_str)
    last_position = 0
    check_interval = 0.5
    is_db_complete = False
    final_status = "unknown"
    file_disappeared = False
    file_exists_ever = False
    completion_grace_period = 2.0
    completion_detected_time: Optional[float] = None
    found_final_marker = False

    logger.info(f"SSE Stream {build_id_str}: Generator started. Watching {log_file_path}")

    sse_event = sse_connections.get(build_id_str)
    if sse_event:
        sse_event.set()
        logger.info(f"SSE Stream {build_id_str}: Signaled background task.")
    else:
        try:
            status_doc = build_status_repo.get_build_by_id(build_id_obj)
            if status_doc and status_doc.status in ["success", "failed", "cancelled"]:
                 logger.warning(f"SSE Stream {build_id_str}: Connection attempt, build already completed ({status_doc.status}). Reading logs once.")
                 if os.path.exists(log_file_path):
                     async with aiofiles.open(log_file_path, mode='r', encoding='utf-8') as log_file:
                         lines = await log_file.readlines()
                         for line in lines: yield f"data: {line.strip()}\n\n"
                 yield f"event: BUILD_COMPLETE\ndata: {status_doc.status}\n\n"
                 return
            elif not status_doc:
                 logger.warning(f"SSE Stream {build_id_str}: Build ID not found.")
                 yield f"event: ERROR\ndata: Build not found\n\n"
                 return
            else:
                 logger.warning(f"SSE Stream {build_id_str}: No SSE event found, but build active. Proceeding.")
        except Exception as db_err:
             logger.error(f"SSE Stream {build_id_str}: DB error on initial check: {db_err}")
             yield f"event: ERROR\ndata: DB error checking build status\n\n"
             return

    try:
        while True:
            if not is_db_complete:
                try:
                    status_doc = build_status_repo.get_build_by_id(build_id_obj)
                    if status_doc:
                        current_status = status_doc.status
                        if current_status in ["success", "failed", "cancelled"]:
                            logger.info(f"SSE Stream {build_id_str}: DB status is complete ({current_status}). Starting grace period.")
                            is_db_complete = True
                            final_status = current_status
                            completion_detected_time = time.monotonic()
                    elif file_exists_ever:
                         logger.warning(f"SSE Stream {build_id_str}: Status doc missing, file existed. Assuming completion.")
                         is_db_complete = True
                         final_status = "unknown (status doc missing)"
                         completion_detected_time = time.monotonic()
                except Exception as db_err:
                    logger.error(f"SSE Stream {build_id_str}: Error fetching build status: {db_err}")

            new_lines_read = False
            try:
                if os.path.exists(log_file_path):
                    file_exists_ever = True
                    async with aiofiles.open(log_file_path, mode='r', encoding='utf-8') as log_file:
                        await log_file.seek(last_position)
                        new_lines = await log_file.readlines()
                        current_position = await log_file.tell()
                        if new_lines:
                            new_lines_read = True
                            for line in new_lines:
                                stripped_line = line.strip()
                                if "--- BUILD FINISHED" in stripped_line:
                                     found_final_marker = True
                                     logger.info(f"SSE Stream {build_id_str}: Found final marker in logs.")
                                yield f"data: {stripped_line}\n\n"
                            last_position = current_position
                        elif current_position < last_position:
                             logger.warning(f"SSE Stream {build_id_str}: Log file truncated? Resetting position.")
                             last_position = 0
                elif file_exists_ever:
                     logger.warning(f"SSE Stream {build_id_str}: Log file disappeared.")
                     file_disappeared = True
                     if not is_db_complete:
                         is_db_complete = True
                         final_status = final_status if final_status != "unknown" else "unknown (file disappeared)"
                         completion_detected_time = time.monotonic()
                else:
                     logger.debug(f"SSE Stream {build_id_str}: Log file not found yet. Waiting...")
            except Exception as file_err:
                logger.error(f"SSE Stream {build_id_str}: Error reading log file {log_file_path}: {file_err}")
                yield f"event: ERROR\ndata: Error reading log file: {file_err}\n\n"

            if is_db_complete:
                grace_over = completion_detected_time and (time.monotonic() - completion_detected_time > completion_grace_period)
                if file_disappeared or grace_over or found_final_marker:
                    logger.info(f"SSE Stream {build_id_str}: Exiting loop. DB Complete: {is_db_complete}, File Disappeared: {file_disappeared}, Grace Over: {grace_over}, Found Marker: {found_final_marker}")
                    break

            if not new_lines_read:
                 await asyncio.sleep(check_interval)

        logger.info(f"SSE Stream {build_id_str}: Performing final log read...")
        try:
            if os.path.exists(log_file_path):
                async with aiofiles.open(log_file_path, mode='r', encoding='utf-8') as log_file:
                    await log_file.seek(last_position)
                    final_lines = await log_file.readlines()
                    if final_lines:
                        logger.info(f"SSE Stream {build_id_str}: Sending {len(final_lines)} final log lines.")
                        for line in final_lines: yield f"data: {line.strip()}\n\n"
        except Exception as final_read_err:
             logger.error(f"SSE Stream {build_id_str}: Error during final log read: {final_read_err}")
             yield f"event: ERROR\ndata: Error during final log read: {final_read_err}\n\n"

        logger.info(f"SSE Stream {build_id_str}: Sending final BUILD_COMPLETE event with status '{final_status}'.")
        yield f"event: BUILD_COMPLETE\ndata: {final_status}\n\n"

    except asyncio.CancelledError:
         logger.info(f"SSE Stream {build_id_str}: Client disconnected.")
    except Exception as e:
        logger.error(f"SSE Stream {build_id_str}: Unexpected error in generator: {e}", exc_info=True)
        try: yield f"event: ERROR\ndata: Internal stream error: {e}\n\n"
        except Exception: pass
    finally:
        logger.info(f"SSE Stream {build_id_str}: Generator finished.")
        if build_id_str in sse_connections:
            if sse_connections.get(build_id_str):
                 del sse_connections[build_id_str]
                 logger.info(f"SSE Stream {build_id_str}: Cleaned up SSE event.")


# --- API Routes ---

@router.post("/docker/{owner}/{repo_name}/{branch}", response_model_by_alias=True)
async def trigger_docker_build(
    owner: str,
    repo_name: str,
    branch: str,
    request_body: TriggerBuildRequest, # Accept request body with tag_version
    request: Request, # Keep request if needed elsewhere
    background_tasks: BackgroundTasks,
    user: User = Depends(get_current_user_from_token),
    repo_config_repo: RepositoryConfigRepository = Depends(get_repo_config_repository),
    build_status_repo: BuildStatusRepository = Depends(get_build_status_repository),
    build_log_repo: BuildLogRepository = Depends(get_build_log_repository)
):
    """Triggers a manual Docker build for a specific repository and branch, using the provided tag version."""
    # Returns dict, alias doesn't apply here
    return await handle_docker_build_trigger(
        owner=owner,
        repo_name=repo_name,
        branch=branch,
        tag_version=request_body.tag_version, # Pass the tag_version from the request body
        user=user,
        background_tasks=background_tasks,
        repo_config_repo=repo_config_repo,
        build_status_repo=build_status_repo,
        build_log_repo=build_log_repo
    )

@router.get("/{build_id}/logs/stream", response_class=StreamingResponse)
async def stream_build_logs(
    build_id: str,
    request: Request,
    build_status_repo: BuildStatusRepository = Depends(get_build_status_repository)
):
    """Streams build logs using Server-Sent Events (SSE)."""
    logger.info(f"Client connected to SSE stream for build ID: {build_id}")
    if not ObjectId.is_valid(build_id):
         raise HTTPException(status_code=400, detail="Invalid Build ID format.")
    return StreamingResponse(
        log_stream_generator(build_id, build_status_repo),
        media_type="text/event-stream"
    )

@router.get("/statuses", response_model=List[BuildStatus], response_model_by_alias=True)
async def get_all_build_statuses(
    user: User = Depends(get_current_user_from_token),
    build_status_repo: BuildStatusRepository = Depends(get_build_status_repository)
):
    """Retrieves all build statuses."""
    return build_status_repo.get_all_builds()

@router.get("/{build_id}", response_model=BuildStatus, response_model_by_alias=True)
async def get_build_status_details(
    build_id: str,
    user: User = Depends(get_current_user_from_token),
    build_status_repo: BuildStatusRepository = Depends(get_build_status_repository)
):
    """Retrieves the details for a specific build."""
    if not ObjectId.is_valid(build_id):
        raise HTTPException(status_code=400, detail="Invalid Build ID format.")
    build_obj_id = ObjectId(build_id)
    build = build_status_repo.get_build_by_id(build_obj_id)
    if not build:
        raise HTTPException(status_code=404, detail="Build not found.")
    return build

@router.get("/{build_id}/logs", response_model=List[BuildLog], response_model_by_alias=True)
async def get_build_logs_history(
    build_id: str,
    user: User = Depends(get_current_user_from_token),
    build_log_repo: BuildLogRepository = Depends(get_build_log_repository),
    build_status_repo: BuildStatusRepository = Depends(get_build_status_repository) # Keep for potential ownership check
):
    """Retrieves the historical logs for a completed build."""
    if not ObjectId.is_valid(build_id):
        raise HTTPException(status_code=400, detail="Invalid Build ID format.")
    build_obj_id = ObjectId(build_id)
    # Fetch ALL logs by setting limit=0
    logs: List[BuildLog] = build_log_repo.get_logs_by_build(build_obj_id, limit=0)
    return logs
