import logging
import docker
import tempfile
import shutil
import os
import git # Import gitpython
import asyncio # Import asyncio
from git import GitCommandError # Import specific error
from docker.errors import BuildError, APIError, DockerException
from typing import Iterator, Any, List, Dict, Tuple # Added Tuple
from datetime import datetime, timezone # Added timezone
from bson import ObjectId # Import ObjectId
import functools # Import functools for partial
import json # Import json for stream decoding

# Import repositories - needed for saving logs/status
from repositories.build_status_repository import BuildStatusRepository
from repositories.build_log_repository import BuildLogRepository
from models import PyObjectId # Import PyObjectId

from config import settings

logger = logging.getLogger(__name__)

# Helper type hint for stream chunks
DockerStreamChunk = dict[str, Any]

# Helper function to check if registry credentials are properly set
def are_registry_credentials_set():
    """Checks if registry URL, user, and password seem configured."""
    return bool(
        settings.REGISTRY_URL and not settings.REGISTRY_URL.startswith("YOUR_") and
        settings.REGISTRY_USER and not settings.REGISTRY_USER.startswith("YOUR_") and
        settings.REGISTRY_PASSWORD and not settings.REGISTRY_PASSWORD.startswith("YOUR_")
    )

class DockerService:
    def __init__(self):
        try:
            self.client = docker.from_env()
            self.api_client = docker.APIClient(base_url='unix://var/run/docker.sock')
            self.client.ping()
            logger.info("Docker client initialized and connected successfully.")
        except DockerException as e:
            logger.error(f"Failed to initialize Docker client: {e}", exc_info=True)
            self.client = None
            self.api_client = None
        except Exception as e:
            logger.error(f"An unexpected error occurred initializing Docker client: {e}", exc_info=True)
            self.client = None
            self.api_client = None

    # --- Synchronous Helper Functions (to be run in thread) ---

    def _write_log_sync(self, handle: Any, buffer: List[str], message: str):
        """Synchronous helper to write to file and buffer."""
        try:
            message_str = str(message)
            handle.write(message_str + '\n')
            handle.flush()
            buffer.append(message_str)
        except Exception as write_err:
            logger.error(f"SyncWriteLog Error: {write_err} - Msg: {message_str[:100]}...")

    def _process_stream_to_file_sync(
        self,
        stream: Iterator[DockerStreamChunk],
        log_file_handle: Any,
        log_lines_buffer: List[str]
    ) -> str | None:
        """Synchronous stream processor."""
        final_error = None
        try:
            for chunk in stream:
                if isinstance(chunk, bytes):
                    try: chunk = json.loads(chunk.decode('utf-8'))
                    except json.JSONDecodeError: continue

                error_detail = chunk.get('errorDetail')
                error_message = chunk.get('error')
                stream_content = chunk.get('stream')
                status = chunk.get('status')
                progress = chunk.get('progress')
                line_to_write = None

                if error_detail:
                    msg = error_detail.get('message', 'Unknown error detail')
                    final_error = msg
                    line_to_write = f"ERROR: {msg}"
                elif error_message and not final_error:
                    final_error = error_message
                    line_to_write = f"ERROR: {error_message}"

                if stream_content:
                    line = str(stream_content).strip()
                    if line: line_to_write = line
                elif status and not stream_content:
                    log_line = f"{status}"
                    if progress: log_line += f" - {progress}"
                    line_to_write = log_line

                if line_to_write:
                    self._write_log_sync(log_file_handle, log_lines_buffer, line_to_write)
        except Exception as e:
            logger.error(f"SyncStreamProcess Error: {e}", exc_info=True)
            final_error = final_error or f"Error processing stream: {e}"
            error_line = f"ERROR processing stream: {e}"
            self._write_log_sync(log_file_handle, log_lines_buffer, error_line)
        return final_error

    def _sync_run_build_core(
        self,
        build_id: PyObjectId,
        log_file_path: str,
        repo_url: str,
        branch: str,
        build_image_tag: str,
        temp_dir: str
    ) -> Tuple[str, str, List[str]]:
        """
        Synchronous core build logic. Skips login/push if registry not configured.
        Returns (final_status, final_message, log_lines_buffer).
        """
        log_lines_buffer: List[str] = []
        final_status = "failed"
        final_message = "Build did not complete."
        log_file = None
        registry_configured = are_registry_credentials_set() # Check credentials

        if not self.client or not self.api_client:
             return "failed", "Docker client unavailable", ["ERROR: Docker client unavailable at start of core build."]

        try:
            log_file = open(log_file_path, 'w', encoding='utf-8')
            self._write_log_sync(log_file, log_lines_buffer, f"Build {build_id} core task started at {datetime.now(timezone.utc).isoformat()}")
            self._write_log_sync(log_file, log_lines_buffer, f"Registry Configured: {registry_configured}")

            # --- Git Clone ---
            try:
                log_msg_clone = f"Cloning repository: {repo_url}, branch: {branch}..."
                self._write_log_sync(log_file, log_lines_buffer, log_msg_clone)
                repo = git.Repo.clone_from(repo_url, temp_dir, branch=branch, depth=1)
                cloned_commit = repo.head.commit.hexsha
                log_msg_clone_ok = f"Successfully cloned repository. Head commit: {cloned_commit}"
                self._write_log_sync(log_file, log_lines_buffer, log_msg_clone_ok)
            except GitCommandError as e:
                error_msg = f"Git clone failed. Command: '{' '.join(e.command)}'. Stderr: {e.stderr.strip()}"
                self._write_log_sync(log_file, log_lines_buffer, f"ERROR: {error_msg}")
                final_message = error_msg
                raise
            except Exception as e:
                 error_msg = f"Error during git clone: {e}"
                 self._write_log_sync(log_file, log_lines_buffer, f"ERROR: {error_msg}")
                 final_message = error_msg
                 raise

            # --- Docker Build ---
            try:
                log_msg_build = f"Starting Docker build: {build_image_tag}..."
                self._write_log_sync(log_file, log_lines_buffer, log_msg_build)
                build_stream = self.api_client.build(path=temp_dir, tag=build_image_tag, rm=True, forcerm=True, decode=True)
                build_error = self._process_stream_to_file_sync(iter(build_stream), log_file, log_lines_buffer)
                if build_error: raise BuildError(build_error, build_stream)
                log_msg_build_ok = f"Successfully built image: {build_image_tag}"
                self._write_log_sync(log_file, log_lines_buffer, log_msg_build_ok)
            except BuildError as e:
                error_msg = f"Docker build failed: {e.msg}"
                final_message = error_msg
                raise
            except APIError as e:
                error_msg = f"Docker API error during build: {e}"
                self._write_log_sync(log_file, log_lines_buffer, f"ERROR: {error_msg}")
                final_message = error_msg
                raise
            except Exception as e:
                error_msg = f"Unexpected error during build: {e}"
                self._write_log_sync(log_file, log_lines_buffer, f"ERROR: {error_msg}")
                final_message = error_msg
                raise

            # --- Docker Login & Push (Conditional) ---
            if registry_configured:
                logger.info(f"Build {build_id}: Registry configured, proceeding with login and push.")
                # --- Docker Login ---
                try:
                    log_msg_login = f"Logging into registry: {settings.REGISTRY_URL}..."
                    self._write_log_sync(log_file, log_lines_buffer, log_msg_login)
                    login_result = self.client.login(username=settings.REGISTRY_USER, password=settings.REGISTRY_PASSWORD, registry=settings.REGISTRY_URL)
                    login_status = login_result.get('Status', 'Unknown Status')
                    log_msg_login_status = f"Docker login status: {login_status}"
                    self._write_log_sync(log_file, log_lines_buffer, log_msg_login_status)
                    if login_status != "Login Succeeded": raise APIError(f"Login failed: {login_status}")
                except APIError as e:
                    error_msg = f"Docker login failed: {e}"
                    self._write_log_sync(log_file, log_lines_buffer, f"ERROR: {error_msg}")
                    final_message = error_msg
                    raise
                except Exception as e:
                     error_msg = f"Unexpected error during docker login: {e}"
                     self._write_log_sync(log_file, log_lines_buffer, f"ERROR: {error_msg}")
                     final_message = error_msg
                     raise

                # --- Docker Push ---
                try:
                    log_msg_push = f"Pushing image: {build_image_tag}..."
                    self._write_log_sync(log_file, log_lines_buffer, log_msg_push)
                    push_stream = self.api_client.push(build_image_tag, stream=True, decode=True)
                    push_error = self._process_stream_to_file_sync(iter(push_stream), log_file, log_lines_buffer)
                    if push_error: raise APIError(f"Push failed: {push_error}")
                    log_msg_push_ok = f"Successfully pushed image: {build_image_tag}"
                    self._write_log_sync(log_file, log_lines_buffer, log_msg_push_ok)

                    # Set success only if push completes
                    final_status = "success"
                    final_message = f"Successfully built and pushed {build_image_tag}"

                except APIError as e:
                    error_msg = f"Docker push failed: {e}"
                    final_message = error_msg
                    raise
                except Exception as e:
                    error_msg = f"Unexpected error during push: {e}"
                    self._write_log_sync(log_file, log_lines_buffer, f"ERROR: {error_msg}")
                    final_message = error_msg
                    raise
            else:
                # Registry not configured, skip login/push
                skip_msg = "Registry not configured. Skipping login and push."
                self._write_log_sync(log_file, log_lines_buffer, skip_msg)
                logger.info(f"Build {build_id}: {skip_msg}")
                # Build succeeded locally if we reached this point after build step
                final_status = "success"
                final_message = f"Successfully built image locally: {build_image_tag} (Registry not configured)"


        except Exception as build_err:
             if final_message == "Build did not complete.": final_message = f"Build failed with error: {build_err}"
             logger.error(f"Build {build_id}: Core build process failed. Final Message: {final_message}")
             if log_file and not log_file.closed:
                  self._write_log_sync(log_file, log_lines_buffer, f"ERROR: Build process failed: {build_err}")
             # final_status remains "failed"

        finally:
            if log_file and not log_file.closed:
                final_log_marker = f"--- BUILD CORE FINISHED (Sync Status: {final_status}) ---"
                self._write_log_sync(log_file, log_lines_buffer, final_log_marker)
                log_file.close()

        return final_status, final_message, log_lines_buffer


    # --- Main Async Task Function (Refactored) ---
    async def run_build_task_and_save_logs(
        self,
        build_id: PyObjectId,
        log_file_path: str,
        repo_url: str,
        branch: str,
        commit_sha: str,
        build_image_tag: str,
        build_status_repo: BuildStatusRepository,
        build_log_repo: BuildLogRepository
    ):
        logger.info(f"Build {build_id}: Async background task started.")
        temp_dir = None
        final_status = "failed"
        final_message = "Task initialization failed."
        log_lines_buffer = []

        if not self.client or not self.api_client:
            logger.error(f"Build {build_id}: Docker client(s) not available. Aborting early.")
            final_message = "Docker client unavailable"
            try: build_status_repo.update_build_status(build_id, {"status": "failed", "message": final_message, "completed_at": datetime.now(timezone.utc)})
            except Exception as db_err: logger.error(f"Build {build_id}: DB error updating status after Docker client failure: {db_err}")
            return

        try:
            logger.info(f"Build {build_id}: Creating temp directory...")
            temp_dir = await asyncio.to_thread(tempfile.mkdtemp, prefix=f"cypher_build_{build_id}_")
            logger.info(f"Build {build_id}: Temp directory created: {temp_dir}")

            logger.info(f"Build {build_id}: Updating status to 'running'...")
            update_success = await asyncio.to_thread(
                build_status_repo.update_build_status,
                build_id, {"status": "running", "started_at": datetime.now(timezone.utc)}
            )
            if not update_success: raise Exception("Failed to update build status to running in DB.")
            logger.info(f"Build {build_id}: Status updated to 'running'.")

            # --- Mandatory Delay ---
            delay_seconds = 10
            logger.info(f"Build {build_id}: Waiting for {delay_seconds} seconds before starting core build...")
            await asyncio.sleep(delay_seconds)
            logger.info(f"Build {build_id}: Delay finished. Starting core build logic in thread...")

            # --- Run Core Build Logic (in thread) ---
            final_status, final_message, log_lines_buffer = await asyncio.to_thread(
                self._sync_run_build_core,
                build_id, log_file_path, repo_url, branch, build_image_tag, temp_dir
            )
            logger.info(f"Build {build_id}: Core build logic thread finished with status: {final_status}")

        except Exception as task_err:
             logger.error(f"Build {build_id}: Async task wrapper caught error: {task_err}", exc_info=True)
             final_status = "failed"
             final_message = final_message if final_message != "Task initialization failed." else f"Task failed: {task_err}"

        finally:
            # Give SSE a tiny bit more time after file close/thread finish
            await asyncio.sleep(0.2)

            logger.info(f"Build {build_id}: Entering finally block. Final status: {final_status}")

            # --- Save Logs to Database (in thread) ---
            if log_lines_buffer:
                log_documents = [{"build_id": build_id, "timestamp": datetime.now(timezone.utc), "type": "error" if line.startswith("ERROR:") else "log", "message": line.removeprefix("ERROR: ").strip() if line.startswith("ERROR:") else line} for line in log_lines_buffer]
                try:
                    logger.info(f"Build {build_id}: Saving {len(log_documents)} logs to database...")
                    save_func = functools.partial(build_log_repo.add_many_logs, log_documents)
                    inserted_count = await asyncio.to_thread(save_func)
                    logger.info(f"Build {build_id}: Saved {inserted_count}/{len(log_lines_buffer)} logs.")
                except Exception as db_err:
                    logger.error(f"Build {build_id}: Failed to save logs to database: {db_err}", exc_info=True)
                    final_message += " (Failed to save logs to DB)"
            else:
                 logger.warning(f"Build {build_id}: No logs were buffered.")

            # --- Update Final Build Status (in thread) ---
            try:
                logger.info(f"Build {build_id}: Updating final status to '{final_status}'...")
                update_payload = {"status": final_status, "completed_at": datetime.now(timezone.utc), "message": final_message}
                if final_status == "success" and are_registry_credentials_set():
                     update_payload["image_tag"] = build_image_tag
                elif final_status == "success" and not are_registry_credentials_set():
                     pass

                update_func = functools.partial(build_status_repo.update_build_status, build_id, update_payload)
                success = await asyncio.to_thread(update_func)
                logger.info(f"Build {build_id}: Final status update successful: {success}")
            except Exception as db_err:
                 logger.error(f"Build {build_id}: DB error updating final status: {db_err}", exc_info=True)

            # --- Cleanup (Run blocking I/O in thread) ---
            def cleanup_blocking_sync():
                if temp_dir and os.path.exists(temp_dir):
                    logger.info(f"Build {build_id}: Cleaning up temp directory: {temp_dir}")
                    try: shutil.rmtree(temp_dir)
                    except Exception as e: logger.error(f"Build {build_id}: Failed to remove temp dir {temp_dir}: {e}")
                # Temporarily disable log file removal for debugging
                # if os.path.exists(log_file_path):
                #      logger.info(f"Build {build_id}: Cleaning up log file: {log_file_path}")
                #      try: os.remove(log_file_path)
                #      except Exception as e: logger.error(f"Build {build_id}: Failed to remove log file {log_file_path}: {e}")
                if os.path.exists(log_file_path):
                     logger.warning(f"Build {build_id}: Log file removal temporarily disabled for debugging: {log_file_path}")


            await asyncio.to_thread(cleanup_blocking_sync)

            logger.info(f"Build {build_id}: Background task finished.")


# Single instance
docker_service = DockerService()
