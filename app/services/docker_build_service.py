import docker
from docker.errors import BuildError, APIError, ImageNotFound
import logging
import os
import tempfile
import shutil
import git
from typing import Optional, Tuple, Callable, AsyncGenerator, Dict, Any, Generator # Added Generator
import asyncio
import functools # Import functools

# Import settings from config
from config import settings

logger = logging.getLogger(__name__)

class DockerBuildService:
    """Handles Docker image building from Git repositories."""

    def __init__(self):
        try:
            self.client = docker.from_env()
            logger.info("Docker client initialized successfully.")
        except Exception as e:
            logger.error(f"Failed to initialize Docker client: {e}", exc_info=True)
            self.client = None

    # This function now runs synchronously within a thread
    def _process_sync_stream(
        self,
        build_stream: Generator[Dict[str, Any], None, None],
        log_file_path: str,
        log_callback: Optional[Callable[[str], Any]] = None,
        loop: Optional[asyncio.AbstractEventLoop] = None # Pass the loop for run_coroutine_threadsafe
    ) -> Tuple[bool, Optional[str]]:
        """Processes the Docker build log stream synchronously."""
        image_id = None
        build_successful = False # Start assuming failure
        last_log_line = ""
        has_seen_success_message = False # Flag to track if success message was seen

        try:
            with open(log_file_path, 'a') as log_file:
                for chunk in build_stream: # Use standard 'for' loop
                    if 'stream' in chunk:
                        line = chunk['stream'].strip()
                        if line:
                            log_file.write(line + '\n')
                            log_file.flush()
                            last_log_line = line
                            if log_callback and loop:
                                # Directly schedule the coroutine returned by log_callback(line)
                                # Assumes log_callback returns a coroutine (like send_log_update)
                                try:
                                    # Create the coroutine by calling the callback
                                    coro = log_callback(line)
                                    # Schedule it to run on the main event loop
                                    asyncio.run_coroutine_threadsafe(coro, loop)
                                except Exception as call_e:
                                     logger.error(f"Error scheduling log callback: {call_e}", exc_info=True)


                            # Check for success message within the stream
                            if "Successfully built" in line:
                                logger.info("Detected 'Successfully built' message in stream (sync).")
                                has_seen_success_message = True # Set the flag

                    elif 'aux' in chunk and 'ID' in chunk['aux']:
                        image_id = chunk['aux']['ID']
                        logger.info(f"Detected image ID during build (sync): {image_id}")
                    elif 'errorDetail' in chunk:
                        error_message = chunk['errorDetail']['message']
                        log_file.write(f"ERROR: {error_message}\n")
                        log_file.flush()
                        if log_callback and loop:
                             try:
                                 coro = log_callback(f"ERROR: {error_message}")
                                 asyncio.run_coroutine_threadsafe(coro, loop)
                             except Exception as call_e:
                                 logger.error(f"Error scheduling error log callback: {call_e}", exc_info=True)

                        logger.error(f"Build error reported by Docker (sync): {error_message}")
                        build_successful = False # Explicitly mark as failed on error

            build_successful = has_seen_success_message

            if build_successful:
                 logger.info("Final determination: Build successful (success message was detected).")
            else:
                 logger.warning("Final determination: Build failed (success message not detected or error occurred).")


            logger.info(f"Finished processing build stream (sync). Success: {build_successful}, Image ID: {image_id}")
            return build_successful, image_id

        except Exception as e:
            logger.error(f"Error processing build stream or writing logs (sync): {e}", exc_info=True)
            try:
                with open(log_file_path, 'a') as log_file:
                    log_file.write(f"FATAL ERROR processing logs: {e}\n")
            except Exception as log_err:
                 logger.error(f"Could not even write fatal error to log file: {log_err}")
            return False, None

    async def build_image_from_git(
        self,
        repo_url: str,
        branch: str,
        tag_version: str,
        repo_full_name: str, # e.g., "owner/repo"
        build_id: str, # For logging correlation
        log_file_path: str,
        log_callback: Optional[Callable[[str], Any]] = None
    ) -> Tuple[bool, Optional[str], Optional[str], Optional[str]]:
        """
        Clones a Git repo, builds a Docker image, streams logs, and returns status.

        Returns: (success: bool, error_message: Optional[str], image_id: Optional[str], final_image_tag: Optional[str])
        """
        if not self.client:
            logger.error("Docker client not available. Cannot build image.")
            return False, "Docker client unavailable", None, None

        registry_prefix = settings.DOCKER_REGISTRY_URL.rstrip('/') + "/" if settings.DOCKER_REGISTRY_URL else ""
        image_tag_base = f"{repo_full_name.lower().replace('/', '-')}"
        final_image_tag = f"{registry_prefix}{image_tag_base}:{tag_version}"
        logger.info(f"Attempting to build image with tag: {final_image_tag} for build ID: {build_id}")

        temp_dir = tempfile.mkdtemp(prefix=f"cypher_build_{build_id}_")
        logger.info(f"Created temporary directory for cloning: {temp_dir}")

        try:
            # 1. Clone the repository (blocking)
            logger.info(f"Cloning repository {repo_url} branch {branch} into {temp_dir}")
            await asyncio.to_thread(git.Repo.clone_from, repo_url, temp_dir, branch=branch, depth=1)
            logger.info(f"Successfully cloned repository for build {build_id}")

            # 2. Start the Docker build (blocking API call, returns sync generator)
            logger.info(f"Starting Docker build process for {final_image_tag} from path {temp_dir}")
            build_stream_generator = self.client.api.build(
                path=temp_dir,
                tag=final_image_tag,
                rm=True,
                pull=True,
                decode=True,
                nocache=False
            )

            # 3. Process the synchronous build stream in a separate thread
            logger.info(f"Processing build stream in thread for build {build_id}")
            current_loop = asyncio.get_running_loop()
            process_func = functools.partial(
                self._process_sync_stream,
                build_stream=build_stream_generator,
                log_file_path=log_file_path,
                log_callback=log_callback,
                loop=current_loop
            )
            success, image_id = await asyncio.to_thread(process_func)
            logger.info(f"Finished processing build stream thread for build {build_id}. Success: {success}")


            if success:
                logger.info(f"Docker build successful for {final_image_tag}. Image ID: {image_id}")
                return True, None, image_id, final_image_tag
            else:
                logger.error(f"Docker build failed for {final_image_tag}.")
                error_detail = "Build failed. Check logs for details."
                try:
                    with open(log_file_path, 'r') as f:
                        lines = f.readlines()
                        if lines: error_detail = lines[-1].strip()
                except Exception: pass
                return False, error_detail, None, None

        except git.GitCommandError as e:
            logger.error(f"Git clone failed for {repo_url} branch {branch}: {e}", exc_info=True)
            with open(log_file_path, 'a') as log_file:
                 log_file.write(f"ERROR: Git clone failed: {e.stderr}\n")
            # Schedule callback safely
            if log_callback:
                try:
                    asyncio.run_coroutine_threadsafe(log_callback(f"ERROR: Git clone failed: {e.stderr}"), asyncio.get_running_loop())
                except Exception as call_e:
                    logger.error(f"Error scheduling git error log callback: {call_e}", exc_info=True)
            return False, f"Git clone failed: {e.stderr}", None, None
        except BuildError as e:
            logger.error(f"Docker build failed (BuildError) for {final_image_tag}: {e}", exc_info=True)
            error_msg = f"Docker build failed: {getattr(e, 'msg', str(e))}"
            with open(log_file_path, 'a') as log_file:
                 log_file.write(f"ERROR: {error_msg}\n")
            # Schedule callback safely
            if log_callback:
                try:
                    asyncio.run_coroutine_threadsafe(log_callback(f"ERROR: {error_msg}"), asyncio.get_running_loop())
                except Exception as call_e:
                    logger.error(f"Error scheduling build error log callback: {call_e}", exc_info=True)
            return False, error_msg, None, None
        except APIError as e:
            logger.error(f"Docker API error during build for {final_image_tag}: {e}", exc_info=True)
            error_msg = f"Docker API error: {e.explanation}"
            with open(log_file_path, 'a') as log_file:
                 log_file.write(f"ERROR: {error_msg}\n")
            # Schedule callback safely
            if log_callback:
                try:
                    asyncio.run_coroutine_threadsafe(log_callback(f"ERROR: {error_msg}"), asyncio.get_running_loop())
                except Exception as call_e:
                    logger.error(f"Error scheduling API error log callback: {call_e}", exc_info=True)
            return False, error_msg, None, None
        except Exception as e:
            logger.error(f"Unexpected error during build process for {final_image_tag}: {e}", exc_info=True)
            error_msg = f"Unexpected build error: {e}"
            try:
                 with open(log_file_path, 'a') as log_file:
                      log_file.write(f"ERROR: {error_msg}\n")
                 # Schedule callback safely
                 if log_callback:
                     try:
                         asyncio.run_coroutine_threadsafe(log_callback(f"ERROR: {error_msg}"), asyncio.get_running_loop())
                     except Exception as call_e:
                         logger.error(f"Error scheduling unexpected error log callback: {call_e}", exc_info=True)
            except Exception as log_err:
                 logger.error(f"Could not write unexpected build error to log file: {log_err}")
            return False, error_msg, None, None
        finally:
            # 4. Clean up the temporary directory
            try:
                await asyncio.to_thread(shutil.rmtree, temp_dir)
                logger.info(f"Removed temporary directory: {temp_dir}")
            except Exception as e:
                logger.error(f"Failed to remove temporary directory {temp_dir}: {e}", exc_info=True)

# Instantiate the service
docker_build_service = DockerBuildService()
