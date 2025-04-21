import docker
from docker.errors import BuildError, APIError, ImageNotFound
import logging
import os
import tempfile
import shutil
import git
from typing import Optional, Tuple, Callable, AsyncGenerator, Dict, Any, Generator
import asyncio
import functools

from config import settings

logger = logging.getLogger(__name__)


class DockerBuildService:
    """Handles Docker image building from Git repositories.

    Provides functionality to clone a repository, build a Docker image from it,
    stream build logs, and manage the underlying Docker client connection.

    Attributes:
        client: The Docker client instance obtained from the environment.
    """

    def __init__(self):
        """Initializes the DockerBuildService and the Docker client."""
        try:
            self.client = docker.from_env()
            logger.info("Docker client initialized successfully.")
        except Exception as e:
            logger.error(f"Failed to initialize Docker client: {e}", exc_info=True)
            self.client = None

    def _process_sync_stream(
        self,
        build_stream: Generator[Dict[str, Any], None, None],
        log_file_path: str,
        log_callback: Optional[Callable[[str], Any]] = None,
        loop: Optional[asyncio.AbstractEventLoop] = None,
    ) -> Tuple[bool, Optional[str]]:
        """Processes the synchronous Docker build log stream.

        Iterates through the build log generator provided by docker-py's build API,
        writes logs to a file, calls an optional asynchronous callback for each line
        (scheduling it on the provided event loop), and determines the final build
        status and image ID.

        Args:
            build_stream (Generator[Dict[str, Any], None, None]): The synchronous generator yielding build log chunks.
            log_file_path (str): Path to the file where logs should be written.
            log_callback (Optional[Callable[[str], Any]]): An optional async function to call with each log line.
                                                           It's expected to return a coroutine.
            loop (Optional[asyncio.AbstractEventLoop]): The event loop to schedule the log_callback on. Required if log_callback is provided.

        Returns:
            Tuple[bool, Optional[str]]: A tuple containing:
                - bool: True if the build was successful, False otherwise.
                - Optional[str]: The built image ID if successful, None otherwise.
        """
        image_id = None
        build_successful = False
        last_log_line = ""
        has_seen_success_message = False

        try:
            with open(log_file_path, "a") as log_file:
                for chunk in build_stream:
                    if "stream" in chunk:
                        line = chunk["stream"].strip()
                        if line:
                            log_file.write(line + "\n")
                            log_file.flush()
                            last_log_line = line
                            if log_callback and loop:
                                try:
                                    coro = log_callback(line)
                                    asyncio.run_coroutine_threadsafe(coro, loop)
                                except Exception as call_e:
                                    logger.error(
                                        f"Error scheduling log callback: {call_e}",
                                        exc_info=True,
                                    )

                            if "Successfully built" in line:
                                logger.info(
                                    "Detected 'Successfully built' message in stream (sync)."
                                )
                                has_seen_success_message = True

                    elif "aux" in chunk and "ID" in chunk["aux"]:
                        image_id = chunk["aux"]["ID"]
                        logger.info(
                            f"Detected image ID during build (sync): {image_id}"
                        )
                    elif "errorDetail" in chunk:
                        error_message = chunk["errorDetail"]["message"]
                        log_file.write(f"ERROR: {error_message}\n")
                        log_file.flush()
                        if log_callback and loop:
                            try:
                                coro = log_callback(f"ERROR: {error_message}")
                                asyncio.run_coroutine_threadsafe(coro, loop)
                            except Exception as call_e:
                                logger.error(
                                    f"Error scheduling error log callback: {call_e}",
                                    exc_info=True,
                                )
                        logger.error(
                            f"Build error reported by Docker (sync): {error_message}"
                        )
                        build_successful = False  # Explicitly mark as failed

            # Final success determination based on seeing the success message
            build_successful = has_seen_success_message

            if build_successful:
                logger.info(
                    "Final determination: Build successful (success message was detected)."
                )
            else:
                logger.warning(
                    "Final determination: Build failed (success message not detected or error occurred)."
                )

            logger.info(
                f"Finished processing build stream (sync). Success: {build_successful}, Image ID: {image_id}"
            )
            return build_successful, image_id

        except Exception as e:
            logger.error(
                f"Error processing build stream or writing logs (sync): {e}",
                exc_info=True,
            )
            try:
                with open(log_file_path, "a") as log_file:
                    log_file.write(f"FATAL ERROR processing logs: {e}\n")
            except Exception as log_err:
                logger.error(f"Could not even write fatal error to log file: {log_err}")
            return False, None

    async def build_image_from_git(
        self,
        repo_url: str,
        branch: str,
        tag_version: str,
        repo_full_name: str,
        build_id: str,
        log_file_path: str,
        log_callback: Optional[Callable[[str], Any]] = None,
    ) -> Tuple[bool, Optional[str], Optional[str], Optional[str]]:
        """Clones a Git repository, builds a Docker image, streams logs, and returns status.

        Handles cloning, running the Docker build (which is blocking but processed in a thread),
        processing the log stream via `_process_sync_stream`, and cleaning up.

        Args:
            repo_url (str): The URL of the Git repository to clone.
            branch (str): The specific branch to clone and build.
            tag_version (str): The version tag to apply to the built image (e.g., 'latest', 'v1.2').
            repo_full_name (str): The full repository name (e.g., 'owner/repo') used for tagging.
            build_id (str): A unique identifier for this build, used for logging and temp dirs.
            log_file_path (str): The path to the file where build logs should be stored.
            log_callback (Optional[Callable[[str], Any]]): An optional async function to call with each log line.

        Returns:
            Tuple[bool, Optional[str], Optional[str], Optional[str]]: A tuple containing:
                - success (bool): True if the build was successful, False otherwise.
                - error_message (Optional[str]): An error message if the build failed, None otherwise.
                - image_id (Optional[str]): The Docker image ID if the build was successful, None otherwise.
                - final_image_tag (Optional[str]): The full tag applied to the image if successful, None otherwise.
        """
        if not self.client:
            logger.error("Docker client not available. Cannot build image.")
            return False, "Docker client unavailable", None, None

        registry_prefix = (
            settings.DOCKER_REGISTRY_URL.rstrip("/") + "/"
            if settings.DOCKER_REGISTRY_URL
            else ""
        )
        image_tag_base = f"{repo_full_name.lower().replace('/', '-')}"
        final_image_tag = f"{registry_prefix}{image_tag_base}:{tag_version}"
        logger.info(
            f"Attempting to build image with tag: {final_image_tag} for build ID: {build_id}"
        )

        temp_dir = tempfile.mkdtemp(prefix=f"cypher_build_{build_id}_")
        logger.info(f"Created temporary directory for cloning: {temp_dir}")

        try:
            logger.info(
                f"Cloning repository {repo_url} branch {branch} into {temp_dir}"
            )
            await asyncio.to_thread(
                git.Repo.clone_from, repo_url, temp_dir, branch=branch, depth=1
            )
            logger.info(f"Successfully cloned repository for build {build_id}")

            logger.info(
                f"Starting Docker build process for {final_image_tag} from path {temp_dir}"
            )
            # Note: self.client.api.build is synchronous and returns a generator
            build_stream_generator = self.client.api.build(
                path=temp_dir,
                tag=final_image_tag,
                rm=True,  # Remove intermediate containers
                pull=True,  # Attempt to pull base images
                decode=True,  # Decode JSON stream objects
                nocache=False,  # Use Docker cache
            )

            logger.info(f"Processing build stream in thread for build {build_id}")
            current_loop = asyncio.get_running_loop()
            # Use functools.partial to pass arguments to the sync function run in the thread
            process_func = functools.partial(
                self._process_sync_stream,
                build_stream=build_stream_generator,
                log_file_path=log_file_path,
                log_callback=log_callback,
                loop=current_loop,
            )
            success, image_id = await asyncio.to_thread(process_func)
            logger.info(
                f"Finished processing build stream thread for build {build_id}. Success: {success}"
            )

            if success:
                logger.info(
                    f"Docker build successful for {final_image_tag}. Image ID: {image_id}"
                )
                return True, None, image_id, final_image_tag
            else:
                logger.error(f"Docker build failed for {final_image_tag}.")
                error_detail = "Build failed. Check logs for details."
                # Attempt to get last line from log file as error detail
                try:
                    with open(log_file_path, "r") as f:
                        lines = f.readlines()
                        if lines:
                            error_detail = lines[-1].strip()
                except Exception:
                    pass
                return False, error_detail, None, None

        except git.GitCommandError as e:
            logger.error(
                f"Git clone failed for {repo_url} branch {branch}: {e}", exc_info=True
            )
            error_msg = f"Git clone failed: {e.stderr}"
            with open(log_file_path, "a") as log_file:
                log_file.write(f"ERROR: {error_msg}\n")
            if log_callback:
                try:
                    asyncio.run_coroutine_threadsafe(
                        log_callback(f"ERROR: {error_msg}"), asyncio.get_running_loop()
                    )
                except Exception as call_e:
                    logger.error(
                        f"Error scheduling git error log callback: {call_e}",
                        exc_info=True,
                    )
            return False, error_msg, None, None
        except BuildError as e:
            logger.error(
                f"Docker build failed (BuildError) for {final_image_tag}: {e}",
                exc_info=True,
            )
            error_msg = f"Docker build failed: {getattr(e, 'msg', str(e))}"
            with open(log_file_path, "a") as log_file:
                log_file.write(f"ERROR: {error_msg}\n")
            if log_callback:
                try:
                    asyncio.run_coroutine_threadsafe(
                        log_callback(f"ERROR: {error_msg}"), asyncio.get_running_loop()
                    )
                except Exception as call_e:
                    logger.error(
                        f"Error scheduling build error log callback: {call_e}",
                        exc_info=True,
                    )
            return False, error_msg, None, None
        except APIError as e:
            logger.error(
                f"Docker API error during build for {final_image_tag}: {e}",
                exc_info=True,
            )
            error_msg = f"Docker API error: {e.explanation}"
            with open(log_file_path, "a") as log_file:
                log_file.write(f"ERROR: {error_msg}\n")
            if log_callback:
                try:
                    asyncio.run_coroutine_threadsafe(
                        log_callback(f"ERROR: {error_msg}"), asyncio.get_running_loop()
                    )
                except Exception as call_e:
                    logger.error(
                        f"Error scheduling API error log callback: {call_e}",
                        exc_info=True,
                    )
            return False, error_msg, None, None
        except Exception as e:
            logger.error(
                f"Unexpected error during build process for {final_image_tag}: {e}",
                exc_info=True,
            )
            error_msg = f"Unexpected build error: {e}"
            try:
                with open(log_file_path, "a") as log_file:
                    log_file.write(f"ERROR: {error_msg}\n")
                if log_callback:
                    try:
                        asyncio.run_coroutine_threadsafe(
                            log_callback(f"ERROR: {error_msg}"),
                            asyncio.get_running_loop(),
                        )
                    except Exception as call_e:
                        logger.error(
                            f"Error scheduling unexpected error log callback: {call_e}",
                            exc_info=True,
                        )
            except Exception as log_err:
                logger.error(
                    f"Could not write unexpected build error to log file: {log_err}"
                )
            return False, error_msg, None, None
        finally:
            try:
                await asyncio.to_thread(shutil.rmtree, temp_dir)
                logger.info(f"Removed temporary directory: {temp_dir}")
            except Exception as e:
                logger.error(
                    f"Failed to remove temporary directory {temp_dir}: {e}",
                    exc_info=True,
                )


# Singleton instance of the DockerBuildService
docker_build_service = DockerBuildService()
