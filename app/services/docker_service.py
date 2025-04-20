import logging
import docker
import tempfile
import shutil
import os
import git # Import gitpython
import asyncio # Import asyncio
from git import GitCommandError # Import specific error
# Import specific docker errors
from docker.errors import BuildError, APIError, DockerException, NotFound as DockerNotFound, ContainerError
from typing import Iterator, Any, List, Dict, Tuple, Optional # Added Optional
from datetime import datetime, timezone # Added timezone
from bson import ObjectId # Import ObjectId
import functools # Import functools for partial
import json # Import json for stream decoding
import re # Import re for regex matching
import uuid # Import uuid for generating unique names
# Import ThreadPoolExecutor for concurrent stats fetching
from concurrent.futures import ThreadPoolExecutor, as_completed

# Import repositories - needed for saving logs/status
# Assuming execution context allows direct import from sibling/parent directories
from repositories.build_status_repository import BuildStatusRepository
from repositories.build_log_repository import BuildLogRepository
# Import necessary models
# Use the updated models including LabelPair
from models import PyObjectId, ContainerRuntimeConfig, VolumeMapping, EnvironmentVariable, LabelPair, PortMapping

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

# Regex to extract repo_full_name from image tag (adjust if tag format changes)
IMAGE_TAG_REPO_REGEX = re.compile(r"^(?:[^/]+/)?([^:]+)/([^:]+):.*$")
# Label used to identify containers managed by this service for a specific repo
CYPHER_REPO_LABEL = "cypher.repo_full_name"


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

    # --- Build Related Synchronous Helpers (Keep as is) ---
    def _write_log_sync(self, handle: Any, buffer: List[str], message: str):
        # ... (unchanged) ...
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
        # ... (unchanged) ...
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
        # ... (unchanged) ...
        log_lines_buffer: List[str] = []
        final_status = "failed"
        final_message = "Build did not complete."
        log_file = None
        registry_configured = are_registry_credentials_set()

        if not self.client or not self.api_client:
             return "failed", "Docker client unavailable", ["ERROR: Docker client unavailable at start of core build."]

        try:
            log_file = open(log_file_path, 'w', encoding='utf-8')
            self._write_log_sync(log_file, log_lines_buffer, f"Build {build_id} core task started at {datetime.now(timezone.utc).isoformat()}")
            self._write_log_sync(log_file, log_lines_buffer, f"Registry Configured: {registry_configured}")

            # Git Clone
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

            # Docker Build
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

            # Docker Login & Push (Conditional)
            if registry_configured:
                logger.info(f"Build {build_id}: Registry configured, proceeding with login and push.")
                try: # Login
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
                try: # Push
                    log_msg_push = f"Pushing image: {build_image_tag}..."
                    self._write_log_sync(log_file, log_lines_buffer, log_msg_push)
                    push_stream = self.api_client.push(build_image_tag, stream=True, decode=True)
                    push_error = self._process_stream_to_file_sync(iter(push_stream), log_file, log_lines_buffer)
                    if push_error: raise APIError(f"Push failed: {push_error}")
                    log_msg_push_ok = f"Successfully pushed image: {build_image_tag}"
                    self._write_log_sync(log_file, log_lines_buffer, log_msg_push_ok)
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
                skip_msg = "Registry not configured. Skipping login and push."
                self._write_log_sync(log_file, log_lines_buffer, skip_msg)
                logger.info(f"Build {build_id}: {skip_msg}")
                final_status = "success"
                final_message = f"Successfully built image locally: {build_image_tag} (Registry not configured)"

        except Exception as build_err:
             if final_message == "Build did not complete.": final_message = f"Build failed with error: {build_err}"
             logger.error(f"Build {build_id}: Core build process failed. Final Message: {final_message}")
             if log_file and not log_file.closed:
                  self._write_log_sync(log_file, log_lines_buffer, f"ERROR: Build process failed: {build_err}")

        finally:
            if log_file and not log_file.closed:
                final_log_marker = f"--- BUILD CORE FINISHED (Sync Status: {final_status}) ---"
                self._write_log_sync(log_file, log_lines_buffer, final_log_marker)
                log_file.close()

        return final_status, final_message, log_lines_buffer

    # --- Main Async Build Task (Corrected image_tag saving) ---
    async def run_build_task_and_save_logs(
        self, build_id: PyObjectId, log_file_path: str, repo_url: str, branch: str,
        commit_sha: str, build_image_tag: str, build_status_repo: BuildStatusRepository,
        build_log_repo: BuildLogRepository,
    ):
        # ... (unchanged) ...
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
            update_success = await asyncio.to_thread(build_status_repo.update_build_status, build_id, {"status": "running", "started_at": datetime.now(timezone.utc)})
            if not update_success: raise Exception("Failed to update build status to running in DB.")
            logger.info(f"Build {build_id}: Status updated to 'running'.")

            delay_seconds = 10
            logger.info(f"Build {build_id}: Waiting for {delay_seconds} seconds before starting core build...")
            await asyncio.sleep(delay_seconds)
            logger.info(f"Build {build_id}: Delay finished. Starting core build logic in thread...")

            final_status, final_message, log_lines_buffer = await asyncio.to_thread(
                self._sync_run_build_core, build_id, log_file_path, repo_url, branch, build_image_tag, temp_dir
            )
            logger.info(f"Build {build_id}: Core build logic thread finished with status: {final_status}")

        except Exception as task_err:
             logger.error(f"Build {build_id}: Async task wrapper caught error: {task_err}", exc_info=True)
             final_status = "failed"
             final_message = final_message if final_message != "Task initialization failed." else f"Task failed: {task_err}"

        finally:
            await asyncio.sleep(0.2)
            logger.info(f"Build {build_id}: Entering finally block. Final status: {final_status}")

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

            try:
                logger.info(f"Build {build_id}: Updating final status to '{final_status}'...")
                update_payload = {"status": final_status, "completed_at": datetime.now(timezone.utc), "message": final_message}
                if final_status == "success":
                    update_payload["image_tag"] = build_image_tag
                # --- END CORRECTION ---
                update_func = functools.partial(build_status_repo.update_build_status, build_id, update_payload)
                success = await asyncio.to_thread(update_func)
                logger.info(f"Build {build_id}: Final status update successful: {success}")
            except Exception as db_err:
                 logger.error(f"Build {build_id}: DB error updating final status: {db_err}", exc_info=True)

            def cleanup_blocking_sync():
                if temp_dir and os.path.exists(temp_dir):
                    logger.info(f"Build {build_id}: Cleaning up temp directory: {temp_dir}")
                    try: shutil.rmtree(temp_dir)
                    except Exception as e: logger.error(f"Build {build_id}: Failed to remove temp dir {temp_dir}: {e}")

            await asyncio.to_thread(cleanup_blocking_sync)
            logger.info(f"Build {build_id}: Background task finished.")


    # --- Container Status and Resource Calculation Helpers (Keep as is) ---
    def _calculate_cpu_percent(self, stats: Dict[str, Any]) -> float:
        # ... (unchanged) ...
        try:
            cpu_stats = stats.get('cpu_stats', {})
            precpu_stats = stats.get('precpu_stats', {})
            cpu_delta = cpu_stats.get('cpu_usage', {}).get('total_usage', 0) - \
                        precpu_stats.get('cpu_usage', {}).get('total_usage', 0)
            system_cpu_delta = cpu_stats.get('system_cpu_usage', 0) - \
                               precpu_stats.get('system_cpu_usage', 0)
            number_cpus = cpu_stats.get('online_cpus') # Use online_cpus if available
            if number_cpus is None:
                number_cpus = len(cpu_stats.get('cpu_usage', {}).get('percpu_usage', [1])) # Fallback

            if system_cpu_delta > 0.0 and cpu_delta > 0.0 and number_cpus > 0:
                cpu_percent = (cpu_delta / system_cpu_delta) * number_cpus * 100.0
                return round(cpu_percent, 2)
            return 0.0
        except (KeyError, TypeError, ZeroDivisionError) as e:
            logger.warning(f"Could not calculate CPU percent: {e}. Stats: {stats}")
            return 0.0

    def _calculate_memory_mb(self, stats: Dict[str, Any]) -> float:
        # ... (unchanged) ...
        try:
            memory_stats = stats.get('memory_stats', {})
            usage_bytes = memory_stats.get('usage')
            if usage_bytes is not None:
                return round(usage_bytes / (1024 * 1024), 2) # Convert bytes to MB
            return 0.0
        except (KeyError, TypeError) as e:
            logger.warning(f"Could not calculate Memory MB: {e}. Stats: {stats}")
            return 0.0

    # --- Container Listing (OPTIMIZED with concurrent stats fetching) ---
    def list_managed_containers(self, repo_full_name_filter: Optional[str] = None) -> List[Dict[str, Any]]:
        if not self.client:
            logger.warning("Cannot list containers: Docker client not available.")
            return []

        filters = {"label": f"{CYPHER_REPO_LABEL}"}
        if repo_full_name_filter:
            filters["label"] = f"{CYPHER_REPO_LABEL}={repo_full_name_filter}"

        try:
            managed_containers = self.client.containers.list(all=True, filters=filters)
            logger.info(f"Found {len(managed_containers)} containers matching filter: {filters}")
        except Exception as e:
            logger.error(f"Error listing containers with filter {filters}: {e}", exc_info=True)
            return []

        repo_containers: Dict[str, Dict[str, Any]] = {}
        running_container_objects: Dict[str, Any] = {} # Store running container objects by full ID

        # First pass: Collect basic info and identify running containers
        for container in managed_containers:
            repo_full_name = container.labels.get(CYPHER_REPO_LABEL)
            if not repo_full_name:
                logger.warning(f"Container {container.short_id} matched filter but missing label {CYPHER_REPO_LABEL}")
                continue

            if repo_full_name not in repo_containers:
                repo_containers[repo_full_name] = {
                    "repo_full_name": repo_full_name,
                    "containers": [],
                    "total_memory_mb": 0.0, # Initialize resource counters
                    "total_cpu_percent": 0.0,
                }

            ports_info = {}
            try:
                raw_ports = container.attrs.get('NetworkSettings', {}).get('Ports', {})
                if raw_ports:
                    for internal_port_proto, host_bindings in raw_ports.items():
                        if host_bindings:
                            host_ip = host_bindings[0].get('HostIp', '0.0.0.0')
                            host_port = host_bindings[0].get('HostPort')
                            if host_port: ports_info[internal_port_proto] = f"{host_ip}:{host_port}"
                            else: ports_info[internal_port_proto] = "exposed"
                        else: ports_info[internal_port_proto] = "exposed"
            except Exception as port_err:
                logger.warning(f"Could not parse ports for container {container.short_id}: {port_err}")

            container_detail = {
                "id": container.short_id,
                "full_id": container.id, # Keep full ID temporarily
                "name": container.name,
                "status": container.status,
                "image": container.image.tags[0] if container.image.tags else 'N/A',
                "ports": ports_info,
            }
            repo_containers[repo_full_name]["containers"].append(container_detail)

            # Store running container object for stats fetching later
            if container.status == 'running':
                running_container_objects[container.id] = container

        # Second pass: Fetch stats concurrently for running containers
        container_stats: Dict[str, Dict[str, Any]] = {}
        if running_container_objects:
            logger.info(f"Fetching stats concurrently for {len(running_container_objects)} running containers...")
            # Use ThreadPoolExecutor to fetch stats in parallel
            with ThreadPoolExecutor(max_workers=10) as executor: # Adjust max_workers as needed
                future_to_container_id = {
                    executor.submit(c.stats, stream=False): cid
                    for cid, c in running_container_objects.items()
                }
                for future in as_completed(future_to_container_id):
                    container_id = future_to_container_id[future]
                    try:
                        stats_data = future.result()
                        container_stats[container_id] = stats_data
                    except APIError as stats_err:
                        logger.warning(f"API error getting stats for container ID {container_id}: {stats_err}")
                    except Exception as stats_err:
                        logger.warning(f"Unexpected error getting stats for container ID {container_id}: {stats_err}")
            logger.info(f"Finished fetching stats. Got results for {len(container_stats)} containers.")


        # Third pass: Aggregate results and calculate resources
        aggregated_status = []
        for repo_name, data in repo_containers.items():
            containers_list = data["containers"]
            running_count = 0
            stopped_count = 0
            paused_count = 0
            total_memory_mb = 0.0
            total_cpu_percent = 0.0

            for c_detail in containers_list:
                if c_detail["status"] == "running":
                    running_count += 1
                    stats = container_stats.get(c_detail["full_id"])
                    if stats:
                        total_memory_mb += self._calculate_memory_mb(stats)
                        total_cpu_percent += self._calculate_cpu_percent(stats)
                    else:
                         logger.debug(f"No stats found for running container {c_detail['id']} ({c_detail['name']})")

                elif c_detail["status"] == "exited":
                    stopped_count += 1
                elif c_detail["status"] == "paused":
                    paused_count += 1

                del c_detail["full_id"] # Remove temporary full ID

            aggregated_status.append({
                "repo_full_name": repo_name,
                "running": running_count,
                "stopped": stopped_count,
                "paused": paused_count,
                "memory_usage_mb": round(total_memory_mb, 2),
                "cpu_usage_percent": round(total_cpu_percent, 2),
                "containers": containers_list
            })

        logger.info(f"Aggregated status for {len(aggregated_status)} managed repositories.")
        return aggregated_status

    # --- Container Management Functions ---

    def stop_container(self, container_id: str) -> bool:
        # ... (unchanged) ...
        if not self.client:
            logger.error(f"Cannot stop container {container_id}: Docker client not available.")
            return False
        try:
            container = self.client.containers.get(container_id)
            logger.info(f"Stopping container {container.name} ({container.short_id})...")
            container.stop()
            logger.info(f"Container {container.short_id} stopped.")
            return True
        except DockerNotFound:
            logger.warning(f"Container {container_id} not found for stopping.")
            return False
        except APIError as e:
            logger.error(f"API error stopping container {container_id}: {e}", exc_info=True)
            return False
        except Exception as e:
             logger.error(f"Unexpected error stopping container {container_id}: {e}", exc_info=True)
             return False

    def start_container(self, container_id: str) -> bool:
        # ... (unchanged) ...
        if not self.client:
            logger.error(f"Cannot start container {container_id}: Docker client not available.")
            return False
        try:
            container = self.client.containers.get(container_id)
            if container.status == 'running':
                logger.info(f"Container {container.short_id} is already running.")
                return True
            logger.info(f"Starting container {container.name} ({container.short_id})...")
            container.start()
            logger.info(f"Container {container.short_id} started.")
            return True
        except DockerNotFound:
            logger.warning(f"Container {container_id} not found for starting.")
            return False
        except APIError as e:
            logger.error(f"API error starting container {container_id}: {e}", exc_info=True)
            return False
        except Exception as e:
             logger.error(f"Unexpected error starting container {container_id}: {e}", exc_info=True)
             return False

    def remove_container(self, container_id: str, force: bool = True) -> bool:
        # ... (unchanged) ...
        if not self.client:
            logger.error(f"Cannot remove container {container_id}: Docker client not available.")
            return False
        try:
            container = self.client.containers.get(container_id)
            logger.info(f"Removing container {container.name} ({container.short_id})...")
            container.remove(force=force)
            logger.info(f"Container {container.short_id} removed.")
            return True
        except DockerNotFound:
            logger.warning(f"Container {container_id} not found for removal.")
            return False
        except APIError as e:
            logger.error(f"API error removing container {container_id}: {e}", exc_info=True)
            return False
        except Exception as e:
             logger.error(f"Unexpected error removing container {container_id}: {e}", exc_info=True)
             return False

    # --- Helper methods for formatting run args ---
    def _format_volumes(self, volume_list: List[VolumeMapping]) -> Dict[str, Dict[str, str]]:
        """ Parses VolumeMapping list into {'host_path': {'bind': 'container_path', 'mode': 'rw'}} """
        volumes_dict = {}
        for vol in volume_list:
            if vol.host_path and vol.container_path:
                # Assuming 'rw' mode by default, add 'ro' if needed based on VolumeMapping model
                volumes_dict[vol.host_path] = {'bind': vol.container_path, 'mode': 'rw'}
        return volumes_dict

    def _format_ports(self, port_mapping_list: List[PortMapping]) -> Optional[Dict[str, Optional[int]]]:
        """ Parses PortMapping list into {'<container_port>/<protocol>': host_port} """
        if not port_mapping_list:
            return None

        ports_dict = {}
        for port_map in port_mapping_list:
            if port_map.container_port:
                container_key = f"{port_map.container_port}/{port_map.protocol}"
                # Use None if host_port is not provided or explicitly null/empty
                ports_dict[container_key] = port_map.host_port if port_map.host_port is not None else None
        return ports_dict if ports_dict else None


    def _format_labels(self, label_list: List[LabelPair]) -> Dict[str, str]:
        """Converts a list of LabelPair objects into a dictionary."""
        return {label.key: label.value for label in label_list if label.key}


    def scale_repository(
        self,
        repo_full_name: str,
        image_tag: str,
        runtime_config: ContainerRuntimeConfig,
        desired_instances: int
    ) -> Dict[str, int]:
        """
        Scales the number of running containers for a repository to the desired count.
        Uses the provided image tag and runtime configuration.
        Returns a dict with {'started': count, 'removed': count}.
        """
        if not self.client:
            logger.error(f"Cannot scale {repo_full_name}: Docker client not available.")
            raise DockerException("Docker client not available.")

        if desired_instances < 0:
            raise ValueError("Desired instances cannot be negative.")

        logger.info(f"Scaling repository {repo_full_name} to {desired_instances} instances using image {image_tag}.")

        try:
            current_containers = self.client.containers.list(
                all=True,
                filters={"label": f"{CYPHER_REPO_LABEL}={repo_full_name}"}
            )
            running_containers = [c for c in current_containers if c.status == 'running']
            current_running_count = len(running_containers)
            logger.info(f"Found {current_running_count} running containers for {repo_full_name}.")

        except APIError as e:
            logger.error(f"API error listing containers for scaling {repo_full_name}: {e}", exc_info=True)
            raise DockerException(f"Failed to list containers for {repo_full_name}: {e}")
        except Exception as e:
            logger.error(f"Unexpected error listing containers for scaling {repo_full_name}: {e}", exc_info=True)
            raise DockerException(f"Unexpected error listing containers for {repo_full_name}: {e}")


        delta = desired_instances - current_running_count
        started_count = 0
        removed_count = 0

        # --- Scale Up ---
        if delta > 0:
            logger.info(f"Scaling up {repo_full_name} by {delta} instances.")
            # Prepare common container run arguments from runtime_config
            formatted_volumes = self._format_volumes(runtime_config.volumes)
            formatted_ports = self._format_ports(runtime_config.port_mappings) # Use new port_mappings field
            formatted_labels = self._format_labels(runtime_config.labels)
            formatted_labels[CYPHER_REPO_LABEL] = repo_full_name # Ensure our management label is present

            run_kwargs = {
                "image": image_tag,
                "detach": True,
                "labels": formatted_labels,
                "environment": [f"{env.name}={env.value}" for env in runtime_config.environment_variables],
                "volumes": formatted_volumes if formatted_volumes else None,
                "ports": formatted_ports if formatted_ports else None,
                "network_mode": runtime_config.network_mode if runtime_config.network_mode else None, # Use network_mode
                "restart_policy": {"Name": "unless-stopped"}
            }
            run_kwargs = {k: v for k, v in run_kwargs.items() if v is not None} # Clean None values

            logger.debug(f"Docker run arguments for scaling up {repo_full_name}: {run_kwargs}")

            for i in range(delta):
                container_name = f"{repo_full_name.replace('/', '-')}-{uuid.uuid4().hex[:8]}"
                logger.info(f"Attempting to start container {i+1}/{delta}: {container_name}")
                try:
                    new_container = self.client.containers.run(name=container_name, **run_kwargs)
                    logger.info(f"Successfully started container {new_container.short_id} ({container_name})")
                    started_count += 1
                except APIError as e:
                    logger.error(f"API error starting container {container_name}: {e}", exc_info=True)
                except Exception as e:
                    logger.error(f"Unexpected error starting container {container_name}: {e}", exc_info=True)

        # --- Scale Down ---
        elif delta < 0:
            count_to_remove = abs(delta)
            logger.info(f"Scaling down {repo_full_name} by {count_to_remove} instances.")
            containers_to_remove = running_containers[:count_to_remove]
            for container in containers_to_remove:
                logger.info(f"Attempting to remove container {container.short_id} ({container.name})")
                if self.remove_container(container.id, force=True):
                    removed_count += 1
                else:
                    logger.warning(f"Failed to remove container {container.short_id} during scale down.")

        else:
            logger.info(f"Repository {repo_full_name} is already at the desired scale ({desired_instances}). No action needed.")

        return {"started": started_count, "removed": removed_count}

    # --- New method to list Docker networks ---
    def list_networks(self) -> List[Dict[str, Any]]:
        """Lists available Docker networks."""
        if not self.client:
            logger.warning("Cannot list networks: Docker client not available.")
            return []
        try:
            networks = self.client.networks.list()
            network_list = [{"id": net.short_id, "name": net.name, "driver": net.attrs.get("Driver")} for net in networks]
            logger.info(f"Found {len(network_list)} Docker networks.")
            return network_list
        except APIError as e:
            logger.error(f"API error listing Docker networks: {e}", exc_info=True)
            raise DockerException(f"Failed to list Docker networks: {e}")
        except Exception as e:
            logger.error(f"Unexpected error listing Docker networks: {e}", exc_info=True)
            raise DockerException(f"Unexpected error listing Docker networks: {e}")


# Single instance
docker_service = DockerService()
