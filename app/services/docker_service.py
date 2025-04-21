import docker
from docker.errors import NotFound, APIError, DockerException # Added DockerException
from docker.models.containers import Container
from typing import List, Dict, Optional, Any, Generator, AsyncGenerator # Added Generator, AsyncGenerator
import logging
import time
import asyncio # Added asyncio for potential async operations

# Import ContainerRuntimeConfig model from its new location
from models.container.db_models import ContainerRuntimeConfig
# Import settings using absolute path
from config import settings

logger = logging.getLogger(__name__)

class DockerService:
    def __init__(self):
        try:
            # Use async client if available and needed, otherwise sync
            # For simplicity here, sticking with sync client but designing logs for async endpoint
            self.client = docker.from_env()
            logger.info("Docker client initialized successfully.")
        except DockerException as e:
            logger.error(f"Failed to initialize Docker client: {e}")
            self.client = None
        except Exception as e:
            logger.error(f"An unexpected error occurred during Docker client initialization: {e}")
            self.client = None

    def _get_managed_label_filter(self) -> Dict[str, str]:
        """Returns the label filter used to identify containers managed by this application."""
        return {"label": "managed-by=cypher"}

    def _calculate_cpu_percent(self, stats: Dict[str, Any]) -> float:
        """Calculates CPU usage percentage from Docker stats."""
        try:
            cpu_delta = stats['cpu_stats']['cpu_usage']['total_usage'] - stats['precpu_stats']['cpu_usage']['total_usage']
            system_cpu_delta = stats['cpu_stats']['system_cpu_usage'] - stats['precpu_stats']['system_cpu_usage']
            number_cpus = stats['cpu_stats'].get('online_cpus', len(stats['cpu_stats']['cpu_usage']['percpu_usage']))

            if system_cpu_delta > 0.0 and cpu_delta > 0.0:
                cpu_percent = (cpu_delta / system_cpu_delta) * number_cpus * 100.0
                return round(cpu_percent, 2)
            return 0.0
        except (KeyError, TypeError, ZeroDivisionError) as e:
            logger.warning(f"Could not calculate CPU percentage: {e}. Stats: {stats}")
            return 0.0

    def _calculate_memory_mb(self, stats: Dict[str, Any]) -> float:
        """Calculates Memory usage in MB from Docker stats."""
        try:
            mem_usage_bytes = stats.get('memory_stats', {}).get('usage')
            if mem_usage_bytes is not None:
                return round(mem_usage_bytes / (1024 * 1024), 2) # Bytes to MB
            return 0.0
        except (KeyError, TypeError) as e:
            logger.warning(f"Could not calculate Memory usage: {e}. Stats: {stats}")
            return 0.0


    def list_managed_containers(self) -> List[Dict[str, Any]]:
        """Lists all containers managed by Cypher, aggregating status and stats by repo."""
        if not self.client:
            logger.warning("Docker client not available. Cannot list containers.")
            return []

        statuses_by_repo: Dict[str, Dict[str, Any]] = {}
        all_containers: List[Container] = []
        try:
            all_containers = self.client.containers.list(all=True, filters=self._get_managed_label_filter())
        except APIError as e:
            logger.error(f"Docker API error listing containers: {e}", exc_info=True)
            return []
        except Exception as e:
            logger.error(f"Unexpected error listing containers: {e}", exc_info=True)
            return []


        for container in all_containers:
            try:
                repo_full_name = container.labels.get("cypher.repo_full_name", "unknown/repository")
                container_status = container.status

                if repo_full_name not in statuses_by_repo:
                    statuses_by_repo[repo_full_name] = {
                        "repo_full_name": repo_full_name,
                        "running": 0,
                        "stopped": 0,
                        "paused": 0,
                        "memory_usage_mb": 0.0,
                        "cpu_usage_percent": 0.0,
                        "containers": []
                    }

                cpu_percent = 0.0
                mem_mb = 0.0
                if container_status == 'running':
                    statuses_by_repo[repo_full_name]["running"] += 1
                    try:
                        stats = container.stats(stream=False)
                        cpu_percent = self._calculate_cpu_percent(stats)
                        mem_mb = self._calculate_memory_mb(stats)
                        statuses_by_repo[repo_full_name]["cpu_usage_percent"] += cpu_percent
                        statuses_by_repo[repo_full_name]["memory_usage_mb"] += mem_mb
                    except APIError as stats_error:
                         logger.warning(f"Could not get stats for container {container.short_id}: {stats_error}")
                    except Exception as stats_exception:
                         logger.error(f"Unexpected error getting stats for container {container.short_id}: {stats_exception}", exc_info=True)

                elif container_status == 'exited':
                    statuses_by_repo[repo_full_name]["stopped"] += 1
                elif container_status == 'paused':
                    statuses_by_repo[repo_full_name]["paused"] += 1

                formatted_ports = {}
                if container.ports:
                    for internal_port_proto, host_bindings in container.ports.items():
                        if host_bindings:
                            host_ip = host_bindings[0].get('HostIp', '0.0.0.0')
                            host_port = host_bindings[0].get('HostPort', '')
                            formatted_ports[internal_port_proto] = f"{host_ip}:{host_port}"
                        else:
                            formatted_ports[internal_port_proto] = "exposed"

                statuses_by_repo[repo_full_name]["containers"].append({
                    "id": container.short_id,
                    "name": container.name,
                    "status": container_status,
                    "image": ", ".join(container.image.tags) if container.image.tags else "unknown:image",
                    "ports": formatted_ports,
                    "cpu_usage_percent": cpu_percent,
                    "memory_usage_mb": mem_mb,
                })
            except Exception as e:
                 logger.error(f"Error processing container {container.short_id} ({container.name}): {e}", exc_info=True)
                 continue

        for repo_data in statuses_by_repo.values():
            repo_data["memory_usage_mb"] = round(repo_data["memory_usage_mb"], 2)
            repo_data["cpu_usage_percent"] = round(repo_data["cpu_usage_percent"], 2)

        return list(statuses_by_repo.values())


    def start_container(self, container_id: str) -> bool:
        """Starts a stopped container by its ID."""
        if not self.client: raise DockerException("Docker client not available.")
        try:
            container = self.client.containers.get(container_id)
            container.start()
            logger.info(f"Started container {container_id}")
            return True
        except NotFound:
            logger.warning(f"Container {container_id} not found for starting.")
            return False
        except APIError as e:
            logger.error(f"Docker API error starting container {container_id}: {e}", exc_info=True)
            raise
        except Exception as e:
            logger.error(f"Unexpected error starting container {container_id}: {e}", exc_info=True)
            raise


    def stop_container(self, container_id: str) -> bool:
        """Stops a running container by its ID."""
        if not self.client: raise DockerException("Docker client not available.")
        try:
            container = self.client.containers.get(container_id)
            container.stop()
            logger.info(f"Stopped container {container_id}")
            return True
        except NotFound:
            logger.warning(f"Container {container_id} not found for stopping.")
            return False
        except APIError as e:
            logger.error(f"Docker API error stopping container {container_id}: {e}", exc_info=True)
            raise
        except Exception as e:
            logger.error(f"Unexpected error stopping container {container_id}: {e}", exc_info=True)
            raise

    def remove_container(self, container_id: str, force: bool = True) -> bool:
        """Removes a container by its ID."""
        if not self.client: raise DockerException("Docker client not available.")
        try:
            container = self.client.containers.get(container_id)
            container.remove(force=force)
            logger.info(f"Removed container {container_id}")
            return True
        except NotFound:
            logger.warning(f"Container {container_id} not found for removal.")
            return False
        except APIError as e:
            logger.error(f"Docker API error removing container {container_id}: {e}", exc_info=True)
            raise
        except Exception as e:
            logger.error(f"Unexpected error removing container {container_id}: {e}", exc_info=True)
            raise

    # Changed return type and implementation for streaming
    def get_container_logs_stream(self, container_id: str, tail: int = 100) -> Generator[str, None, None]:
        """Retrieves logs for a specific container as a stream (generator)."""
        if not self.client:
            logger.error("Docker client not available for log streaming.")
            raise DockerException("Docker client not available.")
        try:
            container = self.client.containers.get(container_id)
            logger.info(f"Starting log stream for container {container_id} (tail={tail})")
            # stream=True and follow=True are key for real-time logs
            log_stream = container.logs(stream=True, follow=True, tail=tail, timestamps=True)
            for log_line_bytes in log_stream:
                try:
                    # Decode bytes to string, replacing errors
                    log_line_str = log_line_bytes.decode('utf-8', errors='replace').strip()
                    if log_line_str: # Avoid yielding empty lines if any
                        yield log_line_str
                except Exception as decode_error:
                    logger.warning(f"Error decoding log line for {container_id}: {decode_error}")
                    # Optionally yield a placeholder or skip
                    yield f"[DECODE ERROR: {decode_error}]"

            logger.info(f"Log stream finished for container {container_id}")

        except NotFound:
            logger.warning(f"Container {container_id} not found for fetching logs.")
            raise NotFound(f"Container {container_id} not found.") # Raise NotFound for the endpoint to catch
        except APIError as e:
            logger.error(f"Docker API error starting log stream for container {container_id}: {e}", exc_info=True)
            # Depending on the API error, you might want to yield an error message or just raise
            yield f"[API ERROR: {e}]"
            raise # Re-raise to signal the endpoint about the issue
        except Exception as e:
            logger.error(f"Unexpected error during log stream for container {container_id}: {e}", exc_info=True)
            yield f"[UNEXPECTED STREAM ERROR: {e}]"
            raise # Re-raise

    # Keep the original non-streaming method as well, if needed elsewhere, or remove if unused
    def get_container_logs(self, container_id: str, tail: int = 100) -> str:
        """Retrieves logs for a specific container (non-streaming)."""
        if not self.client: raise DockerException("Docker client not available.")
        try:
            container = self.client.containers.get(container_id)
            logs = container.logs(tail=tail, stream=False, follow=False, timestamps=True)
            return logs.decode('utf-8', errors='replace')
        except NotFound:
            logger.warning(f"Container {container_id} not found for fetching logs.")
            raise ValueError(f"Container {container_id} not found.")
        except APIError as e:
            logger.error(f"Docker API error fetching logs for container {container_id}: {e}", exc_info=True)
            raise RuntimeError(f"Docker API error fetching logs: {e}")
        except Exception as e:
            logger.error(f"Unexpected error fetching logs for container {container_id}: {e}", exc_info=True)
            raise RuntimeError(f"Unexpected error fetching logs: {e}")


    def scale_repository(self, repo_full_name: str, image_tag: str, runtime_config: ContainerRuntimeConfig, desired_instances: int) -> Dict[str, int]:
        """Scales containers for a repository to the desired number."""
        if not self.client: raise DockerException("Docker client not available.")

        logger.info(f"Scaling repository {repo_full_name} to {desired_instances} instances using image {image_tag}")

        label_filter = self._get_managed_label_filter()
        label_filter["label"] = f"{label_filter['label']},cypher.repo_full_name={repo_full_name}"
        try:
            current_containers = self.client.containers.list(all=True, filters=label_filter)
        except APIError as e:
             logger.error(f"Docker API error listing containers for scaling {repo_full_name}: {e}", exc_info=True)
             raise DockerException(f"Could not list containers for scaling: {e}")

        current_count = len(current_containers)
        logger.info(f"Found {current_count} existing containers for {repo_full_name}.")

        started_count = 0
        removed_count = 0

        if desired_instances > current_count:
            needed = desired_instances - current_count
            logger.info(f"Scaling up: Starting {needed} new instances for {repo_full_name}.")
            for i in range(needed):
                try:
                    container_config = self._prepare_container_config(repo_full_name, image_tag, runtime_config)
                    container = self.client.containers.run(**container_config)
                    logger.info(f"Started new container {container.short_id} for {repo_full_name}")
                    started_count += 1
                except APIError as e:
                    logger.error(f"Docker API error starting new instance {i+1}/{needed} for {repo_full_name}: {e}", exc_info=True)
                    raise DockerException(f"Error starting new container: {e}")
                except Exception as e:
                    logger.error(f"Unexpected error starting new instance {i+1}/{needed} for {repo_full_name}: {e}", exc_info=True)
                    raise
        elif desired_instances < current_count:
            to_remove_count = current_count - desired_instances
            logger.info(f"Scaling down: Removing {to_remove_count} instances for {repo_full_name}.")
            containers_to_remove = current_containers[:to_remove_count]
            for container in containers_to_remove:
                try:
                    container.remove(force=True)
                    logger.info(f"Removed container {container.short_id} for {repo_full_name}")
                    removed_count += 1
                except APIError as e:
                    logger.error(f"Docker API error removing container {container.short_id} for {repo_full_name}: {e}", exc_info=True)
                except Exception as e:
                    logger.error(f"Unexpected error removing container {container.short_id} for {repo_full_name}: {e}", exc_info=True)

        logger.info(f"Scaling complete for {repo_full_name}. Started: {started_count}, Removed: {removed_count}.")
        return {"started": started_count, "removed": removed_count}

    def _prepare_container_config(self, repo_full_name: str, image_tag: str, config: ContainerRuntimeConfig) -> Dict[str, Any]:
        """Helper to prepare the dictionary for docker.containers.run()"""
        run_config: Dict[str, Any] = {
            "image": image_tag,
            "detach": True,
            "labels": {
                "managed-by": "cypher",
                "cypher.repo_full_name": repo_full_name,
                **{label.key: label.value for label in config.labels if label.key}
            },
            "environment": {env.name: env.value for env in config.environment_variables if env.name},
            "volumes": {vol.host_path: {'bind': vol.container_path, 'mode': 'rw'}
                        for vol in config.volumes if vol.host_path and vol.container_path},
            "network_mode": config.network_mode if config.network_mode else 'bridge',
            "ports": {},
            "restart_policy": {"Name": "unless-stopped"},
        }

        if config.port_mappings:
            ports_dict = {}
            for pm in config.port_mappings:
                container_port_str = f"{pm.container_port}/{pm.protocol}"
                if pm.host_port is not None:
                     ports_dict[container_port_str] = int(pm.host_port)
                else:
                     ports_dict[container_port_str] = None
            run_config["ports"] = ports_dict

        if run_config["network_mode"] == "host":
            run_config.pop("ports", None)

        logger.debug(f"Prepared run config for {repo_full_name}: {run_config}")
        return run_config

    def list_networks(self) -> List[Dict[str, Any]]:
        """Lists available Docker networks."""
        if not self.client:
            logger.warning("Docker client not available. Cannot list networks.")
            return []
        try:
            networks = self.client.networks.list()
            return [{"id": net.short_id, "name": net.name, "driver": net.attrs.get('Driver')} for net in networks]
        except APIError as e:
            logger.error(f"Docker API error listing networks: {e}", exc_info=True)
            raise DockerException(f"Could not list networks: {e}")
        except Exception as e:
            logger.error(f"Unexpected error listing networks: {e}", exc_info=True)
            raise


# Instantiate the service
docker_service = DockerService()
