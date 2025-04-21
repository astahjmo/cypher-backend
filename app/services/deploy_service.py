import logging
import asyncio
import httpx
from typing import List, Dict, Optional, Any
from docker.errors import NotFound as DockerNotFound, APIError, DockerException
from docker.models.containers import Container

from .docker_service import docker_service
from models.container.db_models import ContainerRuntimeConfig

logger = logging.getLogger(__name__)


class DeployError(Exception):
    """Custom exception for deployment-related errors."""

    pass


class DeployService:
    """Provides services for deploying new container versions with health checks."""

    async def _perform_health_check(
        self,
        container: Container,
        endpoint: str = "/health",
        retries: int = 5,
        delay: int = 3,
    ) -> bool:
        """Performs an HTTP health check on a container's internal IP address.

        Attempts to reach the specified endpoint on the container's primary internal IP.
        Retries several times with delays if the check fails initially.

        Args:
            container (Container): The docker-py Container object to check.
            endpoint (str): The HTTP path to check (e.g., '/health', '/status'). Defaults to '/health'.
            retries (int): The maximum number of check attempts. Defaults to 5.
            delay (int): The delay in seconds between retry attempts. Defaults to 3.

        Returns:
            bool: True if the health check passes within the retries, False otherwise.
        """
        if not endpoint:
            logger.info(
                f"No health check endpoint specified for {container.short_id}, skipping check."
            )
            await asyncio.sleep(delay)
            return True

        try:
            await asyncio.to_thread(container.reload)
        except (APIError, DockerException) as e:
            logger.error(
                f"Health Check: Failed to reload container attributes for {container.short_id}: {e}"
            )
            return False

        ip_address = None
        container_port = None

        if container.attrs.get("Config", {}).get("ExposedPorts"):
            port_str = list(container.attrs["Config"]["ExposedPorts"].keys())[0]
            container_port = int(port_str.split("/")[0])
            logger.debug(
                f"Health Check: Determined container port {container_port} for {container.short_id}"
            )
        else:
            logger.warning(
                f"Health Check: Could not determine container port for {container.short_id}. Cannot perform check."
            )
            return False

        networks = container.attrs.get("NetworkSettings", {}).get("Networks", {})
        if networks:
            network_name = next(iter(networks))
            ip_address = networks[network_name].get("IPAddress")
            logger.debug(
                f"Health Check: Found IP {ip_address} on network '{network_name}' for {container.short_id}"
            )

        if not ip_address:
            logger.error(
                f"Health Check: Could not obtain internal IP address for container {container.short_id}."
            )
            return False

        health_url = f"http://{ip_address}:{container_port}{endpoint if endpoint.startswith('/') else '/' + endpoint}"
        logger.info(
            f"Health Check: Starting checks for {container.short_id} at {health_url}"
        )

        async with httpx.AsyncClient(timeout=delay - 1) as client:
            for attempt in range(retries):
                try:
                    logger.debug(
                        f"Health Check: Attempt {attempt + 1}/{retries} for {health_url}"
                    )
                    response = await client.get(health_url)
                    if response.is_success:
                        logger.info(
                            f"Health Check: Container {container.short_id} passed health check (Status: {response.status_code})."
                        )
                        return True
                    else:
                        logger.warning(
                            f"Health Check: Attempt {attempt + 1} failed for {container.short_id}. Status: {response.status_code}"
                        )
                except httpx.RequestError as e:
                    logger.warning(
                        f"Health Check: Attempt {attempt + 1} failed for {container.short_id}. Error: {e}"
                    )

                if attempt < retries - 1:
                    await asyncio.sleep(delay)

        logger.error(
            f"Health Check: Container {container.short_id} failed health check after {retries} attempts."
        )
        return False

    async def deploy_new_version(
        self,
        new_image_tag: str,
        repo_full_name: str,
        runtime_config: ContainerRuntimeConfig,
        build_id: str,
        health_check_endpoint: str = "/health",
    ):
        """Deploys a new version of a service using a rolling update strategy.

        Starts a new container with the specified image tag and a unique deployment ID label.
        Performs a health check on the new container. If healthy, it finds and removes
        any older containers for the same repository that do not have the current
        deployment ID label.

        Args:
            new_image_tag (str): The Docker image tag for the new version.
            repo_full_name (str): The full name of the repository (e.g., 'owner/repo').
            runtime_config (ContainerRuntimeConfig): The runtime configuration for the container.
            build_id (str): The unique ID of the build triggering this deployment, used for labeling.
            health_check_endpoint (str): The HTTP path for health checks. Defaults to '/health'.

        Raises:
            DeployError: If any critical step of the deployment fails (e.g., starting container, health check).
            DockerException: If underlying Docker operations fail unexpectedly.
        """
        if not docker_service.client:
            raise DeployError("Docker client not available.")

        logger.info(
            f"Deploy Service: Starting deployment for {repo_full_name}, image {new_image_tag}, build ID {build_id}"
        )

        try:
            logger.debug("Deploy Service: Preparing container configuration...")
            container_config = await asyncio.to_thread(
                docker_service._prepare_container_config,
                repo_full_name,
                new_image_tag,
                runtime_config,
            )
            container_config["labels"]["cypher.deployment.id"] = build_id
            logger.debug(
                f"Deploy Service: Added deployment label: cypher.deployment.id={build_id}"
            )

        except Exception as e:
            logger.error(
                f"Deploy Service: Failed to prepare container config: {e}",
                exc_info=True,
            )
            raise DeployError(f"Failed to prepare container configuration: {e}")

        new_container = None
        try:
            logger.info(
                f"Deploy Service: Starting new container with image {new_image_tag}..."
            )
            new_container = await asyncio.to_thread(
                docker_service.client.containers.run, **container_config
            )
            logger.info(
                f"Deploy Service: Started new container {new_container.short_id} ({new_container.name})"
            )
        except APIError as e:
            logger.error(
                f"Deploy Service: Docker API error starting new container: {e}",
                exc_info=True,
            )
            raise DeployError(f"Docker API error starting container: {e.explanation}")
        except Exception as e:
            logger.error(
                f"Deploy Service: Unexpected error starting new container: {e}",
                exc_info=True,
            )
            # Clean up potentially created container if start failed mid-way
            if new_container:
                try:
                    await asyncio.to_thread(new_container.remove, force=True)
                except Exception:
                    pass
            raise DeployError(f"Unexpected error starting container: {e}")

        try:
            is_healthy = await self._perform_health_check(
                new_container, health_check_endpoint
            )
            if not is_healthy:
                logger.error(
                    f"Deploy Service: New container {new_container.short_id} failed health check. Attempting cleanup."
                )
                try:
                    await asyncio.to_thread(new_container.remove, force=True)
                    logger.info(
                        f"Deploy Service: Cleaned up unhealthy new container {new_container.short_id}."
                    )
                except Exception as cleanup_e:
                    logger.error(
                        f"Deploy Service: Failed to cleanup unhealthy container {new_container.short_id}: {cleanup_e}"
                    )
                raise DeployError(
                    f"New container {new_container.short_id} failed health check."
                )
            logger.info(
                f"Deploy Service: New container {new_container.short_id} is healthy."
            )
        except Exception as e:
            logger.error(
                f"Deploy Service: Error during health check phase for {new_container.short_id}: {e}",
                exc_info=True,
            )
            try:
                await asyncio.to_thread(new_container.remove, force=True)
            except Exception:
                pass
            raise DeployError(f"Health check failed or errored for new container: {e}")

        removed_count = 0
        try:
            logger.info(
                f"Deploy Service: Finding old containers for {repo_full_name} (without label cypher.deployment.id={build_id})..."
            )
            label_filter = {
                "label": [
                    f"managed-by=cypher",
                    f"cypher.repo_full_name={repo_full_name}",
                ]
            }
            all_managed_containers = await asyncio.to_thread(
                docker_service.client.containers.list, all=True, filters=label_filter
            )

            old_containers = [
                c
                for c in all_managed_containers
                if c.labels.get("cypher.deployment.id") != build_id
                and c.id != new_container.id
            ]

            if not old_containers:
                logger.info(
                    f"Deploy Service: No old containers found for {repo_full_name} to remove."
                )
            else:
                logger.info(
                    f"Deploy Service: Found {len(old_containers)} old container(s) to remove."
                )
                for old_container in old_containers:
                    logger.info(
                        f"Deploy Service: Removing old container {old_container.short_id} ({old_container.name})..."
                    )
                    try:
                        await asyncio.to_thread(old_container.remove, force=True)
                        removed_count += 1
                        logger.info(
                            f"Deploy Service: Removed old container {old_container.short_id}."
                        )
                    except DockerNotFound:
                        logger.warning(
                            f"Deploy Service: Old container {old_container.short_id} already gone."
                        )
                    except APIError as e:
                        logger.error(
                            f"Deploy Service: API error removing old container {old_container.short_id}: {e}",
                            exc_info=True,
                        )
                    except Exception as e:
                        logger.error(
                            f"Deploy Service: Unexpected error removing old container {old_container.short_id}: {e}",
                            exc_info=True,
                        )

        except APIError as e:
            logger.error(
                f"Deploy Service: API error listing containers to find old ones: {e}",
                exc_info=True,
            )
        except Exception as e:
            logger.error(
                f"Deploy Service: Unexpected error finding/removing old containers: {e}",
                exc_info=True,
            )

        logger.info(
            f"Deploy Service: Deployment completed for {repo_full_name}. New container: {new_container.short_id}. Old containers removed: {removed_count}."
        )


# Singleton instance of the DeployService
deploy_service = DeployService()
