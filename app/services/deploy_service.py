import logging
import asyncio
import httpx # Using httpx for async health checks
from typing import List, Dict, Optional, Any
from docker.errors import NotFound as DockerNotFound, APIError, DockerException
from docker.models.containers import Container

# Import necessary components
from .docker_service import docker_service # Import the existing docker service instance
from models.container.db_models import ContainerRuntimeConfig

logger = logging.getLogger(__name__)

# Define a custom exception for deploy errors
class DeployError(Exception):
    pass

class DeployService:

    async def _perform_health_check(self, container: Container, endpoint: str = "/health", retries: int = 5, delay: int = 3) -> bool:
        """Performs HTTP health check on a container's internal IP."""
        if not endpoint:
            logger.info(f"No health check endpoint specified for {container.short_id}, skipping check.")
            await asyncio.sleep(delay) # Add a small delay anyway
            return True

        # Reload container attributes to get network information
        try:
            await asyncio.to_thread(container.reload)
        except (APIError, DockerException) as e:
             logger.error(f"Health Check: Failed to reload container attributes for {container.short_id}: {e}")
             return False

        # Find the internal IP address (assuming it's connected to a bridge/overlay network)
        ip_address = None
        container_port = None

        # Extract the target port from the runtime config (assuming first exposed port is the app port)
        # This might need refinement if multiple ports are exposed.
        if container.attrs.get('Config', {}).get('ExposedPorts'):
             # Get the first key like '8000/tcp' and extract the port number
             port_str = list(container.attrs['Config']['ExposedPorts'].keys())[0]
             container_port = int(port_str.split('/')[0])
             logger.debug(f"Health Check: Determined container port {container_port} for {container.short_id}")
        else:
             logger.warning(f"Health Check: Could not determine container port for {container.short_id}. Cannot perform check.")
             return False # Cannot check without port

        # Find IP on a common network (adjust if using specific named networks)
        networks = container.attrs.get('NetworkSettings', {}).get('Networks', {})
        if networks:
            # Prioritize common networks or get the first one found
            network_name = next(iter(networks)) # Get the name of the first network
            ip_address = networks[network_name].get('IPAddress')
            logger.debug(f"Health Check: Found IP {ip_address} on network '{network_name}' for {container.short_id}")

        if not ip_address:
            logger.error(f"Health Check: Could not obtain internal IP address for container {container.short_id}.")
            return False

        health_url = f"http://{ip_address}:{container_port}{endpoint if endpoint.startswith('/') else '/' + endpoint}"
        logger.info(f"Health Check: Starting checks for {container.short_id} at {health_url}")

        async with httpx.AsyncClient(timeout=delay - 1) as client: # Timeout slightly less than delay
            for attempt in range(retries):
                try:
                    logger.debug(f"Health Check: Attempt {attempt + 1}/{retries} for {health_url}")
                    response = await client.get(health_url)
                    if response.is_success:
                        logger.info(f"Health Check: Container {container.short_id} passed health check (Status: {response.status_code}).")
                        return True
                    else:
                        logger.warning(f"Health Check: Attempt {attempt + 1} failed for {container.short_id}. Status: {response.status_code}")
                except httpx.RequestError as e:
                    logger.warning(f"Health Check: Attempt {attempt + 1} failed for {container.short_id}. Error: {e}")

                if attempt < retries - 1:
                    await asyncio.sleep(delay)

        logger.error(f"Health Check: Container {container.short_id} failed health check after {retries} attempts.")
        return False

    async def deploy_new_version(
        self,
        new_image_tag: str,
        repo_full_name: str,
        runtime_config: ContainerRuntimeConfig,
        build_id: str, # Used for the deployment label
        health_check_endpoint: str = "/health" # Default health check endpoint
    ):
        """
        Deploys a new version using Traefik labels and rolling update strategy.
        1. Starts new container with new image and deployment label.
        2. Performs health check.
        3. Stops and removes old containers without the current deployment label.
        """
        if not docker_service.client:
            raise DeployError("Docker client not available.")

        logger.info(f"Deploy Service: Starting deployment for {repo_full_name}, image {new_image_tag}, build ID {build_id}")

        # 1. Prepare Config for New Container (including deployment label)
        try:
            logger.debug("Deploy Service: Preparing container configuration...")
            container_config = await asyncio.to_thread(
                docker_service._prepare_container_config,
                repo_full_name, new_image_tag, runtime_config
            )
            # Add the unique deployment label
            container_config["labels"]["cypher.deployment.id"] = build_id
            logger.debug(f"Deploy Service: Added deployment label: cypher.deployment.id={build_id}")

        except Exception as e:
            logger.error(f"Deploy Service: Failed to prepare container config: {e}", exc_info=True)
            raise DeployError(f"Failed to prepare container configuration: {e}")

        # 2. Start New Container
        new_container = None
        try:
            logger.info(f"Deploy Service: Starting new container with image {new_image_tag}...")
            new_container = await asyncio.to_thread(docker_service.client.containers.run, **container_config)
            logger.info(f"Deploy Service: Started new container {new_container.short_id} ({new_container.name})")
        except APIError as e:
            logger.error(f"Deploy Service: Docker API error starting new container: {e}", exc_info=True)
            raise DeployError(f"Docker API error starting container: {e.explanation}")
        except Exception as e:
            logger.error(f"Deploy Service: Unexpected error starting new container: {e}", exc_info=True)
            raise DeployError(f"Unexpected error starting container: {e}")

        # 3. Perform Health Check on New Container
        try:
            is_healthy = await self._perform_health_check(new_container, health_check_endpoint)
            if not is_healthy:
                # Attempt cleanup of the newly started container if health check fails
                logger.error(f"Deploy Service: New container {new_container.short_id} failed health check. Attempting cleanup.")
                try:
                    await asyncio.to_thread(new_container.remove, force=True)
                    logger.info(f"Deploy Service: Cleaned up unhealthy new container {new_container.short_id}.")
                except Exception as cleanup_e:
                    logger.error(f"Deploy Service: Failed to cleanup unhealthy container {new_container.short_id}: {cleanup_e}")
                raise DeployError(f"New container {new_container.short_id} failed health check.")
            logger.info(f"Deploy Service: New container {new_container.short_id} is healthy.")
        except Exception as e:
             # Catch potential errors during health check itself or cleanup
             logger.error(f"Deploy Service: Error during health check phase for {new_container.short_id}: {e}", exc_info=True)
             # Attempt cleanup before re-raising
             try: await asyncio.to_thread(new_container.remove, force=True)
             except Exception: pass
             raise DeployError(f"Health check failed or errored for new container: {e}")


        # 4. Find and Remove Old Containers
        removed_count = 0
        try:
            logger.info(f"Deploy Service: Finding old containers for {repo_full_name} (without label cypher.deployment.id={build_id})...")
            label_filter = {
                "label": [
                    f"managed-by=cypher",
                    f"cypher.repo_full_name={repo_full_name}"
                ]
            }
            all_managed_containers = await asyncio.to_thread(docker_service.client.containers.list, all=True, filters=label_filter)

            old_containers = [
                c for c in all_managed_containers
                if c.labels.get("cypher.deployment.id") != build_id and c.id != new_container.id # Ensure not removing the new one
            ]

            if not old_containers:
                logger.info(f"Deploy Service: No old containers found for {repo_full_name} to remove.")
            else:
                logger.info(f"Deploy Service: Found {len(old_containers)} old container(s) to remove.")
                for old_container in old_containers:
                    logger.info(f"Deploy Service: Removing old container {old_container.short_id} ({old_container.name})...")
                    try:
                        await asyncio.to_thread(old_container.remove, force=True)
                        removed_count += 1
                        logger.info(f"Deploy Service: Removed old container {old_container.short_id}.")
                    except DockerNotFound:
                         logger.warning(f"Deploy Service: Old container {old_container.short_id} already gone.")
                    except APIError as e:
                         logger.error(f"Deploy Service: API error removing old container {old_container.short_id}: {e}", exc_info=True)
                    except Exception as e:
                         logger.error(f"Deploy Service: Unexpected error removing old container {old_container.short_id}: {e}", exc_info=True)

        except APIError as e:
            logger.error(f"Deploy Service: API error listing containers to find old ones: {e}", exc_info=True)
            # Don't raise here, the new container is running, but cleanup failed. Logged error.
        except Exception as e:
            logger.error(f"Deploy Service: Unexpected error finding/removing old containers: {e}", exc_info=True)
            # Don't raise here.

        logger.info(f"Deploy Service: Deployment completed for {repo_full_name}. New container: {new_container.short_id}. Old containers removed: {removed_count}.")


# Instantiate the service (can be improved with dependency injection later if needed)
deploy_service = DeployService()
