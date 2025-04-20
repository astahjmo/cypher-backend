import docker
from docker.errors import DockerException, APIError
import logging
from ..config import settings

logger = logging.getLogger(__name__)

class RegistryService:
    def __init__(self):
        try:
            self.client = docker.from_env()
            self.client.ping()
            logger.info("Docker client initialized for RegistryService.")
        except DockerException as e:
            logger.error(f"Failed to initialize Docker client for RegistryService: {e}", exc_info=True)
            self.client = None
        except Exception as e:
             logger.error(f"An unexpected error occurred during Docker client initialization for RegistryService: {e}", exc_info=True)
             self.client = None

    def login_to_registry(self) -> bool:
        """Logs in to the configured container registry."""
        if not self.client:
            logger.error("Docker client not available for registry login.")
            return False
        if not settings.REGISTRY_URL or not settings.REGISTRY_USER or not settings.REGISTRY_PASSWORD:
            logger.error("Registry credentials (URL, USER, PASSWORD) not configured.")
            return False

        try:
            logger.info(f"Logging in to registry: {settings.REGISTRY_URL} as user {settings.REGISTRY_USER}")
            login_result = self.client.login(
                username=settings.REGISTRY_USER,
                password=settings.REGISTRY_PASSWORD,
                registry=settings.REGISTRY_URL
            )
            logger.info(f"Registry login status: {login_result.get('Status')}")
            return login_result.get('Status') == 'Login Succeeded'
        except APIError as e:
            logger.error(f"Failed to login to registry {settings.REGISTRY_URL}: {e}", exc_info=True)
            return False
        except Exception as e:
            logger.error(f"Unexpected error during registry login: {e}", exc_info=True)
            return False

    def push_image(self, image_tag: str) -> tuple[bool, str]:
        """
        Pushes a tagged Docker image to the configured registry.

        Args:
            image_tag: The full tag of the image to push (e.g., ghcr.io/user/repo:tag).

        Returns:
            Tuple (success_boolean, log_output_string).
        """
        if not self.client:
            return False, "Docker client not available."

        # Attempt login first (optional, depends on Docker daemon config)
        # if not self.login_to_registry():
        #     return False, "Registry login failed."

        logger.info(f"Attempting to push image: {image_tag}")
        push_logs = []
        try:
            # Stream push logs
            push_log_stream = self.client.images.push(image_tag, stream=True, decode=True)

            for chunk in push_log_stream:
                 if isinstance(chunk, dict):
                    status = chunk.get('status')
                    progress = chunk.get('progress')
                    error = chunk.get('error')

                    if error:
                        logger.error(f"Error pushing image {image_tag}: {error}")
                        push_logs.append(f"ERROR: {error}")
                        # Decide if we should return immediately on error
                        # return False, "\n".join(push_logs)
                    elif status:
                        log_line = f"{status}"
                        if progress:
                            log_line += f" - {progress}"
                        logger.debug(f"Push log: {log_line}")
                        push_logs.append(log_line)
                    else:
                        # Log other messages if needed
                        logger.debug(f"Push stream chunk: {chunk}")

                 else:
                    logger.warning(f"Unexpected chunk type in push stream: {type(chunk)} - {chunk}")


            # Verify push success (the stream might not always throw error)
            # A more reliable check might involve querying the registry API if available
            # For now, assume success if no error was explicitly logged in the stream
            if any("ERROR:" in log for log in push_logs):
                 logger.error(f"Push failed for {image_tag}. See logs.")
                 return False, "\n".join(push_logs)
            else:
                 logger.info(f"Successfully pushed image: {image_tag}")
                 return True, "\n".join(push_logs)

        except APIError as e:
            logger.error(f"API error pushing image {image_tag}: {e}", exc_info=True)
            push_logs.append(f"APIError: {e}")
            return False, "\n".join(push_logs)
        except Exception as e:
            logger.error(f"Unexpected error pushing image {image_tag}: {e}", exc_info=True)
            push_logs.append(f"Unexpected Error: {e}")
            return False, "\n".join(push_logs)


# Single instance
registry_service = RegistryService()
