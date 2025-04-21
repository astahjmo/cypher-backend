import docker
from docker.errors import APIError
import logging

# Import settings from config
from config import settings

logger = logging.getLogger(__name__)

def are_registry_credentials_set() -> bool:
    """Checks if Docker registry credentials are configured in settings."""
    # Corrected to use DOCKER_ prefixed variables from settings
    configured = bool(
        settings.DOCKER_REGISTRY_URL and
        settings.DOCKER_REGISTRY_USERNAME and
        settings.DOCKER_REGISTRY_PASSWORD
    )
    if not configured:
        logger.info("Registry check: Credentials or URL not fully configured.")
    return configured

class RegistryService:
    """Handles interactions with a Docker registry."""

    def __init__(self):
        try:
            self.client = docker.from_env()
            logger.info("RegistryService: Docker client initialized.")
            self._login() # Attempt login on initialization if configured
        except Exception as e:
            logger.error(f"RegistryService: Failed to initialize Docker client: {e}", exc_info=True)
            self.client = None

    def _login(self):
        """Logs in to the Docker registry if credentials are set."""
        if not self.client:
            logger.error("RegistryService: Docker client unavailable, cannot login.")
            return False

        if are_registry_credentials_set():
            logger.info(f"RegistryService: Attempting login to {settings.DOCKER_REGISTRY_URL} as {settings.DOCKER_REGISTRY_USERNAME}")
            try:
                login_result = self.client.login(
                    username=settings.DOCKER_REGISTRY_USERNAME,
                    password=settings.DOCKER_REGISTRY_PASSWORD,
                    registry=settings.DOCKER_REGISTRY_URL
                )
                logger.info(f"RegistryService: Login successful: {login_result}")
                return True
            except APIError as e:
                logger.error(f"RegistryService: Docker registry login failed: {e}", exc_info=True)
                return False
            except Exception as e:
                 logger.error(f"RegistryService: Unexpected error during registry login: {e}", exc_info=True)
                 return False
        else:
            logger.info("RegistryService: Credentials not set, skipping login.")
            return False # Indicate login was not attempted/successful

    def push_image(self, image_tag: str) -> tuple[bool, str]:
        """Pushes a tagged Docker image to the configured registry."""
        if not self.client:
            logger.error(f"RegistryService: Docker client unavailable, cannot push {image_tag}.")
            return False, "Docker client unavailable."

        if not are_registry_credentials_set():
            logger.error(f"RegistryService: Registry credentials not set, cannot push {image_tag}.")
            return False, "Registry credentials not configured."

        # Attempt login before push, in case the initial login failed or token expired
        # if not self._login():
        #     logger.error(f"RegistryService: Login failed, cannot push {image_tag}.")
        #     return False, "Registry login failed."
        # Note: Docker client might handle token refresh automatically after initial login.
        # Re-logging in might not always be necessary, but can be added if pushes fail due to auth.

        logger.info(f"RegistryService: Pushing image {image_tag}...")
        push_log_output = ""
        try:
            # The push method returns a generator yielding progress lines
            push_generator = self.client.images.push(image_tag, stream=True, decode=True)
            for line in push_generator:
                if 'status' in line:
                    log_line = line['status']
                    if 'id' in line and line['id']:
                        log_line += f" ({line['id']})"
                    if 'progress' in line and line['progress']:
                         log_line += f" - {line['progress']}"
                    push_log_output += log_line + "\n"
                    # logger.debug(f"Push progress: {log_line}") # Optional: log progress
                elif 'errorDetail' in line:
                    error_msg = line['errorDetail']['message']
                    push_log_output += f"ERROR: {error_msg}\n"
                    logger.error(f"RegistryService: Error pushing {image_tag}: {error_msg}")
                    return False, push_log_output # Return False on first error
                else:
                     # Log unexpected lines
                     push_log_output += str(line) + "\n"
                     # logger.warning(f"RegistryService: Unexpected push output line: {line}")


            logger.info(f"RegistryService: Successfully pushed image {image_tag}")
            return True, push_log_output
        except APIError as e:
            logger.error(f"RegistryService: Docker API error pushing {image_tag}: {e}", exc_info=True)
            push_log_output += f"\nERROR: Docker API Error - {e.explanation}"
            return False, push_log_output
        except Exception as e:
             logger.error(f"RegistryService: Unexpected error pushing {image_tag}: {e}", exc_info=True)
             push_log_output += f"\nERROR: Unexpected error - {e}"
             return False, push_log_output

# Instantiate the service (consider if singleton is needed via Depends)
registry_service = RegistryService()
