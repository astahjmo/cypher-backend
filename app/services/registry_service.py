import docker
from docker.errors import APIError
import logging

from config import settings

logger = logging.getLogger(__name__)


def are_registry_credentials_set() -> bool:
    """Checks if Docker registry credentials (URL, username, password) are configured in settings.

    Returns:
        bool: True if all necessary registry credentials are set, False otherwise.
    """
    configured = bool(
        settings.DOCKER_REGISTRY_URL
        and settings.DOCKER_REGISTRY_USERNAME
        and settings.DOCKER_REGISTRY_PASSWORD
    )
    if not configured:
        logger.info("Registry check: Credentials or URL not fully configured.")
    return configured


class RegistryService:
    """Handles interactions with a configured Docker registry, primarily login and image push.

    Attributes:
        client: The Docker client instance obtained from the environment.
    """

    def __init__(self):
        """Initializes the RegistryService and the Docker client.

        Attempts to log in to the configured registry immediately if credentials are set.
        """
        try:
            self.client = docker.from_env()
            logger.info("RegistryService: Docker client initialized.")
            self._login()  # Attempt login on initialization
        except Exception as e:
            logger.error(
                f"RegistryService: Failed to initialize Docker client: {e}",
                exc_info=True,
            )
            self.client = None

    def _login(self) -> bool:
        """Logs in to the configured Docker registry using credentials from settings.

        This is called during initialization and potentially before push operations
        if needed (though the docker client might handle token refresh).

        Returns:
            bool: True if login was successful or not needed (credentials not set),
                  False if login was attempted but failed.
        """
        if not self.client:
            logger.error("RegistryService: Docker client unavailable, cannot login.")
            return False

        if are_registry_credentials_set():
            logger.info(
                f"RegistryService: Attempting login to {settings.DOCKER_REGISTRY_URL} as {settings.DOCKER_REGISTRY_USERNAME}"
            )
            try:
                login_result = self.client.login(
                    username=settings.DOCKER_REGISTRY_USERNAME,
                    password=settings.DOCKER_REGISTRY_PASSWORD,
                    registry=settings.DOCKER_REGISTRY_URL,
                )
                logger.info(f"RegistryService: Login successful: {login_result}")
                return True
            except APIError as e:
                logger.error(
                    f"RegistryService: Docker registry login failed: {e}", exc_info=True
                )
                return False
            except Exception as e:
                logger.error(
                    f"RegistryService: Unexpected error during registry login: {e}",
                    exc_info=True,
                )
                return False
        else:
            logger.info("RegistryService: Credentials not set, skipping login.")
            # Return True here as login wasn't required/attempted due to config
            return True

    def push_image(self, image_tag: str) -> tuple[bool, str]:
        """Pushes a locally tagged Docker image to the configured remote registry.

        Checks for client availability and registry configuration before attempting the push.
        Streams the push progress and captures the output.

        Args:
            image_tag (str): The full tag of the image to push (e.g., 'registry.example.com/owner/repo:latest').

        Returns:
            tuple[bool, str]: A tuple containing:
                - bool: True if the push was successful, False otherwise.
                - str: The captured log output from the push operation (including status and errors).
        """
        if not self.client:
            logger.error(
                f"RegistryService: Docker client unavailable, cannot push {image_tag}."
            )
            return False, "Docker client unavailable."

        if not are_registry_credentials_set():
            logger.error(
                f"RegistryService: Registry credentials not set, cannot push {image_tag}."
            )
            return False, "Registry credentials not configured."

        # Consider re-login if needed:
        # if not self._login():
        #     return False, "Registry login failed before push."

        logger.info(f"RegistryService: Pushing image {image_tag}...")
        push_log_output = ""
        try:
            push_generator = self.client.images.push(
                image_tag, stream=True, decode=True
            )
            for line in push_generator:
                if "status" in line:
                    log_line = line["status"]
                    if "id" in line and line["id"]:
                        log_line += f" ({line['id']})"
                    if "progress" in line and line["progress"]:
                        log_line += f" - {line['progress']}"
                    push_log_output += log_line + "\n"
                elif "errorDetail" in line:
                    error_msg = line["errorDetail"]["message"]
                    push_log_output += f"ERROR: {error_msg}\n"
                    logger.error(
                        f"RegistryService: Error pushing {image_tag}: {error_msg}"
                    )
                    return False, push_log_output  # Fail fast on error
                else:
                    push_log_output += str(line) + "\n"

            logger.info(f"RegistryService: Successfully pushed image {image_tag}")
            return True, push_log_output
        except APIError as e:
            logger.error(
                f"RegistryService: Docker API error pushing {image_tag}: {e}",
                exc_info=True,
            )
            push_log_output += f"\nERROR: Docker API Error - {e.explanation}"
            return False, push_log_output
        except Exception as e:
            logger.error(
                f"RegistryService: Unexpected error pushing {image_tag}: {e}",
                exc_info=True,
            )
            push_log_output += f"\nERROR: Unexpected error - {e}"
            return False, push_log_output


# Singleton instance of the RegistryService
registry_service = RegistryService()
