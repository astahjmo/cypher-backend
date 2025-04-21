from pymongo.database import Database
from pymongo.errors import ConnectionFailure, OperationFailure
from pymongo import ReturnDocument
from bson import ObjectId
from typing import Optional, List
import logging
from datetime import datetime, timezone

from models.container.db_models import ContainerRuntimeConfig
from models.base import PyObjectId
from services.db_service import get_database
from fastapi import Depends

logger = logging.getLogger(__name__)


class ContainerRuntimeConfigRepository:
    """Handles database operations for ContainerRuntimeConfig objects.

    Provides methods to retrieve and upsert container runtime configurations
    in the MongoDB collection.

    Attributes:
        collection: The MongoDB collection instance for 'container_runtime_configs'.
    """

    def __init__(self, db: Database):
        """Initializes the repository with the database connection.

        Args:
            db (Database): The pymongo Database instance.
        """
        self.collection = db["container_runtime_configs"]
        logger.info("ContainerRuntimeConfigRepository initialized.")

    def get_by_repo_and_user(
        self, repo_full_name: str, user_id: PyObjectId
    ) -> Optional[ContainerRuntimeConfig]:
        """Finds a container runtime configuration by repository full name and user ID.

        Args:
            repo_full_name (str): The full name of the repository (e.g., 'owner/repo').
            user_id (PyObjectId): The ObjectId of the user this configuration belongs to.

        Returns:
            Optional[ContainerRuntimeConfig]: The found configuration instance, or None if not found.

        Raises:
            OperationFailure: If a database operation fails during the find query.
            Exception: For any other unexpected errors.
        """
        logger.debug(
            f"Finding container runtime config for repo: {repo_full_name}, user ID: {user_id}"
        )
        try:
            # Note: Storing user_id as ObjectId in the model, but might need string comparison if stored differently.
            # Assuming user_id in DB is ObjectId here based on the model definition.
            config_data = self.collection.find_one(
                {"repo_full_name": repo_full_name, "user_id": user_id}
            )
            if config_data:
                return ContainerRuntimeConfig.model_validate(config_data)
            return None
        except OperationFailure as e:
            logger.error(
                f"Database operation failed finding runtime config for repo {repo_full_name}, user {user_id}: {e}",
                exc_info=True,
            )
            raise
        except Exception as e:
            logger.error(
                f"Unexpected error finding runtime config for repo {repo_full_name}, user {user_id}: {e}",
                exc_info=True,
            )
            raise

    def upsert(self, config: ContainerRuntimeConfig) -> ContainerRuntimeConfig:
        """Creates or updates a container runtime configuration in the database.

        Sets the `updated_at` timestamp. If inserting, also sets `created_at`.

        Args:
            config (ContainerRuntimeConfig): The configuration model instance to save.

        Returns:
            ContainerRuntimeConfig: The saved or updated configuration instance.

        Raises:
            OperationFailure: If the database upsert operation fails or fails to return the document.
            Exception: For any other unexpected errors.
        """
        logger.info(
            f"Upserting container runtime config for repo: {config.repo_full_name}, user ID: {config.user_id}"
        )
        try:
            config.updated_at = datetime.now(timezone.utc)
            # Dump model respecting aliases (_id) and excluding fields not set during update
            update_data = config.model_dump(by_alias=True, exclude={"id", "created_at"})

            updated_doc = self.collection.find_one_and_update(
                {"repo_full_name": config.repo_full_name, "user_id": config.user_id},
                {
                    "$set": update_data,
                    # Set created_at only if the document is being inserted (upsert=True)
                    "$setOnInsert": {
                        "created_at": config.created_at or datetime.now(timezone.utc)
                    },
                },
                upsert=True,
                return_document=ReturnDocument.AFTER,
            )

            if updated_doc:
                return ContainerRuntimeConfig.model_validate(updated_doc)
            else:
                logger.error(
                    f"Upsert operation for runtime config {config.repo_full_name} failed to return document."
                )
                raise OperationFailure("Failed to retrieve configuration after saving.")

        except OperationFailure as e:
            logger.error(
                f"Database operation failed upserting runtime config for repo {config.repo_full_name}, user {config.user_id}: {e}",
                exc_info=True,
            )
            raise
        except Exception as e:
            logger.error(
                f"Unexpected error upserting runtime config for repo {config.repo_full_name}, user {config.user_id}: {e}",
                exc_info=True,
            )
            raise


def get_container_runtime_config_repo(
    db: Database = Depends(get_database),
) -> ContainerRuntimeConfigRepository:
    """FastAPI dependency function to get an instance of ContainerRuntimeConfigRepository.

    Injects the database connection provided by the `get_database` dependency.

    Args:
        db (Database): The database instance obtained from `get_database`.

    Returns:
        ContainerRuntimeConfigRepository: An instance of the ContainerRuntimeConfigRepository.
    """
    return ContainerRuntimeConfigRepository(db=db)
