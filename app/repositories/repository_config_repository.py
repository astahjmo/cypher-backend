from pymongo.database import Database
from pymongo.errors import ConnectionFailure, OperationFailure
from bson import ObjectId
from typing import List, Optional
import logging
from datetime import datetime, timezone

from models.github.db_models import RepositoryConfig
from models.base import PyObjectId
from services.db_service import get_database
from fastapi import Depends

logger = logging.getLogger(__name__)


class RepositoryConfigRepository:
    """Handles database operations for RepositoryConfig objects.

    Provides methods to find and upsert repository configurations in the MongoDB collection.

    Attributes:
        collection: The MongoDB collection instance for 'repository_configs'.
    """

    def __init__(self, db: Database):
        """Initializes the repository with the database connection.

        Args:
            db (Database): The pymongo Database instance.
        """
        self.collection = db["repository_configs"]
        logger.info("RepositoryConfigRepository initialized.")

    def find_by_user(self, user_id: PyObjectId) -> List[RepositoryConfig]:
        """Finds all repository configurations for a given user ID.

        Args:
            user_id (PyObjectId): The ObjectId of the user whose configurations to find.

        Returns:
            List[RepositoryConfig]: A list of RepositoryConfig instances found for the user.

        Raises:
            OperationFailure: If a database operation fails during the find query.
            Exception: For any other unexpected errors.
        """
        logger.debug(f"Finding repository configs for user ID: {user_id}")
        try:
            configs_cursor = self.collection.find({"user_id": user_id})
            return [
                RepositoryConfig.model_validate(config) for config in configs_cursor
            ]
        except OperationFailure as e:
            logger.error(
                f"Database operation failed finding configs for user {user_id}: {e}",
                exc_info=True,
            )
            raise
        except Exception as e:
            logger.error(
                f"Unexpected error finding configs for user {user_id}: {e}",
                exc_info=True,
            )
            raise

    def find_by_repo_and_user(
        self, repo_full_name: str, user_id: PyObjectId
    ) -> Optional[RepositoryConfig]:
        """Finds a specific repository configuration by repository full name and user ID.

        Args:
            repo_full_name (str): The full name of the repository (e.g., 'owner/repo').
            user_id (PyObjectId): The ObjectId of the user this configuration belongs to.

        Returns:
            Optional[RepositoryConfig]: The found configuration instance, or None if not found.

        Raises:
            OperationFailure: If a database operation fails during the find query.
            Exception: For any other unexpected errors.
        """
        logger.debug(
            f"Finding repository config for repo: {repo_full_name}, user ID: {user_id}"
        )
        try:
            config_data = self.collection.find_one(
                {"repo_full_name": repo_full_name, "user_id": user_id}
            )
            if config_data:
                return RepositoryConfig.model_validate(config_data)
            return None
        except OperationFailure as e:
            logger.error(
                f"Database operation failed finding config for repo {repo_full_name}, user {user_id}: {e}",
                exc_info=True,
            )
            raise
        except Exception as e:
            logger.error(
                f"Unexpected error finding config for repo {repo_full_name}, user {user_id}: {e}",
                exc_info=True,
            )
            raise

    def upsert_config(
        self, user_id: PyObjectId, repo_full_name: str, auto_build_branches: List[str]
    ) -> RepositoryConfig:
        """Creates or updates a repository configuration in the database.

        Sets `updated_at` timestamp. If inserting, also sets `created_at`.

        Args:
            user_id (PyObjectId): The ObjectId of the user this configuration belongs to.
            repo_full_name (str): The full name of the repository (e.g., 'owner/repo').
            auto_build_branches (List[str]): The list of branch names to configure for auto-build.

        Returns:
            RepositoryConfig: The saved or updated configuration instance.

        Raises:
            OperationFailure: If the database upsert operation fails or fails to return the document.
            Exception: For any other unexpected errors.
        """
        logger.info(
            f"Upserting repository config for repo: {repo_full_name}, user ID: {user_id}"
        )
        now = datetime.now(timezone.utc)
        try:
            update_result = self.collection.update_one(
                {"user_id": user_id, "repo_full_name": repo_full_name},
                {
                    "$set": {
                        "auto_build_branches": auto_build_branches,
                        "updated_at": now,
                    },
                    "$setOnInsert": {
                        "user_id": user_id,
                        "repo_full_name": repo_full_name,
                        "created_at": now,
                    },
                },
                upsert=True,
            )

            if update_result.upserted_id or update_result.matched_count > 0:
                config_data = self.collection.find_one(
                    {"user_id": user_id, "repo_full_name": repo_full_name}
                )
                if config_data:
                    return RepositoryConfig.model_validate(config_data)
                else:
                    logger.error(
                        f"Failed to fetch config for {repo_full_name} immediately after upsert."
                    )
                    raise OperationFailure(
                        "Failed to retrieve configuration after saving."
                    )
            else:
                logger.error(
                    f"Upsert operation for config {repo_full_name} reported no changes."
                )
                raise OperationFailure(
                    "Configuration save operation reported no changes."
                )

        except OperationFailure as e:
            logger.error(
                f"Database operation failed upserting config for repo {repo_full_name}, user {user_id}: {e}",
                exc_info=True,
            )
            raise
        except Exception as e:
            logger.error(
                f"Unexpected error upserting config for repo {repo_full_name}, user {user_id}: {e}",
                exc_info=True,
            )
            raise

    def find_all(self) -> List[RepositoryConfig]:
        """Finds all repository configurations in the database.

        Warning: Use with caution, as this can return a large number of documents.

        Returns:
            List[RepositoryConfig]: A list of all RepositoryConfig instances found.

        Raises:
            OperationFailure: If a database operation fails during the find query.
            Exception: For any other unexpected errors.
        """
        logger.debug("Finding all repository configs.")
        try:
            configs_cursor = self.collection.find({})
            return [
                RepositoryConfig.model_validate(config) for config in configs_cursor
            ]
        except OperationFailure as e:
            logger.error(
                f"Database operation failed finding all configs: {e}", exc_info=True
            )
            raise
        except Exception as e:
            logger.error(f"Unexpected error finding all configs: {e}", exc_info=True)
            raise

    def find_by_name(self, repo_full_name: str) -> List[RepositoryConfig]:
        """Finds all repository configurations matching a specific repository full name.

        Note: Multiple users might configure the same repository.

        Args:
            repo_full_name (str): The full name of the repository (e.g., 'owner/repo').

        Returns:
            List[RepositoryConfig]: A list of RepositoryConfig instances matching the name.

        Raises:
            OperationFailure: If a database operation fails during the find query.
            Exception: For any other unexpected errors.
        """
        logger.debug(f"Finding repository configs by name: {repo_full_name}")
        try:
            configs_cursor = self.collection.find({"repo_full_name": repo_full_name})
            return [
                RepositoryConfig.model_validate(config) for config in configs_cursor
            ]
        except OperationFailure as e:
            logger.error(
                f"Database operation failed finding configs by name {repo_full_name}: {e}",
                exc_info=True,
            )
            raise
        except Exception as e:
            logger.error(
                f"Unexpected error finding configs by name {repo_full_name}: {e}",
                exc_info=True,
            )
            raise


def get_repo_config_repository(
    db: Database = Depends(get_database),
) -> RepositoryConfigRepository:
    """FastAPI dependency function to get an instance of RepositoryConfigRepository.

    Injects the database connection provided by the `get_database` dependency.

    Args:
        db (Database): The database instance obtained from `get_database`.

    Returns:
        RepositoryConfigRepository: An instance of the RepositoryConfigRepository.
    """
    return RepositoryConfigRepository(db=db)
