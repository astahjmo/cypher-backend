from pymongo.database import Database
from pymongo.errors import ConnectionFailure, OperationFailure
from pymongo import ReturnDocument
from bson import ObjectId
from typing import Optional, List
import logging
from datetime import datetime, timezone

# Import ContainerRuntimeConfig model from its new location
from models.container.db_models import ContainerRuntimeConfig
# Import PyObjectId from base
from models.base import PyObjectId
# Import get_database dependency
from services.db_service import get_database
from fastapi import Depends # Keep Depends for dependency injection

logger = logging.getLogger(__name__)

class ContainerRuntimeConfigRepository:
    """Handles database operations for ContainerRuntimeConfig objects."""

    def __init__(self, db: Database):
        self.collection = db["container_runtime_configs"]
        logger.info("ContainerRuntimeConfigRepository initialized.")

    def get_by_repo_and_user(self, repo_full_name: str, user_id: PyObjectId) -> Optional[ContainerRuntimeConfig]:
        """Finds a configuration by repository full name and user ID."""
        print(user_id)
        logger.debug(f"Finding container runtime config for repo: {repo_full_name}, user ID: {user_id}")
        try:
            config_data = self.collection.find_one({"repo_full_name": repo_full_name, "user_id": str(user_id)})
            if config_data:
                return ContainerRuntimeConfig.model_validate(config_data) # Use model_validate
            return None
        except OperationFailure as e:
            logger.error(f"Database operation failed finding runtime config for repo {repo_full_name}, user {user_id}: {e}", exc_info=True)
            raise
        except Exception as e:
            logger.error(f"Unexpected error finding runtime config for repo {repo_full_name}, user {user_id}: {e}", exc_info=True)
            raise

    def upsert(self, config: ContainerRuntimeConfig) -> ContainerRuntimeConfig:
        """Creates or updates a container runtime configuration."""
        logger.info(f"Upserting container runtime config for repo: {config.repo_full_name}, user ID: {config.user_id}")
        try:
            # Ensure dates are set correctly before upsert
            config.updated_at = datetime.now(timezone.utc)
            # Use model_dump for Pydantic v2, exclude 'id' if it's default factory generated
            # Use by_alias=True to handle the '_id' field correctly
            update_data = config.model_dump(by_alias=True, exclude={'id', 'created_at'}) # Exclude created_at from $set

            # Perform the upsert operation
            updated_doc = self.collection.find_one_and_update(
                {"repo_full_name": config.repo_full_name, "user_id": config.user_id},
                {
                    "$set": update_data,
                    "$setOnInsert": {"created_at": config.created_at or datetime.now(timezone.utc)} # Set created_at only on insert
                },
                upsert=True,
                return_document=ReturnDocument.AFTER # Return the modified document
            )

            if updated_doc:
                return ContainerRuntimeConfig.model_validate(updated_doc) # Use model_validate
            else:
                # This case might indicate an issue with the upsert logic or query
                logger.error(f"Upsert operation for runtime config {config.repo_full_name} failed to return document.")
                raise OperationFailure("Failed to retrieve configuration after saving.")

        except OperationFailure as e:
            logger.error(f"Database operation failed upserting runtime config for repo {config.repo_full_name}, user {config.user_id}: {e}", exc_info=True)
            raise
        except Exception as e:
            logger.error(f"Unexpected error upserting runtime config for repo {config.repo_full_name}, user {config.user_id}: {e}", exc_info=True)
            raise

# --- FastAPI Dependency ---
def get_container_runtime_config_repo(db: Database = Depends(get_database)) -> ContainerRuntimeConfigRepository:
    """FastAPI dependency to get an instance of ContainerRuntimeConfigRepository."""
    return ContainerRuntimeConfigRepository(db=db)
