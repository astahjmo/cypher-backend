from pymongo.database import Database
from pymongo.errors import ConnectionFailure, OperationFailure
from bson import ObjectId
from typing import List, Optional
import logging
from datetime import datetime, timezone

# Import RepositoryConfig model from its new location
from models.github.db_models import RepositoryConfig
# Import PyObjectId from base
from models.base import PyObjectId
# Import get_database dependency
from services.db_service import get_database
from fastapi import Depends # Keep Depends for dependency injection

logger = logging.getLogger(__name__)

class RepositoryConfigRepository:
    """Handles database operations for RepositoryConfig objects."""

    def __init__(self, db: Database):
        self.collection = db["repository_configs"]
        logger.info("RepositoryConfigRepository initialized.")

    def find_by_user(self, user_id: PyObjectId) -> List[RepositoryConfig]:
        """Finds all repository configurations for a given user ID."""
        logger.debug(f"Finding repository configs for user ID: {user_id}")
        try:
            configs_cursor = self.collection.find({"user_id": user_id})
            # Use model_validate for each document
            return [RepositoryConfig.model_validate(config) for config in configs_cursor]
        except OperationFailure as e:
            logger.error(f"Database operation failed finding configs for user {user_id}: {e}", exc_info=True)
            raise
        except Exception as e:
            logger.error(f"Unexpected error finding configs for user {user_id}: {e}", exc_info=True)
            raise

    def find_by_repo_and_user(self, repo_full_name: str, user_id: PyObjectId) -> Optional[RepositoryConfig]:
        """Finds a specific repository configuration by repo name and user ID."""
        logger.debug(f"Finding repository config for repo: {repo_full_name}, user ID: {user_id}")
        try:
            config_data = self.collection.find_one({"repo_full_name": repo_full_name, "user_id": user_id})
            if config_data:
                return RepositoryConfig.model_validate(config_data) # Use model_validate
            return None
        except OperationFailure as e:
            logger.error(f"Database operation failed finding config for repo {repo_full_name}, user {user_id}: {e}", exc_info=True)
            raise
        except Exception as e:
            logger.error(f"Unexpected error finding config for repo {repo_full_name}, user {user_id}: {e}", exc_info=True)
            raise

    def upsert_config(self, user_id: PyObjectId, repo_full_name: str, auto_build_branches: List[str]) -> RepositoryConfig:
        """Creates or updates a repository configuration."""
        logger.info(f"Upserting repository config for repo: {repo_full_name}, user ID: {user_id}")
        now = datetime.now(timezone.utc)
        try:
            update_result = self.collection.update_one(
                {"user_id": user_id, "repo_full_name": repo_full_name},
                {
                    "$set": {
                        "auto_build_branches": auto_build_branches,
                        "updated_at": now
                    },
                    "$setOnInsert": {
                        "user_id": user_id,
                        "repo_full_name": repo_full_name,
                        "created_at": now
                    }
                },
                upsert=True
            )

            if update_result.upserted_id or update_result.matched_count > 0:
                 # Fetch the document after upsert to return the full object
                 config_data = self.collection.find_one({"user_id": user_id, "repo_full_name": repo_full_name})
                 if config_data:
                     return RepositoryConfig.model_validate(config_data) # Use model_validate
                 else:
                     # This should ideally not happen after a successful upsert
                     logger.error(f"Failed to fetch config for {repo_full_name} immediately after upsert.")
                     raise OperationFailure("Failed to retrieve configuration after saving.")
            else:
                 logger.error(f"Upsert operation for config {repo_full_name} reported no changes.")
                 raise OperationFailure("Configuration save operation reported no changes.")

        except OperationFailure as e:
            logger.error(f"Database operation failed upserting config for repo {repo_full_name}, user {user_id}: {e}", exc_info=True)
            raise
        except Exception as e:
            logger.error(f"Unexpected error upserting config for repo {repo_full_name}, user {user_id}: {e}", exc_info=True)
            raise

    def find_all(self) -> List[RepositoryConfig]:
        """Finds all repository configurations (use with caution, potentially large)."""
        logger.debug("Finding all repository configs.")
        try:
            configs_cursor = self.collection.find({})
            return [RepositoryConfig.model_validate(config) for config in configs_cursor]
        except OperationFailure as e:
            logger.error(f"Database operation failed finding all configs: {e}", exc_info=True)
            raise
        except Exception as e:
            logger.error(f"Unexpected error finding all configs: {e}", exc_info=True)
            raise

    def find_by_name(self, repo_full_name: str) -> Optional[RepositoryConfig]:
        print(repo_full_name)
        try:
            repo = self.collection.find_one({'repo_full_name': repo_full_name})
            print(self.collection)
            print(repo)
            if repo != None:
                return RepositoryConfig.model_validate(repo)
        except OperationFailure as e:
            logger.error(f"Database operation failed finding all configs: {e}", exc_info=True)
            raise
        except Exception as e:
            logger.error(f"Unexpected error finding all configs: {e}", exc_info=True)
            raise
# --- FastAPI Dependency ---
def get_repo_config_repository(db: Database = Depends(get_database)) -> RepositoryConfigRepository:
    """FastAPI dependency to get an instance of RepositoryConfigRepository."""
    return RepositoryConfigRepository(db=db)
