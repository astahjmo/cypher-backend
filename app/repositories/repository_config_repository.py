import logging
from pymongo.database import Database
from pymongo import ReturnDocument
from datetime import datetime, timezone # Import timezone
from bson import ObjectId
from typing import List, Optional
from fastapi import Depends # Import Depends

# Import models and db service
from models import RepositoryConfig, PyObjectId, now_utc # Import now_utc helper
from services.db_service import get_database # Import get_database

logger = logging.getLogger(__name__)

class RepositoryConfigRepository:
    def __init__(self, db: Database):
        self.collection = db["repository_configs"]

    def find_by_user_and_repo(self, user_id: PyObjectId, repo_full_name: str) -> Optional[RepositoryConfig]:
        """Finds a repository configuration by user ID and full repository name."""
        config_doc = self.collection.find_one({"user_id": user_id, "repo_full_name": repo_full_name})
        if config_doc:
            return RepositoryConfig(**config_doc)
        return None

    def find_by_user(self, user_id: PyObjectId) -> List[RepositoryConfig]:
        """Finds all repository configurations for a given user ID."""
        config_docs = self.collection.find({"user_id": user_id})
        return [RepositoryConfig(**doc) for doc in config_docs]

    # --- NEW: Method to find all configurations ---
    def find_all(self) -> List[RepositoryConfig]:
        """Finds all repository configurations in the collection."""
        config_docs = self.collection.find({})
        return [RepositoryConfig(**doc) for doc in config_docs]
    # --- End NEW ---

    def upsert_config(self, user_id: PyObjectId, repo_full_name: str, branches: List[str]) -> RepositoryConfig:
        """Creates or updates a repository configuration."""
        now = now_utc()
        updated_doc = self.collection.find_one_and_update(
            {"user_id": user_id, "repo_full_name": repo_full_name},
            {
                "$set": {
                    "auto_build_branches": branches,
                    "updated_at": now
                },
                "$setOnInsert": {
                    "user_id": user_id,
                    "repo_full_name": repo_full_name,
                    "created_at": now
                }
            },
            upsert=True,
            return_document=ReturnDocument.AFTER
        )

        if not updated_doc:
             logger.error(f"Upsert failed to return document for user {user_id}, repo {repo_full_name}")
             raise Exception("Failed to create or update repository configuration.")

        logger.info(f"Upserted repository config for user {user_id}, repo {repo_full_name}")
        return RepositoryConfig(**updated_doc)

    def delete_config(self, user_id: PyObjectId, repo_full_name: str) -> bool:
        """Deletes a repository configuration."""
        result = self.collection.delete_one({"user_id": user_id, "repo_full_name": repo_full_name})
        deleted = result.deleted_count > 0
        if deleted:
            logger.info(f"Deleted repository config for user {user_id}, repo {repo_full_name}")
        else:
            logger.warning(f"Attempted to delete non-existent config for user {user_id}, repo {repo_full_name}")
        return deleted

    def is_branch_configured_for_build(self, repo_full_name: str, branch: str) -> bool:
        """
        Checks if a specific branch within a repository is configured for automatic builds (by any user).
        """
        config_doc = self.collection.find_one({
            "repo_full_name": repo_full_name,
            "auto_build_branches": branch
        })
        is_configured = config_doc is not None
        if is_configured:
            logger.info(f"Branch '{branch}' in repo '{repo_full_name}' IS configured for auto-build.")
        else:
            logger.info(f"Branch '{branch}' in repo '{repo_full_name}' is NOT configured for auto-build.")
        return is_configured


# --- Dependency Function ---
def get_repo_config_repository(db: Database = Depends(get_database)) -> RepositoryConfigRepository:
    """Dependency function to provide a RepositoryConfigRepository instance."""
    return RepositoryConfigRepository(db)
