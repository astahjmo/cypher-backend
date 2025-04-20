import logging
from pymongo.database import Database
from pymongo import DESCENDING
from datetime import datetime, timezone # Import timezone
from bson import ObjectId
from typing import List, Optional, Dict, Any
from fastapi import Depends # Import Depends

# Import models and db service
from models import BuildStatus, PyObjectId, now_utc # Import now_utc helper
from services.db_service import get_database # Import get_database

logger = logging.getLogger(__name__)

class BuildStatusRepository:
    def __init__(self, db: Database):
        self.collection = db["build_statuses"]

    def create_build_status(self,tag_version: str, user_id: PyObjectId, repo_full_name: str, branch: str, commit_sha: Optional[str] = None, commit_message: Optional[str] = None) -> BuildStatus:
        """Creates a new build status record."""
        now = now_utc()
        build_doc = {
            "_id": ObjectId(),
            "user_id": user_id,
            "repo_full_name": repo_full_name,
            "branch": branch,
            "commit_sha": commit_sha,
            "commit_message": commit_message,
            "status": "pending",
            "created_at": now,
            "updated_at": now
        }
        insert_result = self.collection.insert_one(build_doc)
        created_doc = self.collection.find_one({"_id": insert_result.inserted_id})
        if not created_doc:
             logger.error(f"Failed to retrieve newly created build status for user {user_id}, repo {repo_full_name}")
             raise Exception("Failed to retrieve newly created build status document.")
        logger.info(f"Created build status {created_doc['_id']} for {repo_full_name} branch {branch}")
        return BuildStatus(**created_doc)


    def get_build_by_id(self, build_id: PyObjectId) -> Optional[BuildStatus]:
        """Retrieves a build status by its ID."""
        build_doc = self.collection.find_one({"_id": build_id})
        if build_doc:
            return BuildStatus(**build_doc)
        return None

    def get_recent_builds_by_user(self, user_id: PyObjectId, limit: int = 20) -> List[BuildStatus]:
        """Retrieves recent build statuses for a specific user."""
        build_docs = self.collection.find({"user_id": user_id}).sort("created_at", DESCENDING).limit(limit)
        return [BuildStatus(**doc) for doc in build_docs]

    def get_all_builds(self, limit: int = 100) -> List[BuildStatus]:
        """Retrieves all build statuses, sorted by creation date descending."""
        build_docs = self.collection.find().sort("created_at", DESCENDING).limit(limit)
        return [BuildStatus(**doc) for doc in build_docs]

    def update_build_status(self, build_id: PyObjectId, updates: Dict[str, Any]) -> bool:
        """Updates specific fields of a build status record."""
        if not updates:
            return False
        now = now_utc()
        updates["updated_at"] = now
        if "status" in updates and updates["status"] in ["success", "failed", "cancelled"]:
             updates["completed_at"] = now

        result = self.collection.update_one({"_id": build_id}, {"$set": updates})

        if result.matched_count == 0:
            logger.warning(f"Attempted to update non-existent build status {build_id}")
            return False
        if result.modified_count > 0:
             logger.info(f"Updated build status {build_id} with fields: {list(updates.keys())}")
             return True
        logger.info(f"Build status {build_id} already had the target values. No update performed.")
        return True

    # --- NEW Method to find latest successful build ---
    def find_latest_successful_build(self, repo_full_name: str, user_id: PyObjectId) -> Optional[BuildStatus]:
        """Finds the most recent successful build for a specific repo and user."""
        build_doc = self.collection.find_one(
            {
                "repo_full_name": repo_full_name,
                "user_id": user_id,
                "status": "success",
                "image_tag": {"$ne": None} # Ensure it has an image tag
            },
            sort=[("created_at", DESCENDING)]
        )
        if build_doc:
            logger.info(f"Found latest successful build {build_doc['_id']} for {repo_full_name}, user {user_id}")
            return BuildStatus(**build_doc)
        else:
            logger.info(f"No successful build found for {repo_full_name}, user {user_id}")
            return None
    # --- End NEW ---

# --- Dependency Function ---
def get_build_status_repository(db: Database = Depends(get_database)) -> BuildStatusRepository:
    """Dependency function to provide a BuildStatusRepository instance."""
    return BuildStatusRepository(db)
