from pymongo.database import Database
from pymongo.errors import ConnectionFailure, OperationFailure
from pymongo import ReturnDocument, DESCENDING
from bson import ObjectId # Keep ObjectId for _id handling
from typing import List, Optional, Dict, Any
import logging
from datetime import datetime, timezone

# Import BuildStatus model from its new location
from models.build.db_models import BuildStatus
# Import PyObjectId from base (still needed for _id)
from models.base import PyObjectId
# Import get_database dependency
from services.db_service import get_database
from fastapi import Depends # Keep Depends for dependency injection

logger = logging.getLogger(__name__)

class BuildStatusRepository:
    """Handles database operations for BuildStatus objects."""

    def __init__(self, db: Database):
        self.collection = db["build_statuses"]
        logger.info("BuildStatusRepository initialized.")

    def create(self, build_status: BuildStatus) -> BuildStatus:
        """Creates a new build status record."""
        logger.info(f"Creating new build status for repo: {build_status.repo_full_name}, branch: {build_status.branch}")
        try:
            # Ensure dates are set correctly before insertion
            now = datetime.now(timezone.utc)
            build_status.created_at = now
            build_status.updated_at = now
            # Use model_dump for Pydantic v2. It should respect the 'str' type for user_id.
            insert_data = build_status.model_dump(by_alias=True, exclude={'id'})
            # Double-check type just before insert (optional, for debugging)
            if not isinstance(insert_data.get('user_id'), str):
                 logger.error(f"CRITICAL: user_id is not a string before insert: {type(insert_data.get('user_id'))}")
                 # Potentially convert here if absolutely necessary, but the model should dictate this
                 # insert_data['user_id'] = str(insert_data['user_id'])

            result = self.collection.insert_one(insert_data)
            # Fetch the inserted document to return the complete object with ID
            created_doc = self.collection.find_one({"_id": result.inserted_id})
            if created_doc:
                logger.info(f"Successfully created and fetched build status with ID: {result.inserted_id}")
                return BuildStatus.model_validate(created_doc) # Use model_validate
            else:
                logger.error(f"Failed to fetch build status immediately after insertion with ID: {result.inserted_id}")
                raise OperationFailure("Failed to retrieve build status after creation.")
        except OperationFailure as e:
            logger.error(f"Database operation failed creating build status: {e}", exc_info=True)
            raise
        except Exception as e:
            logger.error(f"Unexpected error creating build status: {e}", exc_info=True)
            raise

    def get_by_id(self, build_id: str) -> Optional[BuildStatus]:
        """Finds a build status by its MongoDB ObjectId string."""
        logger.debug(f"Finding build status by ID: {build_id}")
        try:
            obj_id = PyObjectId(build_id) # Validate and convert _id
            build_data = self.collection.find_one({"_id": obj_id})
            if build_data:
                logger.debug(f"Found build status by ID {build_id}: {build_data}")
                return BuildStatus.model_validate(build_data) # Use model_validate
            logger.debug(f"Build status not found for ID: {build_id}")
            return None
        except ValueError: # Handle invalid ObjectId format for build_id
            logger.warning(f"Invalid ObjectId format provided for get_by_id: {build_id}")
            return None
        except OperationFailure as e:
            logger.error(f"Database operation failed finding build status by ID {build_id}: {e}", exc_info=True)
            raise
        except Exception as e:
            logger.error(f"Unexpected error finding build status by ID {build_id}: {e}", exc_info=True)
            raise

    # Note: The user_id parameter type hint might need adjustment if it comes from User model which might still use PyObjectId
    # Let's assume for now it's passed correctly as string or PyObjectId and handle it inside
    def get_by_id_and_user(self, build_id: str, user_id: Any) -> Optional[BuildStatus]:
        """Finds a build status by ID, ensuring it belongs to the specified user (comparing user_id as string)."""
        # Convert incoming user_id to string for comparison, regardless of its original type
        user_id_str = str(user_id)
        logger.info(f"Attempting to find build status by ID: {build_id} for user ID (as string): {user_id_str}")
        try:
            obj_id = PyObjectId(build_id) # Validate and convert build_id
            logger.debug(f"Converted build_id to ObjectId: {obj_id}")

            # Query using string comparison for user_id
            build_data = self.collection.find_one({"_id": obj_id, "user_id": user_id_str})
            print(build_data)

            if build_data:
                logger.info(f"Successfully found build {build_id} belonging to user {user_id_str}.")
                return BuildStatus.model_validate(build_data) # Use model_validate
            else:
                # Check if build exists at all to provide better warning
                build_exists = self.collection.find_one({"_id": obj_id})
                if build_exists:
                     found_user_id = build_exists.get('user_id')
                     logger.warning(f"Build {build_id} found, but its user_id '{found_user_id}' (type: {type(found_user_id)}) does not match requesting user_id '{user_id_str}'.")
                else:
                     logger.warning(f"Build with ID {build_id} (ObjectId: {obj_id}) does not exist in the database.")
                return None
        except ValueError: # Handle invalid ObjectId format for build_id
            logger.warning(f"Invalid ObjectId format provided for get_by_id_and_user build_id: {build_id}")
            return None
        except OperationFailure as e:
            logger.error(f"Database operation failed finding build status {build_id} for user {user_id_str}: {e}", exc_info=True)
            raise
        except Exception as e:
            logger.error(f"Unexpected error finding build status {build_id} for user {user_id_str}: {e}", exc_info=True)
            raise

    def update_status(self, build_id: str, status: str, message: Optional[str] = None, image_tag: Optional[str] = None, commit_sha: Optional[str] = None, commit_message: Optional[str] = None) -> Optional[BuildStatus]:
        """Updates the status, message, image_tag, and potentially commit info of a build."""
        logger.info(f"Updating build status for ID: {build_id} to status: {status}")
        try:
            obj_id = PyObjectId(build_id) # Validate and convert build_id
            now = datetime.now(timezone.utc)
            update_fields = {"status": status, "updated_at": now}
            if message is not None:
                update_fields["message"] = message
            if image_tag is not None:
                update_fields["image_tag"] = image_tag
            if commit_sha is not None:
                update_fields["commit_sha"] = commit_sha
            if commit_message is not None:
                update_fields["commit_message"] = commit_message
            if status in ["running"] and "started_at" not in update_fields:
                 update_fields["started_at"] = now
            if status in ["success", "failed", "cancelled"] and "completed_at" not in update_fields:
                 update_fields["completed_at"] = now

            updated_doc = self.collection.find_one_and_update(
                {"_id": obj_id},
                {"$set": update_fields},
                return_document=ReturnDocument.AFTER
            )
            if updated_doc:
                logger.info(f"Successfully updated build status for ID: {build_id}")
                return BuildStatus.model_validate(updated_doc)
            else:
                logger.warning(f"Build status update failed: Build ID {build_id} not found.")
                return None
        except ValueError: # Handle invalid ObjectId format for build_id
            logger.warning(f"Invalid ObjectId format provided for update_status: {build_id}")
            return None
        except OperationFailure as e:
            logger.error(f"Database operation failed updating build status {build_id}: {e}", exc_info=True)
            raise
        except Exception as e:
            logger.error(f"Unexpected error updating build status {build_id}: {e}", exc_info=True)
            raise

    # Note: The user_id parameter type hint might need adjustment if it comes from User model
    def find_by_user(self, user_id: Any, limit: int = 20) -> List[BuildStatus]:
        """Finds build statuses for a user (comparing user_id as string), sorted by creation date descending."""
        user_id_str = str(user_id)
        logger.debug(f"Finding build statuses for user ID (as string): {user_id_str}, limit: {limit}")
        try:
            # Query using string comparison for user_id
            query = self.collection.find({"user_id": user_id_str}).sort("created_at", DESCENDING)
            if limit > 0:
                query = query.limit(limit)
            results = [BuildStatus.model_validate(doc) for doc in query]
            logger.debug(f"Found {len(results)} build statuses for user {user_id_str}")
            return results
        except OperationFailure as e:
            logger.error(f"Database operation failed finding builds for user {user_id_str}: {e}", exc_info=True)
            raise
        except Exception as e:
            logger.error(f"Unexpected error finding builds for user {user_id_str}: {e}", exc_info=True)
            raise

    # Note: The user_id parameter type hint might need adjustment if it comes from User model
    def find_latest_successful_build(self, repo_full_name: str, user_id: Any) -> Optional[BuildStatus]:
        """Finds the most recent successful build for a specific repo and user (comparing user_id as string)."""
        user_id_str = str(user_id)
        logger.debug(f"Finding latest successful build for repo: {repo_full_name}, user ID (as string): {user_id_str}")
        try:
            # Query using string comparison for user_id
            build_data = self.collection.find_one(
                {"repo_full_name": repo_full_name, "user_id": user_id_str, "status": "success", "image_tag": {"$ne": None}},
                sort=[("completed_at", DESCENDING)]
            )
            if build_data:
                logger.debug(f"Found latest successful build: {build_data['_id']}")
                return BuildStatus.model_validate(build_data)
            logger.debug(f"No successful build found for repo {repo_full_name}, user {user_id_str}")
            return None
        except OperationFailure as e:
            logger.error(f"Database operation failed finding latest build for repo {repo_full_name}, user {user_id_str}: {e}", exc_info=True)
            raise
        except Exception as e:
            logger.error(f"Unexpected error finding latest build for repo {repo_full_name}, user {user_id_str}: {e}", exc_info=True)
            raise

# --- FastAPI Dependency ---
def get_build_status_repository(db: Database = Depends(get_database)) -> BuildStatusRepository:
    """FastAPI dependency to get an instance of BuildStatusRepository."""
    return BuildStatusRepository(db=db)
