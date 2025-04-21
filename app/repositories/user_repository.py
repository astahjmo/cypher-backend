from pymongo.database import Database
from pymongo.errors import ConnectionFailure, OperationFailure
from bson import ObjectId
from typing import Optional, Dict, Any
import logging
from datetime import datetime, timezone # Adicionado import

# Import User model from its new location
from models.auth.db_models import User
# Import PyObjectId from base
from models.base import PyObjectId
# Import get_database dependency
from services.db_service import get_database
from fastapi import Depends # Keep Depends for dependency injection

logger = logging.getLogger(__name__)

class UserRepository:
    """Handles database operations for User objects."""

    def __init__(self, db: Database):
        self.collection = db["users"]
        logger.info("UserRepository initialized with database connection.")

    def find_user_by_github_id(self, github_id: int) -> Optional[User]:
        """Finds a user by their GitHub ID."""
        logger.debug(f"Finding user by GitHub ID: {github_id}")
        try:
            user_data = self.collection.find_one({"github_id": github_id})
            if user_data:
                # Use parse_obj to handle potential extra fields from DB gracefully
                return User.model_validate(user_data) # Use model_validate for Pydantic v2
            return None
        except OperationFailure as e:
            logger.error(f"Database operation failed finding user by github_id {github_id}: {e}", exc_info=True)
            raise # Re-raise DB errors
        except Exception as e:
            logger.error(f"Unexpected error finding user by github_id {github_id}: {e}", exc_info=True)
            raise

    def find_user_by_id(self, user_id: str) -> Optional[User]:
        """Finds a user by their internal MongoDB ObjectId."""
        logger.debug(f"Finding user by internal ID: {user_id}")
        try:
            obj_id = PyObjectId(user_id) # Validate and convert string ID
            user_data = self.collection.find_one({"_id": obj_id})
            if user_data:
                return User.model_validate(user_data) # Use model_validate
            return None
        except ValueError: # Catch invalid ObjectId format from PyObjectId
            logger.warning(f"Invalid ObjectId format provided: {user_id}")
            return None
        except OperationFailure as e:
            logger.error(f"Database operation failed finding user by id {user_id}: {e}", exc_info=True)
            raise
        except Exception as e:
            logger.error(f"Unexpected error finding user by id {user_id}: {e}", exc_info=True)
            raise

    def upsert_user(self, user_data: Dict[str, Any]) -> Optional[User]:
        """Creates a new user or updates an existing one based on GitHub ID."""
        github_id = user_data.get("github_id")
        if not github_id:
            logger.error("Cannot upsert user without github_id.")
            return None

        logger.info(f"Upserting user with GitHub ID: {github_id}")
        try:
            # Prepare update data, excluding None values potentially passed in
            update_data = {k: v for k, v in user_data.items() if v is not None}
            update_data["updated_at"] = datetime.now(timezone.utc) # Update timestamp

            result = self.collection.update_one(
                {"github_id": github_id},
                {
                    "$set": update_data,
                    "$setOnInsert": {"created_at": datetime.now(timezone.utc)} # Use timezone here too
                },
                upsert=True
            )

            if result.upserted_id or result.matched_count > 0:
                # Fetch the updated/inserted user data to return the full User object
                upserted_user_data = self.collection.find_one({"github_id": github_id})
                if upserted_user_data:
                    logger.info(f"User {github_id} upserted successfully. Upserted ID: {result.upserted_id}, Matched: {result.matched_count}")
                    return User.model_validate(upserted_user_data) # Use model_validate
                else:
                    # This case should ideally not happen if upsert succeeded
                    logger.error(f"Failed to fetch user {github_id} immediately after successful upsert.")
                    return None
            else:
                # Should not happen with upsert=True unless there's a major issue
                logger.error(f"Upsert operation for user {github_id} reported no changes (upserted_id={result.upserted_id}, matched_count={result.matched_count}).")
                return None
        except OperationFailure as e:
            logger.error(f"Database operation failed upserting user {github_id}: {e}", exc_info=True)
            raise
        except Exception as e:
            logger.error(f"Unexpected error upserting user {github_id}: {e}", exc_info=True)
            raise

    def get_user_token_by_id(self, user_id: PyObjectId) -> Optional[str]:
        """Retrieves the GitHub access token for a user by their internal ID."""
        logger.debug(f"Fetching GitHub token for user ID: {user_id}")
        try:
            user_data = self.collection.find_one({"_id": user_id}, {"github_access_token": 1})
            if user_data:
                return user_data.get("github_access_token")
            return None
        except OperationFailure as e:
            logger.error(f"Database operation failed fetching token for user {user_id}: {e}", exc_info=True)
            raise
        except Exception as e:
            logger.error(f"Unexpected error fetching token for user {user_id}: {e}", exc_info=True)
            raise

# --- FastAPI Dependency ---
def get_user_repository(db: Database = Depends(get_database)) -> UserRepository:
    """FastAPI dependency to get an instance of UserRepository."""
    return UserRepository(db=db)
