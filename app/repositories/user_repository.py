from pymongo.database import Database
from pymongo.errors import ConnectionFailure, OperationFailure
from bson import ObjectId
from typing import Optional, Dict, Any
import logging
from datetime import datetime, timezone

from models.auth.db_models import User
from models.base import PyObjectId
from services.db_service import get_database
from fastapi import Depends

logger = logging.getLogger(__name__)


class UserRepository:
    """Handles database operations for User objects.

    Provides methods to find, upsert, and retrieve specific user data
    from the MongoDB 'users' collection.

    Attributes:
        collection: The MongoDB collection instance for 'users'.
    """

    def __init__(self, db: Database):
        """Initializes the repository with the database connection.

        Args:
            db (Database): The pymongo Database instance.
        """
        self.collection = db["users"]
        logger.info("UserRepository initialized with database connection.")

    def find_user_by_github_id(self, github_id: int) -> Optional[User]:
        """Finds a user by their unique GitHub ID.

        Args:
            github_id (int): The GitHub ID of the user to find.

        Returns:
            Optional[User]: The found User instance, or None if not found.

        Raises:
            OperationFailure: If a database operation fails during the find query.
            Exception: For any other unexpected errors.
        """
        logger.debug(f"Finding user by GitHub ID: {github_id}")
        try:
            user_data = self.collection.find_one({"github_id": github_id})
            if user_data:
                return User.model_validate(user_data)
            return None
        except OperationFailure as e:
            logger.error(
                f"Database operation failed finding user by github_id {github_id}: {e}",
                exc_info=True,
            )
            raise
        except Exception as e:
            logger.error(
                f"Unexpected error finding user by github_id {github_id}: {e}",
                exc_info=True,
            )
            raise

    def find_user_by_id(self, user_id: str) -> Optional[User]:
        """Finds a user by their internal MongoDB ObjectId string.

        Args:
            user_id (str): The string representation of the user's MongoDB ObjectId.

        Returns:
            Optional[User]: The found User instance, or None if not found or if the
                            user_id format is invalid.

        Raises:
            OperationFailure: If a database operation fails during the find query.
            Exception: For any other unexpected errors.
        """
        logger.debug(f"Finding user by internal ID: {user_id}")
        try:
            obj_id = PyObjectId(user_id)
            user_data = self.collection.find_one({"_id": obj_id})
            if user_data:
                return User.model_validate(user_data)
            return None
        except ValueError:
            logger.warning(f"Invalid ObjectId format provided: {user_id}")
            return None
        except OperationFailure as e:
            logger.error(
                f"Database operation failed finding user by id {user_id}: {e}",
                exc_info=True,
            )
            raise
        except Exception as e:
            logger.error(
                f"Unexpected error finding user by id {user_id}: {e}", exc_info=True
            )
            raise

    def upsert_user(self, user_data: Dict[str, Any]) -> Optional[User]:
        """Creates a new user or updates an existing one based on GitHub ID.

        Filters out None values from the input data before setting fields.
        Sets `updated_at` timestamp on every upsert. Sets `created_at` only on insert.

        Args:
            user_data (Dict[str, Any]): A dictionary containing user data, must include 'github_id'.

        Returns:
            Optional[User]: The created or updated User instance, or None if the upsert fails
                            or the user data is missing 'github_id'.

        Raises:
            OperationFailure: If the database upsert operation fails.
            Exception: For any other unexpected errors.
        """
        github_id = user_data.get("github_id")
        if not github_id:
            logger.error("Cannot upsert user without github_id.")
            return None

        logger.info(f"Upserting user with GitHub ID: {github_id}")
        try:
            update_data = {k: v for k, v in user_data.items() if v is not None}
            update_data["updated_at"] = datetime.now(timezone.utc)

            result = self.collection.update_one(
                {"github_id": github_id},
                {
                    "$set": update_data,
                    "$setOnInsert": {"created_at": datetime.now(timezone.utc)},
                },
                upsert=True,
            )

            if result.upserted_id or result.matched_count > 0:
                upserted_user_data = self.collection.find_one({"github_id": github_id})
                if upserted_user_data:
                    logger.info(
                        f"User {github_id} upserted successfully. Upserted ID: {result.upserted_id}, Matched: {result.matched_count}"
                    )
                    return User.model_validate(upserted_user_data)
                else:
                    logger.error(
                        f"Failed to fetch user {github_id} immediately after successful upsert."
                    )
                    return None
            else:
                logger.error(
                    f"Upsert operation for user {github_id} reported no changes (upserted_id={result.upserted_id}, matched_count={result.matched_count})."
                )
                return None
        except OperationFailure as e:
            logger.error(
                f"Database operation failed upserting user {github_id}: {e}",
                exc_info=True,
            )
            raise
        except Exception as e:
            logger.error(
                f"Unexpected error upserting user {github_id}: {e}", exc_info=True
            )
            raise

    def get_user_token_by_id(self, user_id: PyObjectId) -> Optional[str]:
        """Retrieves the GitHub access token for a user by their internal MongoDB ObjectId.

        Args:
            user_id (PyObjectId): The ObjectId of the user whose token to retrieve.

        Returns:
            Optional[str]: The user's GitHub access token if found, otherwise None.

        Raises:
            OperationFailure: If a database operation fails during the find query.
            Exception: For any other unexpected errors.
        """
        logger.debug(f"Fetching GitHub token for user ID: {user_id}")
        try:
            # Query only for the token field for efficiency
            user_data = self.collection.find_one(
                {"_id": user_id}, {"github_access_token": 1}
            )
            if user_data:
                return user_data.get("github_access_token")
            return None
        except OperationFailure as e:
            logger.error(
                f"Database operation failed fetching token for user {user_id}: {e}",
                exc_info=True,
            )
            raise
        except Exception as e:
            logger.error(
                f"Unexpected error fetching token for user {user_id}: {e}",
                exc_info=True,
            )
            raise


def get_user_repository(db: Database = Depends(get_database)) -> UserRepository:
    """FastAPI dependency function to get an instance of UserRepository.

    Injects the database connection provided by the `get_database` dependency.

    Args:
        db (Database): The database instance obtained from `get_database`.

    Returns:
        UserRepository: An instance of the UserRepository.
    """
    return UserRepository(db=db)
