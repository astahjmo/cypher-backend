from pymongo.database import Database
from pymongo.errors import ConnectionFailure, OperationFailure
from pymongo import ReturnDocument, DESCENDING
from bson import ObjectId
from typing import List, Optional, Dict, Any
import logging
from datetime import datetime, timezone

from models.build.db_models import BuildStatus
from models.base import PyObjectId
from services.db_service import get_database
from fastapi import Depends

logger = logging.getLogger(__name__)


class BuildStatusRepository:
    """Handles database operations for BuildStatus objects.

    Provides methods to create, retrieve, and update build status records
    in the MongoDB collection.

    Attributes:
        collection: The MongoDB collection instance for 'build_statuses'.
    """

    def __init__(self, db: Database):
        """Initializes the repository with the database connection.

        Args:
            db (Database): The pymongo Database instance.
        """
        self.collection = db["build_statuses"]
        logger.info("BuildStatusRepository initialized.")

    def create(self, build_status: BuildStatus) -> BuildStatus:
        """Creates a new build status record in the database.

        Sets the created_at and updated_at timestamps before insertion.

        Args:
            build_status (BuildStatus): The BuildStatus model instance to create.

        Returns:
            BuildStatus: The created BuildStatus instance, including the generated _id.

        Raises:
            OperationFailure: If a database operation fails during insertion.
            Exception: For any other unexpected errors.
        """
        logger.info(
            f"Creating new build status for repo: {build_status.repo_full_name}, branch: {build_status.branch}"
        )
        try:
            now = datetime.now(timezone.utc)
            build_status.created_at = now
            build_status.updated_at = now
            insert_data = build_status.model_dump(by_alias=True, exclude={"id"})

            if not isinstance(insert_data.get("user_id"), str):
                logger.error(
                    f"CRITICAL: user_id is not a string before insert: {type(insert_data.get('user_id'))}"
                )

            result = self.collection.insert_one(insert_data)
            created_doc = self.collection.find_one({"_id": result.inserted_id})
            if created_doc:
                logger.info(
                    f"Successfully created and fetched build status with ID: {result.inserted_id}"
                )
                return BuildStatus.model_validate(created_doc)
            else:
                logger.error(
                    f"Failed to fetch build status immediately after insertion with ID: {result.inserted_id}"
                )
                raise OperationFailure(
                    "Failed to retrieve build status after creation."
                )
        except OperationFailure as e:
            logger.error(
                f"Database operation failed creating build status: {e}", exc_info=True
            )
            raise
        except Exception as e:
            logger.error(f"Unexpected error creating build status: {e}", exc_info=True)
            raise

    def get_by_id(self, build_id: str) -> Optional[BuildStatus]:
        """Finds a build status record by its MongoDB ObjectId string.

        Args:
            build_id (str): The string representation of the build's ObjectId.

        Returns:
            Optional[BuildStatus]: The found BuildStatus instance, or None if not found
                                   or if the build_id format is invalid.

        Raises:
            OperationFailure: If a database operation fails during the find query.
            Exception: For any other unexpected errors.
        """
        logger.debug(f"Finding build status by ID: {build_id}")
        try:
            obj_id = PyObjectId(build_id)
            build_data = self.collection.find_one({"_id": obj_id})
            if build_data:
                logger.debug(f"Found build status by ID {build_id}: {build_data}")
                return BuildStatus.model_validate(build_data)
            logger.debug(f"Build status not found for ID: {build_id}")
            return None
        except ValueError:
            logger.warning(
                f"Invalid ObjectId format provided for get_by_id: {build_id}"
            )
            return None
        except OperationFailure as e:
            logger.error(
                f"Database operation failed finding build status by ID {build_id}: {e}",
                exc_info=True,
            )
            raise
        except Exception as e:
            logger.error(
                f"Unexpected error finding build status by ID {build_id}: {e}",
                exc_info=True,
            )
            raise

    def get_by_id_and_user(self, build_id: str, user_id: Any) -> Optional[BuildStatus]:
        """Finds a build status by ID, ensuring it belongs to the specified user.

        Compares the `user_id` field in the database (which is stored as a string)
        with the string representation of the provided `user_id`.

        Args:
            build_id (str): The string representation of the build's ObjectId.
            user_id (Any): The ID of the user requesting the build status (can be str or ObjectId).

        Returns:
            Optional[BuildStatus]: The found BuildStatus instance if it exists and belongs
                                   to the user, otherwise None. Returns None if the build_id
                                   format is invalid.

        Raises:
            OperationFailure: If a database operation fails during the find query.
            Exception: For any other unexpected errors.
        """
        user_id_str = str(user_id)
        logger.info(
            f"Attempting to find build status by ID: {build_id} for user ID (as string): {user_id_str}"
        )
        try:
            obj_id = PyObjectId(build_id)
            logger.debug(f"Converted build_id to ObjectId: {obj_id}")

            build_data = self.collection.find_one(
                {"_id": obj_id, "user_id": user_id_str}
            )

            if build_data:
                logger.info(
                    f"Successfully found build {build_id} belonging to user {user_id_str}."
                )
                return BuildStatus.model_validate(build_data)
            else:
                build_exists = self.collection.find_one({"_id": obj_id})
                if build_exists:
                    found_user_id = build_exists.get("user_id")
                    logger.warning(
                        f"Build {build_id} found, but its user_id '{found_user_id}' (type: {type(found_user_id)}) does not match requesting user_id '{user_id_str}'."
                    )
                else:
                    logger.warning(
                        f"Build with ID {build_id} (ObjectId: {obj_id}) does not exist in the database."
                    )
                return None
        except ValueError:
            logger.warning(
                f"Invalid ObjectId format provided for get_by_id_and_user build_id: {build_id}"
            )
            return None
        except OperationFailure as e:
            logger.error(
                f"Database operation failed finding build status {build_id} for user {user_id_str}: {e}",
                exc_info=True,
            )
            raise
        except Exception as e:
            logger.error(
                f"Unexpected error finding build status {build_id} for user {user_id_str}: {e}",
                exc_info=True,
            )
            raise

    def update_status(
        self,
        build_id: str,
        status: str,
        message: Optional[str] = None,
        image_tag: Optional[str] = None,
        commit_sha: Optional[str] = None,
        commit_message: Optional[str] = None,
    ) -> Optional[BuildStatus]:
        """Updates the status and other optional fields of a build status record.

        Automatically sets `started_at` when status becomes 'running' and
        `completed_at` when status becomes 'success', 'failed', or 'cancelled',
        if not already set. Updates `updated_at` timestamp.

        Args:
            build_id (str): The string representation of the build's ObjectId.
            status (str): The new status value (e.g., 'running', 'success', 'failed').
            message (Optional[str]): An optional message (e.g., error details).
            image_tag (Optional[str]): The Docker image tag (usually set on success).
            commit_sha (Optional[str]): The Git commit SHA (usually set when starting).
            commit_message (Optional[str]): The Git commit message (usually set when starting).

        Returns:
            Optional[BuildStatus]: The updated BuildStatus instance if found, otherwise None.
                                   Returns None if the build_id format is invalid.

        Raises:
            OperationFailure: If a database operation fails during the update.
            Exception: For any other unexpected errors.
        """
        logger.info(f"Updating build status for ID: {build_id} to status: {status}")
        try:
            obj_id = PyObjectId(build_id)
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
            if (
                status in ["success", "failed", "cancelled"]
                and "completed_at" not in update_fields
            ):
                update_fields["completed_at"] = now

            updated_doc = self.collection.find_one_and_update(
                {"_id": obj_id},
                {"$set": update_fields},
                return_document=ReturnDocument.AFTER,
            )
            if updated_doc:
                logger.info(f"Successfully updated build status for ID: {build_id}")
                return BuildStatus.model_validate(updated_doc)
            else:
                logger.warning(
                    f"Build status update failed: Build ID {build_id} not found."
                )
                return None
        except ValueError:
            logger.warning(
                f"Invalid ObjectId format provided for update_status: {build_id}"
            )
            return None
        except OperationFailure as e:
            logger.error(
                f"Database operation failed updating build status {build_id}: {e}",
                exc_info=True,
            )
            raise
        except Exception as e:
            logger.error(
                f"Unexpected error updating build status {build_id}: {e}", exc_info=True
            )
            raise

    def find_by_user(self, user_id: Any, limit: int = 20) -> List[BuildStatus]:
        """Finds build status records for a specific user, sorted by creation date descending.

        Compares the `user_id` field (string) with the string representation of the provided `user_id`.

        Args:
            user_id (Any): The ID of the user whose builds to find (can be str or ObjectId).
            limit (int): The maximum number of records to return (0 for no limit). Defaults to 20.

        Returns:
            List[BuildStatus]: A list of BuildStatus instances found for the user.

        Raises:
            OperationFailure: If a database operation fails during the find query.
            Exception: For any other unexpected errors.
        """
        user_id_str = str(user_id)
        logger.debug(
            f"Finding build statuses for user ID (as string): {user_id_str}, limit: {limit}"
        )
        try:
            query = self.collection.find({"user_id": user_id_str}).sort(
                "created_at", DESCENDING
            )
            if limit > 0:
                query = query.limit(limit)
            results = [BuildStatus.model_validate(doc) for doc in query]
            logger.debug(f"Found {len(results)} build statuses for user {user_id_str}")
            return results
        except OperationFailure as e:
            logger.error(
                f"Database operation failed finding builds for user {user_id_str}: {e}",
                exc_info=True,
            )
            raise
        except Exception as e:
            logger.error(
                f"Unexpected error finding builds for user {user_id_str}: {e}",
                exc_info=True,
            )
            raise

    def find_latest_successful_build(
        self, repo_full_name: str, user_id: Any
    ) -> Optional[BuildStatus]:
        """Finds the most recent successful build for a specific repository and user.

        Looks for builds with status 'success' and a non-null image_tag, sorted by completion date descending.
        Compares the `user_id` field (string) with the string representation of the provided `user_id`.

        Args:
            repo_full_name (str): The full name of the repository (e.g., 'owner/repo').
            user_id (Any): The ID of the user (can be str or ObjectId).

        Returns:
            Optional[BuildStatus]: The most recent successful BuildStatus instance if found, otherwise None.

        Raises:
            OperationFailure: If a database operation fails during the find query.
            Exception: For any other unexpected errors.
        """
        user_id_str = str(user_id)
        logger.debug(
            f"Finding latest successful build for repo: {repo_full_name}, user ID (as string): {user_id_str}"
        )
        try:
            build_data = self.collection.find_one(
                {
                    "repo_full_name": repo_full_name,
                    "user_id": user_id_str,
                    "status": "success",
                    "image_tag": {"$ne": None},
                },
                sort=[("completed_at", DESCENDING)],
            )
            if build_data:
                logger.debug(f"Found latest successful build: {build_data['_id']}")
                return BuildStatus.model_validate(build_data)
            logger.debug(
                f"No successful build found for repo {repo_full_name}, user {user_id_str}"
            )
            return None
        except OperationFailure as e:
            logger.error(
                f"Database operation failed finding latest build for repo {repo_full_name}, user {user_id_str}: {e}",
                exc_info=True,
            )
            raise
        except Exception as e:
            logger.error(
                f"Unexpected error finding latest build for repo {repo_full_name}, user {user_id_str}: {e}",
                exc_info=True,
            )
            raise


def get_build_status_repository(
    db: Database = Depends(get_database),
) -> BuildStatusRepository:
    """FastAPI dependency function to get an instance of BuildStatusRepository.

    Injects the database connection provided by the `get_database` dependency.

    Args:
        db (Database): The database instance obtained from `get_database`.

    Returns:
        BuildStatusRepository: An instance of the BuildStatusRepository.
    """
    return BuildStatusRepository(db=db)
