from pymongo.database import Database
from pymongo.errors import ConnectionFailure, OperationFailure
from pymongo import ASCENDING
from bson import ObjectId
from typing import List, Optional
import logging

from models.build.db_models import BuildLog
from models.base import PyObjectId
from services.db_service import get_database
from fastapi import Depends

logger = logging.getLogger(__name__)


class BuildLogRepository:
    """Handles database operations for BuildLog objects.

    Provides methods to insert and retrieve build log entries from the MongoDB collection.

    Attributes:
        collection: The MongoDB collection instance for 'build_logs'.
    """

    def __init__(self, db: Database):
        """Initializes the repository with the database connection.

        Args:
            db (Database): The pymongo Database instance.
        """
        self.collection = db["build_logs"]
        logger.info("BuildLogRepository initialized.")

    def insert_many(self, logs: List[BuildLog]) -> List[PyObjectId]:
        """Inserts multiple build log entries into the database.

        Args:
            logs (List[BuildLog]): A list of BuildLog model instances to insert.

        Returns:
            List[PyObjectId]: A list of the ObjectIds of the inserted documents.

        Raises:
            OperationFailure: If a database operation fails during insertion.
            Exception: For any other unexpected errors.
        """
        if not logs:
            return []
        logger.debug(
            f"Inserting {len(logs)} log entries for build ID: {logs[0].build_id}"
        )
        try:
            log_data = [log.model_dump(by_alias=True, exclude={"id"}) for log in logs]
            result = self.collection.insert_many(log_data)
            return result.inserted_ids
        except OperationFailure as e:
            logger.error(
                f"Database operation failed inserting logs for build {logs[0].build_id}: {e}",
                exc_info=True,
            )
            raise
        except Exception as e:
            logger.error(
                f"Unexpected error inserting logs for build {logs[0].build_id}: {e}",
                exc_info=True,
            )
            raise

    def find_by_build_id(self, build_id: str) -> List[BuildLog]:
        """Finds all log entries for a specific build ID, sorted by timestamp (insertion order).

        Args:
            build_id (str): The string representation of the build's ObjectId.

        Returns:
            List[BuildLog]: A list of BuildLog model instances found, sorted by insertion time.
                            Returns an empty list if the build_id format is invalid or no logs are found.

        Raises:
            OperationFailure: If a database operation fails during the find query.
            Exception: For any other unexpected errors.
        """
        logger.debug(f"Finding logs for build ID: {build_id}")
        try:
            # Convert string ID to ObjectId for the query
            obj_build_id = PyObjectId(build_id)
            logs_cursor = self.collection.find({"build_id": obj_build_id}).sort(
                "_id", ASCENDING
            )
            return [BuildLog.model_validate(log) for log in logs_cursor]
        except ValueError:
            logger.warning(
                f"Invalid ObjectId format provided for find_by_build_id: {build_id}"
            )
            return []
        except OperationFailure as e:
            logger.error(
                f"Database operation failed finding logs for build {build_id}: {e}",
                exc_info=True,
            )
            raise
        except Exception as e:
            logger.error(
                f"Unexpected error finding logs for build {build_id}: {e}",
                exc_info=True,
            )
            raise


def get_build_log_repository(
    db: Database = Depends(get_database),
) -> BuildLogRepository:
    """FastAPI dependency function to get an instance of BuildLogRepository.

    Injects the database connection provided by the `get_database` dependency.

    Args:
        db (Database): The database instance obtained from `get_database`.

    Returns:
        BuildLogRepository: An instance of the BuildLogRepository.
    """
    return BuildLogRepository(db=db)
