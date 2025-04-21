from pymongo.database import Database
from pymongo.errors import ConnectionFailure, OperationFailure
from pymongo import ASCENDING
from bson import ObjectId
from typing import List, Optional
import logging

# Import BuildLog model from its new location
from models.build.db_models import BuildLog
# Import PyObjectId from base
from models.base import PyObjectId
# Import get_database dependency
from services.db_service import get_database
from fastapi import Depends # Keep Depends for dependency injection

logger = logging.getLogger(__name__)

class BuildLogRepository:
    """Handles database operations for BuildLog objects."""

    def __init__(self, db: Database):
        self.collection = db["build_logs"]
        logger.info("BuildLogRepository initialized.")

    def insert_many(self, logs: List[BuildLog]) -> List[PyObjectId]:
        """Inserts multiple build log entries."""
        if not logs:
            return []
        logger.debug(f"Inserting {len(logs)} log entries for build ID: {logs[0].build_id}")
        try:
            # Use model_dump for Pydantic v2, exclude 'id'
            log_data = [log.model_dump(by_alias=True, exclude={'id'}) for log in logs]
            result = self.collection.insert_many(log_data)
            return result.inserted_ids # Return list of inserted ObjectIds
        except OperationFailure as e:
            logger.error(f"Database operation failed inserting logs for build {logs[0].build_id}: {e}", exc_info=True)
            raise
        except Exception as e:
            logger.error(f"Unexpected error inserting logs for build {logs[0].build_id}: {e}", exc_info=True)
            raise

    def find_by_build_id(self, build_id: str) -> List[BuildLog]:
        """Finds all log entries for a specific build ID, sorted by timestamp."""
        logger.debug(f"Finding logs for build ID: {build_id}")
        try:
            logs_cursor = self.collection.find({"build_id": build_id}).sort("_id", ASCENDING)
            print(build_id)
            return [BuildLog.model_validate(log) for log in logs_cursor]
        except ValueError: # Handle invalid ObjectId format
            logger.warning(f"Invalid ObjectId format provided for find_by_build_id: {build_id}")
            return [] # Return empty list for invalid ID
        except OperationFailure as e:
            logger.error(f"Database operation failed finding logs for build {build_id}: {e}", exc_info=True)
            raise
        except Exception as e:
            logger.error(f"Unexpected error finding logs for build {build_id}: {e}", exc_info=True)
            raise

# --- FastAPI Dependency ---
def get_build_log_repository(db: Database = Depends(get_database)) -> BuildLogRepository:
    """FastAPI dependency to get an instance of BuildLogRepository."""
    return BuildLogRepository(db=db)
