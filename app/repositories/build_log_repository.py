import logging
from pymongo.database import Database
from pymongo import DESCENDING
from datetime import datetime, timezone # Import timezone
from bson import ObjectId
from typing import List, Optional, Dict, Any # Added Dict, Any
from fastapi import Depends # Import Depends

# Import models and db service
from models import BuildLog, PyObjectId, now_utc # Import now_utc helper
from services.db_service import get_database # Import get_database

logger = logging.getLogger(__name__)

class BuildLogRepository:
    def __init__(self, db: Database):
        self.collection = db["build_logs"]
        # Consider creating an index on build_id and timestamp for faster log retrieval
        # self.collection.create_index([("build_id", 1), ("timestamp", 1)])

    def add_log(self, build_id: PyObjectId, message: str, log_type: str = "log", timestamp: Optional[datetime] = None):
        """Adds a new single log entry for a build."""
        # Use timezone-aware UTC now if timestamp not provided
        if timestamp is None:
            timestamp = now_utc()
        # Ensure provided timestamp is timezone-aware (assume UTC if naive, though ideally it should be aware)
        elif timestamp.tzinfo is None:
             timestamp = timestamp.replace(tzinfo=timezone.utc)


        log_doc = {
            "build_id": build_id,
            "timestamp": timestamp,
            "type": log_type,
            "message": message
        }
        try:
            self.collection.insert_one(log_doc)
            # logger.debug(f"Added log for build {build_id}: {message[:50]}...") # Optional: Debug log
        except Exception as e:
            logger.error(f"Failed to add log for build {build_id}: {e}", exc_info=True)

    def add_many_logs(self, log_documents: List[Dict[str, Any]]):
        """Adds multiple log entries for a build using insert_many."""
        if not log_documents:
            logger.warning("Attempted to add an empty list of log documents.")
            return 0

        build_id = log_documents[0].get("build_id", "UNKNOWN") # Get build_id for logging

        # Ensure all documents have a timezone-aware UTC timestamp
        current_time_utc = now_utc()
        for doc in log_documents:
            if "timestamp" not in doc or doc["timestamp"] is None:
                doc["timestamp"] = current_time_utc
            elif isinstance(doc["timestamp"], datetime) and doc["timestamp"].tzinfo is None:
                 doc["timestamp"] = doc["timestamp"].replace(tzinfo=timezone.utc)
            # If timestamp is already aware, keep it

        try:
            result = self.collection.insert_many(log_documents, ordered=False) # ordered=False might improve performance slightly
            inserted_count = len(result.inserted_ids)
            logger.info(f"Added {inserted_count} log entries for build {build_id} via insert_many.")
            return inserted_count
        except Exception as e:
            logger.error(f"Failed to add logs in bulk for build {build_id}: {e}", exc_info=True)
            return 0


    def get_logs_by_build(self, build_id: PyObjectId, limit: int = 1000) -> List[BuildLog]:
        """Retrieves log entries for a specific build, sorted by timestamp."""
        query = {"build_id": build_id}
        # Sort by timestamp ascending
        log_cursor = self.collection.find(query).sort("_id", 1)
        if limit > 0:
            log_cursor = log_cursor.limit(limit)

        logs = [BuildLog(**doc) for doc in log_cursor]
        return logs

    def delete_logs_by_build(self, build_id: PyObjectId) -> int:
        """Deletes all log entries for a specific build."""
        result = self.collection.delete_many({"build_id": build_id})
        deleted_count = result.deleted_count
        if deleted_count > 0:
            logger.info(f"Deleted {deleted_count} log entries for build {build_id}")
        return deleted_count

# --- Dependency Function ---
# Ensure this function is correctly defined and present
def get_build_log_repository(db: Database = Depends(get_database)) -> BuildLogRepository:
    """Dependency function to provide a BuildLogRepository instance."""
    return BuildLogRepository(db)
