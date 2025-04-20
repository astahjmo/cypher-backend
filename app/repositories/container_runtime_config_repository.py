from pymongo import MongoClient # Use pymongo MongoClient
from pymongo.database import Database, Collection # Use pymongo types
from pymongo.results import UpdateResult, DeleteResult # Import result types
# Corrected import paths assuming execution from parent directory or app in PYTHONPATH
# Use the updated models with PyObjectId and LabelPair
from models import ContainerRuntimeConfig, PyObjectId, LabelPair
from config import settings
from typing import Optional, List
from bson import ObjectId
from datetime import datetime, timezone # Import timezone

class ContainerRuntimeConfigRepository:
    """Repository for managing container runtime configurations in MongoDB using pymongo."""

    _collection: Collection # Use pymongo Collection type

    def __init__(self, db: Database): # Expect pymongo Database object
        """Initializes the repository with the database instance."""
        self._collection = db["container_runtime_configs"]

    def get_by_repo_and_user(self, repo_full_name: str, user_id: PyObjectId) -> Optional[ContainerRuntimeConfig]:
        """Finds a runtime configuration by repository full name and user ID (synchronous)."""
        # user_id should already be PyObjectId due to API validation, but handle string just in case
        if isinstance(user_id, str):
            query_user_id = ObjectId(user_id)
        elif isinstance(user_id, ObjectId): # Check for ObjectId directly
            query_user_id = user_id
        else:
            # This path might not be reachable if API validation is strict
            raise TypeError("user_id must be ObjectId or a valid string representation")

        config_doc = self._collection.find_one({"repo_full_name": repo_full_name, "user_id": query_user_id})
        if config_doc:
            # Pydantic should handle the deserialization including the new 'labels' field
            return ContainerRuntimeConfig(**config_doc)
        return None

    def upsert(self, config: ContainerRuntimeConfig) -> ContainerRuntimeConfig:
        """Creates or updates a container runtime configuration (synchronous)."""
        # Ensure user_id is set and is ObjectId before saving
        if config.user_id is None:
            raise ValueError("user_id cannot be None for upsert operation")

        if isinstance(config.user_id, str):
             query_user_id = ObjectId(config.user_id)
        elif isinstance(config.user_id, ObjectId):
             query_user_id = config.user_id
        else:
             raise TypeError("user_id must be ObjectId or a valid string representation")

        # Prepare the document for MongoDB, converting Pydantic models to dicts
        # Use model_dump for Pydantic v2
        config_dict_for_set = config.model_dump(by_alias=True, exclude={"id", "created_at"})

        # Ensure user_id in the dict is ObjectId type
        config_dict_for_set['user_id'] = query_user_id

        # Explicitly set updated_at for the $set operation using timezone-aware UTC now
        config_dict_for_set['updated_at'] = datetime.now(timezone.utc)

        # Use update_one with upsert=True
        # The $set operation will automatically include the 'labels' field if present in config_dict_for_set
        result: UpdateResult = self._collection.update_one(
            {"repo_full_name": config.repo_full_name, "user_id": query_user_id},
            {
                "$set": config_dict_for_set,
                # Set created_at only on insert using timezone-aware UTC now
                "$setOnInsert": {"created_at": datetime.now(timezone.utc)}
            },
            upsert=True
        )

        # Retrieve the document after upsert to return the full object with _id
        updated_doc = self._collection.find_one(
             {"repo_full_name": config.repo_full_name, "user_id": query_user_id}
        )

        if updated_doc:
            # Pydantic handles deserialization, including 'labels'
            return ContainerRuntimeConfig(**updated_doc)
        else:
            # This should ideally not happen after a successful upsert
            raise Exception("Failed to retrieve document after upsert operation")


    def delete_by_repo_and_user(self, repo_full_name: str, user_id: PyObjectId) -> bool:
        """Deletes a runtime configuration by repository full name and user ID (synchronous)."""
        if isinstance(user_id, str):
            query_user_id = ObjectId(user_id)
        elif isinstance(user_id, ObjectId):
            query_user_id = user_id
        else:
            raise TypeError("user_id must be ObjectId or a valid string representation")

        result: DeleteResult = self._collection.delete_one({"repo_full_name": repo_full_name, "user_id": query_user_id})
        return result.deleted_count > 0

    # Potentially add other methods like get_by_id if needed
