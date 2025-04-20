import logging
from pymongo.database import Database
from pymongo import DESCENDING
from datetime import datetime
from bson import ObjectId
from bson.errors import InvalidId
from fastapi import Depends # Import Depends

# Import models and db service
from models import User
from services.db_service import get_database # Import get_database

logger = logging.getLogger(__name__)

class UserRepository:
    def __init__(self, db: Database):
        # This now expects an actual Database instance
        self.collection = db["users"]

    def find_user_by_id(self, user_id_str: str) -> User | None:
        """
        Finds a user by their MongoDB ObjectId string.
        Returns a User model instance or None if not found or ID is invalid.
        """
        try:
            user_oid = ObjectId(user_id_str)
        except InvalidId:
            logger.warning(f"Invalid ObjectId format provided: {user_id_str}")
            return None
        except Exception as e:
            logger.error(f"Error converting string to ObjectId '{user_id_str}': {e}", exc_info=True)
            return None

        try:
            user_doc = self.collection.find_one({"_id": user_oid})
            if user_doc:
                if not isinstance(user_doc.get("_id"), ObjectId):
                     logger.error(f"User {user_doc.get('login')} found by ID {user_oid} but has non-ObjectId _id: {user_doc.get('_id')}")
                     return None
                logger.debug(f"Found user by ID: {user_oid}")
                return User(**user_doc)
            else:
                logger.debug(f"User not found for ID: {user_oid}")
                return None
        except Exception as e:
            logger.error(f"Database error finding user by ID {user_oid}: {e}", exc_info=True)
            return None

    def find_or_create_user(self, github_id: int, user_data: dict) -> User:
        """
        Finds a user by GitHub ID or creates/updates one.
        The GitHub access token is ONLY saved when the user is first created.
        """
        now = datetime.utcnow()
        existing_user_doc = self.collection.find_one({"github_id": github_id})

        if existing_user_doc and isinstance(existing_user_doc.get("_id"), str) and existing_user_doc.get("_id", "").startswith("user_"):
            logger.warning(f"Found existing user {existing_user_doc.get('login')} with old string ID {existing_user_doc['_id']}. Deleting and recreating with ObjectId.")
            try:
                self.collection.delete_one({"_id": existing_user_doc["_id"]})
            except Exception as e:
                 logger.error(f"Failed to delete user with old string ID {existing_user_doc['_id']}: {e}", exc_info=True)
            existing_user_doc = None

        update_fields = {
            "login": user_data.get('login'),
            "name": user_data.get('name'),
            "email": user_data.get('email'),
            "avatar_url": user_data.get('avatar_url'),
            "updated_at": now
        }
        update_fields = {k: v for k, v in update_fields.items() if v is not None}

        if existing_user_doc:
            logger.info(f"Found existing user {existing_user_doc.get('login')}. Updating details (token not updated).")
            self.collection.update_one(
                {"_id": existing_user_doc["_id"]},
                {"$set": update_fields}
            )
            updated_doc_from_db = self.collection.find_one({"_id": existing_user_doc["_id"]})
            if not updated_doc_from_db:
                 logger.error(f"Failed to retrieve updated user document after update for ID {existing_user_doc['_id']}")
                 raise Exception(f"Failed to retrieve updated user document with ID {existing_user_doc['_id']}")

            logger.info(f"Updated existing user record. User ID: {updated_doc_from_db['_id']}")
            if isinstance(updated_doc_from_db.get("_id"), str):
                 logger.error(f"User {updated_doc_from_db.get('login')} still has string ID after update.")
                 raise TypeError(f"User ID {updated_doc_from_db.get('_id')} is not an ObjectId after update.")
            return User(**updated_doc_from_db)
        else:
            logger.info(f"Creating new user for GitHub ID {github_id}.")
            new_user_data = {
                "_id": ObjectId(),
                "github_id": github_id,
                "created_at": now,
                "github_access_token": user_data.get('access_token'),
                **update_fields
            }
            new_user_data.setdefault('login', user_data.get('login'))

            insert_result = self.collection.insert_one(new_user_data)
            logger.info(f"Created new user record. User ID: {insert_result.inserted_id}")
            created_doc = self.collection.find_one({"_id": insert_result.inserted_id})
            if created_doc:
                if not isinstance(created_doc.get("_id"), ObjectId):
                     logger.error(f"Newly created user {created_doc.get('login')} does not have ObjectId. ID: {created_doc.get('_id')}")
                     raise TypeError(f"User ID {created_doc.get('_id')} is not an ObjectId after creation.")
                return User(**created_doc)
            else:
                logger.error(f"Failed to retrieve newly created user document after insert for ID {insert_result.inserted_id}")
                raise Exception("Failed to retrieve newly created user document.")

    def get_user_token_by_id(self, user_id: ObjectId) -> str | None:
        """Retrieves the GitHub access token for a specific user by their ObjectId."""
        if not isinstance(user_id, ObjectId):
            logger.error(f"Invalid user_id type provided to get_user_token_by_id: {type(user_id)}. Expected ObjectId.")
            return None

        try:
            user_doc = self.collection.find_one(
                {"_id": user_id},
                projection={"github_access_token": 1}
            )
            if user_doc:
                token = user_doc.get("github_access_token")
                if token:
                    logger.debug(f"Retrieved token for user ID {user_id}.")
                    return token
                else:
                    logger.warning(f"User with ID {user_id} found but has no access token stored.")
                    return None
            else:
                logger.warning(f"No user found with ID {user_id}.")
                return None
        except Exception as e:
            logger.error(f"Database error fetching token for user ID {user_id}: {e}", exc_info=True)
            return None

# --- Dependency Function ---
# Add a function that FastAPI can use to inject a UserRepository instance
def get_user_repository(db: Database = Depends(get_database)) -> UserRepository:
    """Dependency function to provide a UserRepository instance."""
    return UserRepository(db)
