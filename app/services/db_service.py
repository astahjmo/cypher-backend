from pymongo import MongoClient
from pymongo.database import Database
from pymongo.collection import Collection
from config import settings
import logging
from typing import cast # Import cast for type hinting refinement

logger = logging.getLogger(__name__)

class DatabaseService:
    client: MongoClient | None = None
    db: Database | None = None

    def connect(self):
        """Establishes connection to the MongoDB database."""
        if self.client is None:
            try:
                logger.info(f"Connecting to MongoDB at {settings.MONGO_URI}...")
                self.client = MongoClient(settings.MONGO_URI)
                # Assert client is not None for type checker after assignment
                assert self.client is not None, "MongoClient initialization failed unexpectedly."
                # The ismaster command is cheap and does not require auth.
                self.client.admin.command('ismaster')
                # Assert client is not None again before subscripting
                assert self.client is not None, "MongoClient became None unexpectedly."
                self.db = self.client[settings.MONGO_DB_NAME]
                logger.info(f"Successfully connected to MongoDB database: {settings.MONGO_DB_NAME}")
            except Exception as e:
                logger.error(f"Failed to connect to MongoDB: {e}")
                self.client = None
                self.db = None
                # Depending on the application design, you might want to raise the exception
                # raise

    def disconnect(self):
        """Closes the MongoDB connection."""
        if self.client:
            logger.info("Disconnecting from MongoDB...")
            self.client.close()
            self.client = None
            self.db = None
            logger.info("MongoDB connection closed.")

    def get_db(self) -> Database:
        """Returns the database instance, connecting if necessary."""
        if self.db is None:
            self.connect()
        # Assert db is not None after potentially connecting
        assert self.db is not None, "Database connection could not be established."
        # Use cast if assertion isn't enough for the type checker context
        # return cast(Database, self.db)
        return self.db


    def get_collection(self, collection_name: str) -> Collection:
        """Returns a collection instance from the database."""
        db = self.get_db()
        # db is already asserted to be Database in get_db()
        return db[collection_name]

# Create a single instance of the service to be imported elsewhere
db_service = DatabaseService()

# Functions to be used as FastAPI dependencies or lifespan events
async def get_database():
    """FastAPI dependency to get the database instance."""
    return db_service.get_db()

async def connect_to_mongo():
    """Connect to MongoDB on app startup."""
    db_service.connect()

async def close_mongo_connection():
    """Disconnect from MongoDB on app shutdown."""
    db_service.disconnect()
