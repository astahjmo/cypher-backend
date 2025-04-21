from pymongo import MongoClient
from pymongo.database import Database
from pymongo.collection import Collection
from config import settings
import logging
from typing import cast

logger = logging.getLogger(__name__)


class DatabaseService:
    """Manages the MongoDB connection and provides access to the database and collections.

    Uses a singleton pattern approach by creating a single instance `db_service`
    that can be imported and used throughout the application.

    Attributes:
        client (MongoClient | None): The MongoClient instance, or None if not connected.
        db (Database | None): The Database instance, or None if not connected.
    """

    client: MongoClient | None = None
    db: Database | None = None

    def connect(self):
        """Establishes a connection to the MongoDB database using settings from config.

        Sets the `client` and `db` attributes upon successful connection.
        Logs errors if the connection fails.
        """
        if self.client is None:
            try:
                logger.info(f"Connecting to MongoDB at {settings.MONGO_URI}...")
                self.client = MongoClient(settings.MONGO_URI)
                assert (
                    self.client is not None
                ), "MongoClient initialization failed unexpectedly."
                # The ismaster command is cheap and does not require auth.
                self.client.admin.command("ismaster")
                assert self.client is not None, "MongoClient became None unexpectedly."
                self.db = self.client[settings.MONGO_DB_NAME]
                logger.info(
                    f"Successfully connected to MongoDB database: {settings.MONGO_DB_NAME}"
                )
            except Exception as e:
                logger.error(f"Failed to connect to MongoDB: {e}")
                self.client = None
                self.db = None

    def disconnect(self):
        """Closes the MongoDB connection if it's currently open."""
        if self.client:
            logger.info("Disconnecting from MongoDB...")
            self.client.close()
            self.client = None
            self.db = None
            logger.info("MongoDB connection closed.")

    def get_db(self) -> Database:
        """Returns the database instance, attempting to connect if not already connected.

        Returns:
            Database: The pymongo Database instance.

        Raises:
            AssertionError: If the database connection could not be established after attempting to connect.
        """
        if self.db is None:
            self.connect()
        assert self.db is not None, "Database connection could not be established."
        return self.db

    def get_collection(self, collection_name: str) -> Collection:
        """Returns a collection instance from the connected database.

        Args:
            collection_name (str): The name of the collection to retrieve.

        Returns:
            Collection: The pymongo Collection instance.
        """
        db = self.get_db()
        return db[collection_name]


# Singleton instance of the DatabaseService
db_service = DatabaseService()


async def get_database():
    """FastAPI dependency function to get the database instance via the db_service.

    Returns:
        Database: The pymongo Database instance.
    """
    return db_service.get_db()


async def connect_to_mongo():
    """Connects to MongoDB, intended for application startup (e.g., lifespan event)."""
    db_service.connect()


async def close_mongo_connection():
    """Disconnects from MongoDB, intended for application shutdown (e.g., lifespan event)."""
    db_service.disconnect()
