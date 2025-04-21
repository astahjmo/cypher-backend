from pydantic import Field
from typing import List
from datetime import datetime

from ..base import PyObjectId, BaseModelWithDates, now_utc


class RepositoryConfig(BaseModelWithDates):
    """Configuration for automatic builds for a specific repository (Database Model).

    Stores which branches of a repository are configured for automatic builds
    triggered by webhooks.

    Attributes:
        id (PyObjectId): The unique configuration ID in the MongoDB database (alias for _id).
        user_id (PyObjectId): The internal user ID (ObjectId) this configuration belongs to.
        repo_full_name (str): The full repository name (e.g., 'owner/repo').
        auto_build_branches (List[str]): A list of branch names configured for automatic builds.
        created_at (datetime): Timestamp when the configuration record was created (UTC).
        updated_at (datetime): Timestamp of the last update to this configuration record (UTC).
    """

    id: PyObjectId = Field(default_factory=PyObjectId, alias="_id")
    user_id: PyObjectId = Field(
        ..., description="Internal user ID this config belongs to"
    )
    repo_full_name: str = Field(
        ..., description="Full repository name (e.g., 'owner/repo')"
    )
    auto_build_branches: List[str] = Field(
        default=[], description="List of branches configured for automatic builds"
    )
    created_at: datetime = Field(default_factory=now_utc)
    updated_at: datetime = Field(default_factory=now_utc)
