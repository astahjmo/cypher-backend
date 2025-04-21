from pydantic import Field
from typing import List
from datetime import datetime

# Import base model and helpers from the new base location
from ..base import PyObjectId, BaseModelWithDates, now_utc

class RepositoryConfig(BaseModelWithDates):
    """Configuration for automatic builds for a specific repository (Database Model)."""
    id: PyObjectId = Field(default_factory=PyObjectId, alias="_id")
    user_id: PyObjectId = Field(..., description="Internal user ID this config belongs to")
    repo_full_name: str = Field(..., description="Full repository name (e.g., 'owner/repo')")
    auto_build_branches: List[str] = Field(default=[], description="List of branches configured for automatic builds")
    created_at: datetime = Field(default_factory=now_utc)
    updated_at: datetime = Field(default_factory=now_utc)
