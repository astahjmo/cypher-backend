from pydantic import Field
from typing import Optional
from datetime import datetime

# Import base model and helpers from the new base location
from ..base import PyObjectId, BaseModelWithDates, now_utc

class User(BaseModelWithDates):
    """Represents a user authenticated via GitHub (Database Model)."""
    id: PyObjectId = Field(default_factory=PyObjectId, alias="_id")
    github_id: int = Field(..., description="GitHub user ID")
    login: str = Field(..., description="GitHub username")
    name: Optional[str] = None
    email: Optional[str] = None
    avatar_url: Optional[str] = None
    # Note: Storing access tokens directly is insecure. Consider alternatives.
    github_access_token: Optional[str] = Field(None, description="User's GitHub OAuth token", exclude=True)
    created_at: datetime = Field(default_factory=now_utc)
    updated_at: datetime = Field(default_factory=now_utc)
