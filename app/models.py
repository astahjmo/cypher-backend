from pydantic import BaseModel, Field, HttpUrl, field_validator, BeforeValidator, ConfigDict # Import ConfigDict
# Import Annotated and WithJsonSchema
from typing import List, Optional, Annotated
from pydantic.json_schema import WithJsonSchema
from datetime import datetime, timezone # Import timezone
from bson import ObjectId
import uuid

# Updated PyObjectId to include JSON schema information
PyObjectId = Annotated[
    ObjectId,
    BeforeValidator(lambda v: ObjectId(v) if ObjectId.is_valid(v) else v),
    WithJsonSchema({"type": "string", "format": "objectid"}, mode='serialization') # Treat as string in schema
]

# Helper function for timezone-aware UTC default factory
def now_utc():
    return datetime.now(timezone.utc)

# --- Base Model with Date Serialization Config ---
def serialize_datetime(dt: datetime) -> str:
    """Ensure datetime is UTC and format to ISO 8601 with Z."""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc) # Assume UTC if naive
    else:
        dt = dt.astimezone(timezone.utc) # Convert to UTC if aware but different timezone
    # Format with Z, limiting microseconds to 3 digits for compatibility
    dt_str = dt.strftime('%Y-%m-%dT%H:%M:%S.%f')
    return dt_str[:-3] + 'Z'

class BaseModelWithDates(BaseModel):
    """Base model providing common configuration including date serialization."""
    model_config = ConfigDict(
        populate_by_name=True, # Allows using alias like '_id' in initialization
        arbitrary_types_allowed=True, # Allows types like ObjectId
        json_encoders={
            ObjectId: str, # Keep ObjectId serialization to string
            datetime: serialize_datetime # Use custom serializer for all datetimes
        }
    )

# --- Application Models ---

# User model now inherits from BaseModelWithDates
class User(BaseModelWithDates):
    """Represents a user authenticated via GitHub."""
    id: PyObjectId = Field(default_factory=ObjectId, alias="_id")
    github_id: int = Field(..., description="GitHub user ID")
    login: str = Field(..., description="GitHub username")
    name: Optional[str] = None
    email: Optional[str] = None
    avatar_url: Optional[str] = None
    github_access_token: Optional[str] = Field(None, description="User's GitHub OAuth token (Stored insecurely!)", exclude=True)
    created_at: datetime = Field(default_factory=now_utc)
    updated_at: datetime = Field(default_factory=now_utc)
    # Removed internal Config class, inherits from BaseModelWithDates


# RepositoryConfig model now inherits from BaseModelWithDates
class RepositoryConfig(BaseModelWithDates):
    """Configuration for automatic builds for a specific repository."""
    id: PyObjectId = Field(default_factory=ObjectId, alias="_id")
    user_id: PyObjectId = Field(..., description="Internal user ID this config belongs to")
    repo_full_name: str = Field(..., description="Full repository name (e.g., 'owner/repo')")
    auto_build_branches: List[str] = Field(default=[], description="List of branches configured for automatic builds")
    created_at: datetime = Field(default_factory=now_utc)
    updated_at: datetime = Field(default_factory=now_utc)
    # Removed internal Config class, inherits from BaseModelWithDates


# BuildStatus model now inherits from BaseModelWithDates
class BuildStatus(BaseModelWithDates):
    """Represents the status and details of a specific build job."""
    id: PyObjectId = Field(default_factory=ObjectId, alias="_id")
    user_id: PyObjectId = Field(..., description="Internal user ID who initiated the build")
    repo_full_name: str = Field(..., description="Repository being built")
    branch: str = Field(..., description="Branch being built")
    commit_sha: Optional[str] = None
    commit_message: Optional[str] = None
    k8s_job_name: Optional[str] = None
    status: str = Field(default="pending", description="Build status (e.g., pending, running, success, failed)")
    image_tag: Optional[str] = None
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    created_at: datetime = Field(default_factory=now_utc)
    updated_at: Optional[datetime] = Field(default_factory=now_utc)
    message: Optional[str] = None
    # Removed internal Config class, inherits from BaseModelWithDates


# BuildLog model now inherits from BaseModelWithDates
class BuildLog(BaseModelWithDates):
    """Represents a single log entry for a build."""
    id: PyObjectId = Field(default_factory=ObjectId, alias="_id")
    build_id: PyObjectId = Field(..., description="ID of the build this log belongs to")
    timestamp: datetime = Field(default_factory=now_utc, description="Timestamp of the log entry")
    type: str = Field(..., description="Type of log entry (e.g., status, log, error)")
    message: str = Field(..., description="The log message content")
    # Removed internal Config class, inherits from BaseModelWithDates
