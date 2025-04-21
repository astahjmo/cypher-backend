from pydantic import BaseModel, Field
from typing import List, Optional
from bson import ObjectId # Import ObjectId for json_encoders

# --- GitHub API Models (used in Views) ---

class RepositoryView(BaseModel):
    """Schema for representing a repository in API responses."""
    name: str
    full_name: str
    private: bool
    url: str

class BranchView(BaseModel):
    """Schema for representing a branch in API responses."""
    name: str

class BranchConfigPayload(BaseModel):
    """Schema for the request body when configuring branches."""
    branches: List[str] = Field(..., description="List of branch names to configure for automatic builds.")

class RepositoryConfigView(BaseModel):
    """Schema for representing repository configuration in API responses."""
    id: str = Field(..., alias="_id")
    user_id: str # Keep as string representation of ObjectId
    repo_full_name: str
    auto_build_branches: List[str]
    created_at: str # Keep as string
    updated_at: str # Keep as string

    class Config:
        populate_by_name = True
        json_encoders = {ObjectId: str} # Ensure ObjectId is serialized correctly if passed directly
