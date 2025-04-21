from pydantic import BaseModel, Field, ConfigDict
from typing import List, Optional
from bson import ObjectId


class RepositoryView(BaseModel):
    """Schema for representing a repository in API responses.

    Attributes:
        name (str): The name of the repository.
        full_name (str): The full name of the repository (e.g., 'owner/repo').
        private (bool): Whether the repository is private.
        url (str): The HTML URL of the repository on GitHub.
    """

    name: str
    full_name: str
    private: bool
    url: str


class BranchView(BaseModel):
    """Schema for representing a branch in API responses.

    Attributes:
        name (str): The name of the branch.
    """

    name: str


class BranchConfigPayload(BaseModel):
    """Schema for the request body when configuring branches for auto-build.

    Attributes:
        branches (List[str]): A list of branch names to configure for automatic builds.
    """

    branches: List[str] = Field(
        ..., description="List of branch names to configure for automatic builds."
    )


class RepositoryConfigView(BaseModel):
    """Schema for representing repository configuration in API responses.

    Displays the configuration details stored for a repository, including which
    branches are set up for automatic builds.

    Attributes:
        id (str): The unique configuration ID (maps from MongoDB's _id).
        user_id (str): The string representation of the user's ObjectId this config belongs to.
        repo_full_name (str): The full name of the repository (e.g., 'owner/repo').
        auto_build_branches (List[str]): List of branch names configured for automatic builds.
        created_at (str): ISO 8601 formatted string timestamp of creation.
        updated_at (str): ISO 8601 formatted string timestamp of the last update.
    """

    id: str = Field(..., alias="_id")
    user_id: str
    repo_full_name: str
    auto_build_branches: List[str]
    created_at: str
    updated_at: str

    # Use model_config in Pydantic v2
    model_config = ConfigDict(
        populate_by_name=True,
        json_encoders={
            ObjectId: str
        },  # Ensure ObjectId is serialized correctly if passed directly
    )
