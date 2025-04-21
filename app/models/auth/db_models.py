from pydantic import Field
from typing import Optional
from datetime import datetime

from ..base import PyObjectId, BaseModelWithDates, now_utc


class User(BaseModelWithDates):
    """Represents a user authenticated via GitHub (Database Model).

    Stores basic user information obtained from GitHub and the OAuth access token.

    Attributes:
        id (PyObjectId): The unique user ID in the MongoDB database (alias for _id).
        github_id (int): The unique numeric user ID from GitHub.
        login (str): The GitHub username (login).
        name (Optional[str]): The user's display name from GitHub.
        email (Optional[str]): The user's public email address from GitHub.
        avatar_url (Optional[str]): The URL for the user's avatar from GitHub.
        github_access_token (Optional[str]): The user's GitHub OAuth access token.
                                             This field is excluded from default serialization.
        created_at (datetime): Timestamp when the user record was created (UTC).
        updated_at (datetime): Timestamp of the last update to the user record (UTC).
    """

    id: PyObjectId = Field(default_factory=PyObjectId, alias="_id")
    github_id: int = Field(..., description="GitHub user ID")
    login: str = Field(..., description="GitHub username")
    name: Optional[str] = None
    email: Optional[str] = None
    avatar_url: Optional[str] = None
    github_access_token: Optional[str] = Field(
        None, description="User's GitHub OAuth token", exclude=True
    )
    created_at: datetime = Field(default_factory=now_utc)
    updated_at: datetime = Field(default_factory=now_utc)
