from pydantic import BaseModel, Field, field_validator
from typing import List, Optional
from datetime import datetime

# Import base model and helpers from the new base location
from ..base import PyObjectId, BaseModelWithDates, now_utc

# --- Container Runtime Configuration Database Models ---

class VolumeMapping(BaseModel):
    """Represents a host-to-container volume mapping."""
    host_path: str = Field(..., description="Path on the host machine")
    container_path: str = Field(..., description="Path inside the container")

class EnvironmentVariable(BaseModel):
    """Represents an environment variable for a container."""
    name: str = Field(..., description="Name of the environment variable")
    value: str = Field(..., description="Value of the environment variable")

class LabelPair(BaseModel):
    """Represents a key-value pair for container labels."""
    key: str = Field(..., description="Label key")
    value: str = Field(..., description="Label value")

class PortMapping(BaseModel):
    """Represents a port mapping for a container."""
    container_port: int = Field(..., description="Port inside the container")
    host_port: Optional[int] = Field(None, description="Port on the host machine (optional, Docker assigns if None)")
    protocol: str = Field(default="tcp", description="Protocol (tcp or udp)")

    @field_validator('protocol')
    def validate_protocol(cls, v):
        if v not in ['tcp', 'udp']:
            raise ValueError('Protocol must be tcp or udp')
        return v

class ContainerRuntimeConfig(BaseModelWithDates):
    """Stores runtime configuration settings for a container associated with a repository (Database Model)."""
    id: PyObjectId = Field(default_factory=PyObjectId, alias="_id")
    user_id: Optional[PyObjectId] = Field(None, description="Internal user ID this config belongs to (set by backend)")
    repo_full_name: str = Field(..., description="Full repository name (e.g., 'owner/repo') this config applies to")
    scaling: int = Field(default=1, ge=0, description="Desired number of running container instances")
    volumes: List[VolumeMapping] = Field(default=[], description="List of volume mappings")
    environment_variables: List[EnvironmentVariable] = Field(default=[], description="List of environment variables")
    labels: List[LabelPair] = Field(default=[], description="List of key-value labels for the container")
    network_mode: Optional[str] = Field(None, description="Docker network mode (e.g., bridge, host, none, custom_network_name)")
    port_mappings: List[PortMapping] = Field(default=[], description="List of port mappings")
    created_at: datetime = Field(default_factory=now_utc)
    updated_at: datetime = Field(default_factory=now_utc)
