from pydantic import BaseModel, Field
from typing import List, Optional, Dict

# --- Container API Models (used in Views) ---

class ContainerDetailView(BaseModel):
    """Schema for representing detailed container instance information."""
    id: str = Field(..., description="Short ID of the container")
    name: str = Field(..., description="Name of the container")
    status: str = Field(..., description="Current status (e.g., running, exited, paused)")
    image: str = Field(..., description="Image tag the container is based on")
    ports: Dict[str, str] = Field(default_factory=dict, description="Mapping of internal_port/protocol to host_ip:host_port or 'exposed'")

class ContainerStatusInfoView(BaseModel):
    """Schema for representing aggregated container status for a repository."""
    repo_full_name: str = Field(..., description="Full name of the repository (e.g., owner/repo)")
    running: int = Field(..., description="Number of running containers for this repo")
    stopped: int = Field(..., description="Number of stopped (exited) containers for this repo")
    paused: int = Field(..., description="Number of paused containers for this repo")
    memory_usage_mb: Optional[float] = Field(None, description="Total memory usage in MB (placeholder)")
    cpu_usage_percent: Optional[float] = Field(None, description="Total CPU usage percentage (placeholder)")
    containers: List[ContainerDetailView] = Field(..., description="List of detailed container instances for this repo")

    class Config:
        json_schema_extra = {
            "example": {
                "repo_full_name": "user/example-app",
                "running": 2,
                "stopped": 0,
                "paused": 0,
                "memory_usage_mb": 512.5,
                "cpu_usage_percent": 15.3,
                "containers": [
                    {
                        "id": "a1b2c3d4",
                        "name": "example-app-web-1",
                        "status": "running",
                        "image": "user/example-app:latest",
                        "ports": {"80/tcp": "0.0.0.0:8080", "443/tcp": "exposed"}
                    },
                    {
                        "id": "e5f6g7h8",
                        "name": "example-app-web-2",
                        "status": "running",
                        "image": "user/example-app:latest",
                        "ports": {"80/tcp": "0.0.0.0:8081"}
                    }
                ]
            }
        }

class ScaleRequestView(BaseModel):
    """Schema for the request body when scaling container instances."""
    desired_instances: int = Field(..., ge=0, description="The desired number of running instances (0 or more).")

class ScaleResponseView(BaseModel):
    """Schema for the response body after a scale operation."""
    started: int = Field(..., description="Number of new container instances started.")
    removed: int = Field(..., description="Number of existing container instances removed.")
