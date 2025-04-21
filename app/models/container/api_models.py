from pydantic import BaseModel, Field
from typing import List, Optional, Dict


class ContainerDetailView(BaseModel):
    """Schema for representing detailed container instance information.

    Attributes:
        id (str): Short ID of the container.
        name (str): Name of the container.
        status (str): Current status (e.g., running, exited, paused).
        image (str): Image tag the container is based on.
        ports (Dict[str, str]): Mapping of internal_port/protocol to host_ip:host_port or 'exposed'.
    """

    id: str = Field(..., description="Short ID of the container")
    name: str = Field(..., description="Name of the container")
    status: str = Field(
        ..., description="Current status (e.g., running, exited, paused)"
    )
    image: str = Field(..., description="Image tag the container is based on")
    ports: Dict[str, str] = Field(
        default_factory=dict,
        description="Mapping of internal_port/protocol to host_ip:host_port or 'exposed'",
    )


class ContainerStatusInfoView(BaseModel):
    """Schema for representing aggregated container status for a repository.

    Provides a summary of container states (running, stopped, paused) and resource usage
    for all containers belonging to a specific repository, along with a list of
    detailed container instances.

    Attributes:
        repo_full_name (str): Full name of the repository (e.g., owner/repo).
        running (int): Number of running containers for this repo.
        stopped (int): Number of stopped (exited) containers for this repo.
        paused (int): Number of paused containers for this repo.
        memory_usage_mb (Optional[float]): Total memory usage in MB (placeholder).
        cpu_usage_percent (Optional[float]): Total CPU usage percentage (placeholder).
        containers (List[ContainerDetailView]): List of detailed container instances for this repo.
    """

    repo_full_name: str = Field(
        ..., description="Full name of the repository (e.g., owner/repo)"
    )
    running: int = Field(..., description="Number of running containers for this repo")
    stopped: int = Field(
        ..., description="Number of stopped (exited) containers for this repo"
    )
    paused: int = Field(..., description="Number of paused containers for this repo")
    memory_usage_mb: Optional[float] = Field(
        None, description="Total memory usage in MB (placeholder)"
    )
    cpu_usage_percent: Optional[float] = Field(
        None, description="Total CPU usage percentage (placeholder)"
    )
    containers: List[ContainerDetailView] = Field(
        ..., description="List of detailed container instances for this repo"
    )

    model_config = {
        "json_schema_extra": {
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
                        "ports": {"80/tcp": "0.0.0.0:8080", "443/tcp": "exposed"},
                    },
                    {
                        "id": "e5f6g7h8",
                        "name": "example-app-web-2",
                        "status": "running",
                        "image": "user/example-app:latest",
                        "ports": {"80/tcp": "0.0.0.0:8081"},
                    },
                ],
            }
        }
    }


class ScaleRequestView(BaseModel):
    """Schema for the request body when scaling container instances.

    Attributes:
        desired_instances (int): The desired number of running instances (0 or more).
    """

    desired_instances: int = Field(
        ..., ge=0, description="The desired number of running instances (0 or more)."
    )


class ScaleResponseView(BaseModel):
    """Schema for the response body after a scale operation.

    Attributes:
        started (int): Number of new container instances started.
        removed (int): Number of existing container instances removed.
    """

    started: int = Field(..., description="Number of new container instances started.")
    removed: int = Field(
        ..., description="Number of existing container instances removed."
    )
