import logging
from kubernetes import client, config, watch
from kubernetes.client.exceptions import ApiException
from ..config import settings
import time
import uuid
import json

logger = logging.getLogger(__name__)


class KubernetesService:
    """Provides an interface for interacting with a Kubernetes cluster.

    Handles loading Kubernetes configuration and creating resources like Jobs.

    Attributes:
        core_v1_api: Kubernetes CoreV1Api client instance.
        batch_v1_api: Kubernetes BatchV1Api client instance.
    """

    def __init__(self):
        """Initializes the KubernetesService.

        Loads Kubernetes configuration from KUBECONFIG_PATH, in-cluster config,
        or default kubeconfig, and initializes API client instances.
        Sets API clients to None if initialization fails.
        """
        try:
            if settings.KUBECONFIG_PATH:
                config.load_kube_config(config_file=settings.KUBECONFIG_PATH)
                logger.info(
                    f"Loaded Kubernetes config from: {settings.KUBECONFIG_PATH}"
                )
            else:
                try:
                    config.load_incluster_config()
                    logger.info("Loaded in-cluster Kubernetes config.")
                except config.ConfigException:
                    config.load_kube_config()
                    logger.info("Loaded default Kubernetes config.")

            self.core_v1_api = client.CoreV1Api()
            self.batch_v1_api = client.BatchV1Api()
            logger.info("Kubernetes client initialized successfully.")
        except Exception as e:
            logger.error(f"Failed to initialize Kubernetes client: {e}", exc_info=True)
            self.core_v1_api = None
            self.batch_v1_api = None

    def run_build_job(
        self,
        image_name: str,
        repo_url: str,
        branch: str,
        commit_sha: str,
        build_image_tag: str,
    ) -> tuple[bool, str]:
        """Creates and runs a Kubernetes Job to build and push a Docker image from Git.

        The Job runs a container (using 'docker:git' image) that performs the following steps:
        1. Clones the specified branch of the repository.
        2. Builds a Docker image using the Dockerfile in the repository root.
        3. Logs into the configured Docker registry using credentials from a K8s secret ('registry-creds').
        4. Pushes the built image to the registry with the specified tag.

        Assumes necessary RBAC permissions, secrets ('registry-creds'), and Docker daemon access
        (e.g., via mounted Docker socket) are configured in the Kubernetes cluster.

        Args:
            image_name (str): Base name for the image and job resources (e.g., 'my-app').
            repo_url (str): The URL of the Git repository to clone.
            branch (str): The branch to clone and build.
            commit_sha (str): The specific commit SHA being built (used for logging).
            build_image_tag (str): The full tag for the Docker image to be built and pushed
                                   (e.g., 'registry.example.com/owner/repo:latest').

        Returns:
            tuple[bool, str]: A tuple containing:
                - bool: True if the Job creation API call was successful, False otherwise.
                - str: The generated name of the Kubernetes Job if creation was successful,
                       or an error message if it failed.
        """
        if not self.batch_v1_api or not self.core_v1_api:
            logger.error(
                "Attempted to run build job, but Kubernetes client is not available."
            )
            return False, "Kubernetes client not available."

        job_name = f"{image_name}-build-{uuid.uuid4().hex[:6]}"
        namespace = settings.K8S_NAMESPACE
        workspace_path = "/workspace"

        build_script = f"""
            set -e
            echo "--- Starting build job: {job_name} ---"
            echo "Repo: {repo_url}"
            echo "Branch: {branch}"
            echo "Commit SHA: {commit_sha}"
            echo "Target Image: {build_image_tag}"

            echo "--- Cloning repository ---"
            git clone --branch "{branch}" --depth 1 "{repo_url}" "{workspace_path}"
            cd "{workspace_path}"
            echo "Cloned commit:" $(git rev-parse HEAD)

            echo "--- Building Docker image ---"
            docker build -t "{build_image_tag}" .

            echo "--- Logging into registry: {settings.REGISTRY_URL} ---"
            echo "$REGISTRY_PASSWORD" | docker login "{settings.REGISTRY_URL}" -u "$REGISTRY_USER" --password-stdin

            echo "--- Pushing Docker image ---"
            docker push "{build_image_tag}"

            echo "--- Build job {job_name} completed successfully ---"
        """

        container = client.V1Container(
            name=f"{image_name}-build-container",
            image="docker:git",
            command=["/bin/sh", "-c"],
            args=[build_script],
            working_dir=workspace_path,
            env=[
                client.V1EnvVar(name="REPO_URL", value=repo_url),
                client.V1EnvVar(name="BRANCH", value=branch),
                client.V1EnvVar(name="COMMIT_SHA", value=commit_sha),
                client.V1EnvVar(name="IMAGE_TAG", value=build_image_tag),
                client.V1EnvVar(
                    name="REGISTRY_USER",
                    value_from=client.V1EnvVarSource(
                        secret_key_ref=client.V1SecretKeySelector(
                            name="registry-creds", key="username", optional=False
                        )
                    ),
                ),
                client.V1EnvVar(
                    name="REGISTRY_PASSWORD",
                    value_from=client.V1EnvVarSource(
                        secret_key_ref=client.V1SecretKeySelector(
                            name="registry-creds", key="password", optional=False
                        )
                    ),
                ),
            ],
            volume_mounts=[
                client.V1VolumeMount(
                    name="docker-sock", mount_path="/var/run/docker.sock"
                ),
            ],
        )

        volumes = [
            client.V1Volume(
                name="docker-sock",
                host_path=client.V1HostPathVolumeSource(path="/var/run/docker.sock"),
            ),
        ]

        template = client.V1PodTemplateSpec(
            metadata=client.V1ObjectMeta(labels={"app": image_name, "type": "build"}),
            spec=client.V1PodSpec(
                restart_policy="Never",
                containers=[container],
                volumes=volumes,
            ),
        )

        job_spec = client.V1JobSpec(
            template=template, backoff_limit=1, ttl_seconds_after_finished=3600
        )
        job = client.V1Job(
            api_version="batch/v1",
            kind="Job",
            metadata=client.V1ObjectMeta(name=job_name, namespace=namespace),
            spec=job_spec,
        )

        try:
            logger.info(
                f"Creating Kubernetes Job: {job_name} in namespace {namespace} for commit {commit_sha[:7]}"
            )
            api_response = self.batch_v1_api.create_namespaced_job(
                body=job, namespace=namespace
            )
            logger.info(f"Job {job_name} creation request accepted by Kubernetes API.")
            return True, job_name
        except ApiException as e:
            logger.error(
                f"ApiException when creating Kubernetes Job {job_name}: {e.reason}\nBody: {e.body}",
                exc_info=False,
            )
            error_message = e.reason
            if isinstance(e.body, str) and e.body:
                try:
                    body_json = json.loads(e.body)
                    error_message = body_json.get("message", e.reason)
                except json.JSONDecodeError:
                    logger.warning(
                        f"Could not decode ApiException body as JSON: {e.body}"
                    )
            return False, f"Failed to create K8s Job: {error_message}"
        except Exception as e:
            logger.error(
                f"Unexpected error creating Kubernetes Job {job_name}: {e}",
                exc_info=True,
            )
            return False, f"Unexpected error creating K8s Job: {e}"


# Singleton instance of the KubernetesService
k8s_service = KubernetesService()
