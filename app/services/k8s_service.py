import logging
from kubernetes import client, config, watch
from kubernetes.client.exceptions import ApiException
from ..config import settings
import time
import uuid
import json # Import json at the top

logger = logging.getLogger(__name__)

class KubernetesService:
    def __init__(self):
        try:
            # Load Kubernetes configuration (logic remains the same)
            if settings.KUBECONFIG_PATH:
                config.load_kube_config(config_file=settings.KUBECONFIG_PATH)
                logger.info(f"Loaded Kubernetes config from: {settings.KUBECONFIG_PATH}")
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

    def run_build_job(self, image_name: str, repo_url: str, branch: str, commit_sha: str, build_image_tag: str) -> tuple[bool, str]:
        """
        Creates and runs a Kubernetes Job to perform a Git clone, Docker build, and push.
        Includes the commit SHA for logging and potentially for checkout.
        Requires appropriate volume mounts and secrets configured in the cluster.
        """
        if not self.batch_v1_api or not self.core_v1_api:
             logger.error("Attempted to run build job, but Kubernetes client is not available.")
             return False, "Kubernetes client not available."

        job_name = f"{image_name}-build-{uuid.uuid4().hex[:6]}"
        namespace = settings.K8S_NAMESPACE
        # Define workspace path within the pod
        workspace_path = "/workspace"

        # --- Define Build Steps ---
        # Include commit_sha in the script for logging
        build_script = f"""
            set -e # Exit immediately if a command exits with a non-zero status.
            echo "--- Starting build job: {job_name} ---"
            echo "Repo: {repo_url}"
            echo "Branch: {branch}"
            echo "Commit SHA: {commit_sha}"
            echo "Target Image: {build_image_tag}"

            echo "--- Cloning repository ---"
            # Clone the specific branch first
            git clone --branch "{branch}" --depth 1 "{repo_url}" "{workspace_path}"
            cd "{workspace_path}"
            # Optional: Checkout the specific commit SHA if needed, though cloning the branch tip is often sufficient for CI/CD
            # echo "Checking out specific commit: {commit_sha}"
            # git checkout "{commit_sha}"
            echo "Cloned commit:" $(git rev-parse HEAD) # Log the actual commit used

            echo "--- Building Docker image ---"
            # Assumes Docker daemon access (e.g., via mounted socket or Docker-in-Docker sidecar)
            docker build -t "{build_image_tag}" .

            echo "--- Logging into registry: {settings.REGISTRY_URL} ---"
            # Assumes REGISTRY_USER and REGISTRY_PASSWORD are set as env vars from secrets
            echo "$REGISTRY_PASSWORD" | docker login "{settings.REGISTRY_URL}" -u "$REGISTRY_USER" --password-stdin

            echo "--- Pushing Docker image ---"
            docker push "{build_image_tag}"

            echo "--- Build job {job_name} completed successfully ---"
        """

        # --- Define Container ---
        container = client.V1Container(
            name=f"{image_name}-build-container",
            image="docker:git", # Use an image with both docker and git
            command=["/bin/sh", "-c"],
            args=[build_script],
            working_dir=workspace_path, # Set working directory for the script
            env=[
                client.V1EnvVar(name="REPO_URL", value=repo_url),
                client.V1EnvVar(name="BRANCH", value=branch),
                client.V1EnvVar(name="COMMIT_SHA", value=commit_sha), # Pass commit SHA as env var if needed by script logic elsewhere
                client.V1EnvVar(name="IMAGE_TAG", value=build_image_tag),
                # --- IMPORTANT: Mount credentials securely via Secrets ---
                client.V1EnvVar(name="REGISTRY_USER", value_from=client.V1EnvVarSource(secret_key_ref=client.V1SecretKeySelector(name="registry-creds", key="username", optional=False))), # Mark as non-optional
                client.V1EnvVar(name="REGISTRY_PASSWORD", value_from=client.V1EnvVarSource(secret_key_ref=client.V1SecretKeySelector(name="registry-creds", key="password", optional=False))), # Mark as non-optional
                # Optional: Git token for private repos
                # client.V1EnvVar(name="GIT_TOKEN", value_from=client.V1EnvVarSource(secret_key_ref=client.V1SecretKeySelector(name="git-creds", key="token", optional=True))),
            ],
            volume_mounts=[
                # Mount Docker socket
                client.V1VolumeMount(name="docker-sock", mount_path="/var/run/docker.sock"),
                # Optional: Mount a persistent volume for workspace/cache if needed
                # client.V1VolumeMount(name="workspace-volume", mount_path=workspace_path),
            ]
        )

        # --- Define Volumes ---
        volumes = [
             # Mount host's Docker socket
             client.V1Volume(name="docker-sock", host_path=client.V1HostPathVolumeSource(path="/var/run/docker.sock")),
             # Optional: Define persistent volume claim if using workspace-volume mount
             # client.V1Volume(name="workspace-volume", persistent_volume_claim=client.V1PersistentVolumeClaimVolumeSource(claim_name="build-workspace-pvc")),
        ]

        # --- Define Job ---
        template = client.V1PodTemplateSpec(
            metadata=client.V1ObjectMeta(labels={"app": image_name, "type": "build"}),
            spec=client.V1PodSpec(
                restart_policy="Never",
                containers=[container],
                volumes=volumes,
                # Optional: Specify service account if needed for permissions
                # service_account_name="build-service-account"
                )
        )
        # Job spec: backoff limit, TTL for cleanup
        job_spec = client.V1JobSpec(
            template=template,
            backoff_limit=1, # Retry once on failure
            ttl_seconds_after_finished=3600 # Clean up job 1 hour after completion
        )
        job = client.V1Job(
            api_version="batch/v1",
            kind="Job",
            metadata=client.V1ObjectMeta(name=job_name, namespace=namespace),
            spec=job_spec
        )

        # --- Create Job ---
        try:
            logger.info(f"Creating Kubernetes Job: {job_name} in namespace {namespace} for commit {commit_sha[:7]}")
            # The create_namespaced_job call itself will raise ApiException on failure
            api_response = self.batch_v1_api.create_namespaced_job(body=job, namespace=namespace)
            # If we reach here, the job creation API call was accepted by K8s.
            # Avoid accessing api_response.status which caused type errors.
            logger.info(f"Job {job_name} creation request accepted by Kubernetes API.")
            return True, job_name # Return job name on successful API call
        except ApiException as e:
            logger.error(f"ApiException when creating Kubernetes Job {job_name}: {e.reason}\nBody: {e.body}", exc_info=False)
            error_message = e.reason
            if isinstance(e.body, str) and e.body:
                try:
                    body_json = json.loads(e.body)
                    error_message = body_json.get("message", e.reason)
                except json.JSONDecodeError:
                    logger.warning(f"Could not decode ApiException body as JSON: {e.body}")
            return False, f"Failed to create K8s Job: {error_message}"
        except Exception as e:
            logger.error(f"Unexpected error creating Kubernetes Job {job_name}: {e}", exc_info=True)
            return False, f"Unexpected error creating K8s Job: {e}"

    # TODO: Add functions to get job status, logs, etc.

# Single instance
k8s_service = KubernetesService()
