import subprocess
import multiprocessing
import time
import os
import shutil
import json
import logging
from datetime import datetime
from typing import Any, Optional, Dict # Import Dict

logger = logging.getLogger(__name__)

# Define message types for clarity
MSG_TYPE_STATUS = "status"
MSG_TYPE_LOG = "log"
MSG_TYPE_ERROR = "error"
MSG_TYPE_COMMIT_INFO = "commit_info" 
MSG_TYPE_END = "end"

def send_message(queue: multiprocessing.Queue, build_id: str, msg_type: str, payload: Any): 
    """Helper to send structured messages to the queue."""
    try:
        timestamp = datetime.utcnow().isoformat() + "Z"
        message = json.dumps({
            "build_id": build_id,
            "type": msg_type,
            "payload": payload,
            "timestamp": timestamp
        })
        queue.put(message)
    except Exception as e:
        logger.error(f"Build {build_id}: Failed to put message on queue: {e}")

def stream_process_output(process: subprocess.Popen, queue: multiprocessing.Queue, build_id: str):
    """Reads stdout/stderr line by line and sends to queue."""
    stderr_output = ""
    if process.stdout:
        for line in iter(process.stdout.readline, b''):
            decoded_line = line.decode('utf-8', errors='replace').strip()
            if decoded_line: 
                send_message(queue, build_id, MSG_TYPE_LOG, decoded_line)
        process.stdout.close()
    
    if process.stderr:
        stderr_output = process.stderr.read().decode('utf-8', errors='replace').strip()
        if stderr_output:
             send_message(queue, build_id, MSG_TYPE_ERROR, f"STDERR: {stderr_output}")
        process.stderr.close()
        
    return stderr_output

# Update to return a dictionary with sha and message
def get_commit_info(clone_dir: str) -> Optional[Dict[str, str]]:
     """Gets the current commit SHA and message from a git repository directory."""
     try:
         sha_result = subprocess.run(
             ["git", "rev-parse", "HEAD"],
             cwd=clone_dir, capture_output=True, text=True, check=True
         )
         sha = sha_result.stdout.strip()
         
         # Get commit message (subject line)
         msg_result = subprocess.run(
             ["git", "log", "-1", "--pretty=%s"], # %s gets subject line
             cwd=clone_dir, capture_output=True, text=True, check=True
         )
         message = msg_result.stdout.strip()
         
         return {"sha": sha, "message": message}
     except Exception as e:
         logger.error(f"Failed to get commit info in {clone_dir}: {e}")
         return None


def run_build_task(queue: multiprocessing.Queue, build_id: str, repo_url: str, branch: str):
    """The main function executed by the build runner process."""
    
    send_message(queue, build_id, MSG_TYPE_STATUS, "Build process started.")
    logger.info(f"Build {build_id}: Runner process started for {repo_url} branch {branch}")
    
    commit_info: Optional[Dict[str, str]] = None # Store commit info dict

    try:
        repo_full_name = '/'.join(repo_url.split('/')[-2:]).replace('.git', '')
        repo_name = repo_full_name.split('/')[-1]
    except IndexError:
        logger.error(f"Build {build_id}: Could not parse repository name from URL: {repo_url}")
        send_message(queue, build_id, MSG_TYPE_ERROR, f"Could not parse repository name from URL: {repo_url}")
        send_message(queue, build_id, MSG_TYPE_END, "Build failed.")
        queue.put(None) 
        return 

    clone_dir = f"/tmp/{build_id}_{repo_name}"
    
    try:
        # 1. Clean up previous directory if exists
        if os.path.exists(clone_dir):
            send_message(queue, build_id, MSG_TYPE_STATUS, f"Cleaning up existing directory: {clone_dir}")
            shutil.rmtree(clone_dir)
            
        # 2. Clone the repository
        send_message(queue, build_id, MSG_TYPE_STATUS, f"Cloning repository {repo_url} branch {branch} into {clone_dir}...")
        git_clone_cmd = ["git", "clone", "--branch", branch, "--depth", "1", repo_url, clone_dir]
        
        process = subprocess.Popen(git_clone_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        stderr_output = stream_process_output(process, queue, build_id)
        return_code = process.wait()

        if return_code != 0:
            error_msg = f"Git clone failed with return code {return_code}."
            if stderr_output: error_msg += f" Error details: {stderr_output}"
            raise Exception(error_msg)
            
        send_message(queue, build_id, MSG_TYPE_STATUS, "Repository cloned successfully.")

        # 3. Get Commit Info (SHA and Message)
        commit_info = get_commit_info(clone_dir)
        if commit_info:
             # Send commit info as payload (dictionary)
             send_message(queue, build_id, MSG_TYPE_COMMIT_INFO, commit_info) 
             send_message(queue, build_id, MSG_TYPE_STATUS, f"Building commit: {commit_info['sha'][:7]} ({commit_info['message']})")
        else:
             send_message(queue, build_id, MSG_TYPE_ERROR, "Failed to retrieve commit info.")
             logger.error(f"Build {build_id}: Could not get commit info.")


        # 4. Build the Docker image
        dockerfile_path = os.path.join(clone_dir, "Dockerfile") 
        if not os.path.exists(dockerfile_path):
             raise FileNotFoundError(f"Dockerfile not found in the root of repository {repo_full_name} branch {branch}")
             
        image_tag = f"cypher-build:{build_id}" 
        
        send_message(queue, build_id, MSG_TYPE_STATUS, f"Starting Docker build (tag: {image_tag}, no-cache)...")
        docker_build_cmd = ["docker", "build", "--no-cache", "-t", image_tag, "."] 
        
        process = subprocess.Popen(docker_build_cmd, cwd=clone_dir, stdout=subprocess.PIPE, stderr=subprocess.PIPE) 
        stderr_output = stream_process_output(process, queue, build_id)
        return_code = process.wait()

        if return_code != 0:
            error_msg = f"Docker build failed with return code {return_code}."
            if stderr_output: error_msg += f" Error details: {stderr_output}"
            raise Exception(error_msg)

        send_message(queue, build_id, MSG_TYPE_STATUS, f"Docker image built successfully: {image_tag}")
        
        # TODO: Add Docker push step if needed
        
        send_message(queue, build_id, MSG_TYPE_END, "Build completed successfully.")
        logger.info(f"Build {build_id}: Completed successfully.")

    except FileNotFoundError as e:
         error_msg = f"Build failed: {e}"
         logger.error(f"Build {build_id}: {error_msg}")
         send_message(queue, build_id, MSG_TYPE_ERROR, error_msg)
         send_message(queue, build_id, MSG_TYPE_END, "Build failed.")
    except Exception as e:
        error_msg = f"An unexpected error occurred during build: {e}"
        logger.error(f"Build {build_id}: {error_msg}", exc_info=True)
        send_message(queue, build_id, MSG_TYPE_ERROR, error_msg)
        send_message(queue, build_id, MSG_TYPE_END, "Build failed.")
    finally:
        if os.path.exists(clone_dir):
            try:
                send_message(queue, build_id, MSG_TYPE_STATUS, f"Cleaning up build directory: {clone_dir}")
                shutil.rmtree(clone_dir)
            except Exception as e:
                 logger.error(f"Build {build_id}: Failed to clean up directory {clone_dir}: {e}")
                 send_message(queue, build_id, MSG_TYPE_ERROR, f"Failed to clean up directory {clone_dir}")
        
        queue.put(None) 
        logger.info(f"Build {build_id}: Runner process finished.")
