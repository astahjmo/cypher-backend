import logging
import requests
from typing import Optional, Dict, Any
from datetime import datetime # Import datetime

from config import settings # Import settings to get the webhook URL

logger = logging.getLogger(__name__)

def send_discord_build_notification(
    repo_full_name: str,
    branch_name: str,
    commit_sha: Optional[str], # Make commit info optional for different statuses
    commit_message: Optional[str],
    pusher_name: Optional[str] = None, # Make pusher optional
    build_id: Optional[str] = None,
    status: str = "Triggered"
):
    """Sends a build notification embed to the configured Discord webhook."""

    webhook_url = settings.DISCORD_WEBHOOK_URL
    if not webhook_url:
        logger.warning("DISCORD_WEBHOOK_URL not configured. Skipping notification.")
        return

    logger.info(f"Preparing Discord notification for {repo_full_name} - Status: {status}")

    # Determine color based on status
    color = 0x5865F2 # Default blurple
    if status.lower() == 'success':
        color = 0x2ECC71 # Green
    elif status.lower() == 'failed':
        color = 0xE74C3C # Red
    elif status.lower() == 'running':
        color = 0x3498DB # Blue
    elif status.lower() == 'pending':
         color = 0xFEE75C # Yellow
    elif status.lower() == 'received push':
         color = 0x99AAB5 # Grey

    # Limit commit SHA for display
    short_commit_sha = commit_sha[:7] if commit_sha else "N/A"
    display_pusher = pusher_name or "System/Manual" # Default pusher name

    # Limit commit message length
    truncated_commit_message = "N/A"
    if commit_message:
        truncated_commit_message = (commit_message[:100] + '...') if len(commit_message) > 100 else commit_message

    # Construct the embed payload
    embed = {
        "title": f"🚀 Build {status}: {repo_full_name}",
        "color": color,
        "fields": [
            {"name": "Branch", "value": f"`{branch_name}`", "inline": True},
            {"name": "Commit", "value": f"`{short_commit_sha}`", "inline": True},
            {"name": "Triggered By", "value": display_pusher, "inline": True}, # Changed field name
        ],
        "footer": {"text": f"Cypher Build System | Build ID: {build_id or 'N/A'}"},
        "timestamp": datetime.utcnow().isoformat()
    }

    # Only add commit message field if it exists
    if commit_message:
         embed["fields"].append({"name": "Commit Message", "value": truncated_commit_message, "inline": False})

    # Add commit URL if possible
    if commit_sha and '/' in repo_full_name:
         commit_url = f"https://github.com/{repo_full_name}/commit/{commit_sha}"
         embed["fields"].append({"name": "Commit URL", "value": f"[View Commit]({commit_url})", "inline": False})

    # Add Build URL if build_id exists and FRONTEND_URL is set
    if build_id and settings.FRONTEND_URL:
         # Ensure FRONTEND_URL doesn't end with / before appending
         base_frontend_url = settings.FRONTEND_URL.rstrip('/')
         build_url = f"{base_frontend_url}/builds/{build_id}"
         embed["fields"].append({"name": "Build URL", "value": f"[View Build Details]({build_url})", "inline": False})


    payload = {
        "username": "Cypher Build Bot",
        "embeds": [embed]
    }

    logger.debug(f"Discord payload prepared: {payload}")

    try:
        logger.info(f"Attempting to send notification via requests.post to {webhook_url[:30]}...")
        response = requests.post(webhook_url, json=payload, timeout=15)
        logger.info(f"Discord notification request completed. Status Code: {response.status_code}")
        response.raise_for_status()
        logger.info(f"Successfully sent Discord build notification for {repo_full_name} @ {branch_name} (Status: {status})")
    except requests.exceptions.Timeout:
         logger.error(f"Timeout sending Discord notification for {repo_full_name}.", exc_info=True)
    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to send Discord notification for {repo_full_name}: {e}", exc_info=True)
    except Exception as e:
        logger.error(f"Unexpected error sending Discord notification: {e}", exc_info=True)

# Example usage remains the same
