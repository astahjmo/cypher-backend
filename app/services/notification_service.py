import logging
import requests
from typing import Optional, Dict, Any
from datetime import datetime

from config import settings

logger = logging.getLogger(__name__)


def send_discord_build_notification(
    repo_full_name: str,
    branch_name: str,
    commit_sha: Optional[str],
    commit_message: Optional[str],
    pusher_name: Optional[str] = None,
    build_id: Optional[str] = None,
    status: str = "Triggered",
):
    """Sends a build status notification embed to the configured Discord webhook.

    Constructs a Discord embed message with details about the build event (repository,
    branch, commit, status, trigger source) and sends it to the webhook URL specified
    in the application settings.

    Args:
        repo_full_name (str): The full name of the repository (e.g., 'owner/repo').
        branch_name (str): The name of the branch involved.
        commit_sha (Optional[str]): The Git commit SHA (shortened for display).
        commit_message (Optional[str]): The Git commit message (truncated if long).
        pusher_name (Optional[str]): The name of the user who pushed the commit (if applicable).
                                     Defaults to 'System/Manual' if not provided.
        build_id (Optional[str]): The unique ID of the build process.
        status (str): The current status of the build (e.g., 'Triggered', 'Running',
                      'Success', 'Failed', 'Received Push'). Determines embed color.
    """

    webhook_url = settings.DISCORD_WEBHOOK_URL
    if not webhook_url:
        logger.warning("DISCORD_WEBHOOK_URL not configured. Skipping notification.")
        return

    logger.info(
        f"Preparing Discord notification for {repo_full_name} - Status: {status}"
    )

    color_map = {
        "success": 0x2ECC71,  # Green
        "failed": 0xE74C3C,  # Red
        "running": 0x3498DB,  # Blue
        "pending": 0xFEE75C,  # Yellow
        "received push": 0x99AAB5,  # Grey
        "triggered": 0x5865F2,  # Default blurple
    }
    color = color_map.get(status.lower(), 0x5865F2)  # Default to blurple

    short_commit_sha = commit_sha[:7] if commit_sha else "N/A"
    display_pusher = pusher_name or "System/Manual"

    truncated_commit_message = "N/A"
    if commit_message:
        truncated_commit_message = (
            (commit_message[:100] + "...")
            if len(commit_message) > 100
            else commit_message
        )

    embed = {
        "title": f"🚀 Build {status}: {repo_full_name}",
        "color": color,
        "fields": [
            {"name": "Branch", "value": f"`{branch_name}`", "inline": True},
            {"name": "Commit", "value": f"`{short_commit_sha}`", "inline": True},
            {"name": "Triggered By", "value": display_pusher, "inline": True},
        ],
        "footer": {"text": f"Cypher Build System | Build ID: {build_id or 'N/A'}"},
        "timestamp": datetime.utcnow().isoformat(),
    }

    if commit_message and commit_message != "N/A":
        embed["fields"].append(
            {
                "name": "Commit Message",
                "value": truncated_commit_message,
                "inline": False,
            }
        )

    if commit_sha and "/" in repo_full_name:
        commit_url = f"https://github.com/{repo_full_name}/commit/{commit_sha}"
        embed["fields"].append(
            {
                "name": "Commit URL",
                "value": f"[View Commit]({commit_url})",
                "inline": False,
            }
        )

    if build_id and settings.FRONTEND_URL:
        base_frontend_url = settings.FRONTEND_URL.rstrip("/")
        build_url = f"{base_frontend_url}/builds/{build_id}"
        embed["fields"].append(
            {
                "name": "Build URL",
                "value": f"[View Build Details]({build_url})",
                "inline": False,
            }
        )

    payload = {"username": "Cypher Build Bot", "embeds": [embed]}

    logger.debug(f"Discord payload prepared: {payload}")

    try:
        logger.info(
            f"Attempting to send notification via requests.post to {webhook_url[:30]}..."
        )
        response = requests.post(webhook_url, json=payload, timeout=15)
        logger.info(
            f"Discord notification request completed. Status Code: {response.status_code}"
        )
        response.raise_for_status()
        logger.info(
            f"Successfully sent Discord build notification for {repo_full_name} @ {branch_name} (Status: {status})"
        )
    except requests.exceptions.Timeout:
        logger.error(
            f"Timeout sending Discord notification for {repo_full_name}.", exc_info=True
        )
    except requests.exceptions.RequestException as e:
        logger.error(
            f"Failed to send Discord notification for {repo_full_name}: {e}",
            exc_info=True,
        )
    except Exception as e:
        logger.error(
            f"Unexpected error sending Discord notification: {e}", exc_info=True
        )
