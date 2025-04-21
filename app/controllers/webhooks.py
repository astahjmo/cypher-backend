import logging
import asyncio
from fastapi import Request, Header, HTTPException, status, Depends, BackgroundTasks
from typing import Dict, Any, Optional, List
import hmac
import hashlib
from datetime import datetime
import json

from models.auth.db_models import User
from models.github.db_models import RepositoryConfig
from config import settings
from repositories.repository_config_repository import RepositoryConfigRepository
from repositories.repository_config_repository import get_repo_config_repository
from repositories.build_status_repository import (
    get_build_status_repository,
    BuildStatusRepository,
)
from repositories.build_log_repository import (
    get_build_log_repository,
    BuildLogRepository,
)
from controllers.build import handle_docker_build_trigger
from services.notification_service import send_discord_build_notification
from services.db_service import db_service

logger = logging.getLogger(__name__)


async def _process_webhook_background(
    payload: Dict[str, Any],
    repo_config_repo: RepositoryConfigRepository,
    build_status_repo: BuildStatusRepository,
    build_log_repo: BuildLogRepository,
):
    """Processa o payload do webhook GitHub (evento push) em segundo plano.

    Extrai informações relevantes do payload, encontra configurações de repositório
    correspondentes que têm o build automático habilitado para o branch específico,
    e dispara os builds necessários usando `handle_docker_build_trigger`.
    Também envia notificações via Discord.

    Args:
        payload (Dict[str, Any]): O payload JSON desserializado do webhook GitHub.
        repo_config_repo (RepositoryConfigRepository): Instância do repositório de configuração.
        build_status_repo (BuildStatusRepository): Instância do repositório de status de build.
        build_log_repo (BuildLogRepository): Instância do repositório de logs de build.
    """
    logger.info("Background Task: Starting webhook processing.")
    repo_full_name = None
    branch_name = None
    commit_sha = None

    try:
        repo_data = payload.get("repository", {})
        repo_full_name = repo_data.get("full_name")
        ref = payload.get("ref", "")
        commit_sha = payload.get("after")
        pusher_name = payload.get("pusher", {}).get("name", "Unknown Pusher")
        head_commit = payload.get("head_commit", {})
        commit_message = (
            head_commit.get("message", "No commit message")
            if head_commit
            else "No commit message"
        )
        branch_name = ref.split("refs/heads/")[-1]

        logger.info(
            f"Background Task: Processing push for {repo_full_name}, branch {branch_name}, commit {commit_sha[:7]}"
        )

        matching_configs: List[RepositoryConfig] = []
        try:
            all_configs = await asyncio.to_thread(
                repo_config_repo.find_by_name, repo_full_name=repo_full_name
            )
            if isinstance(all_configs, list):
                matching_configs = [
                    config
                    for config in all_configs
                    if config.repo_full_name == repo_full_name
                    and branch_name in config.auto_build_branches
                ]
            elif (
                all_configs
                and all_configs.repo_full_name == repo_full_name
                and branch_name in all_configs.auto_build_branches
            ):
                matching_configs.append(all_configs)

            if not matching_configs:
                logger.info(
                    f"Background Task: No matching auto-build configuration found for {repo_full_name} branch {branch_name}."
                )
                return
            logger.info(
                f"Background Task: Found {len(matching_configs)} matching configurations for {repo_full_name} branch {branch_name}."
            )

        except Exception as e:
            logger.error(
                f"Background Task: Database error finding repository configurations for {repo_full_name}: {e}",
                exc_info=True,
            )
            return

        final_notification_tasks = []

        for config in matching_configs:
            build_id_for_notification = None
            try:
                logger.info(
                    f"Background Task: Checking auto-build for config {config.id}, user {config.user_id}"
                )
                user_for_build = User(
                    id=config.user_id, github_id=0, login="webhook_trigger"
                )

                build_background_tasks = BackgroundTasks()

                build_result = await handle_docker_build_trigger(
                    repo_owner=repo_full_name.split("/")[0],
                    repo_name=repo_full_name.split("/")[1],
                    branch=branch_name,
                    background_tasks=build_background_tasks,
                    payload=None,
                    current_user=user_for_build,
                    repo_config_repo=repo_config_repo,
                    build_status_repo=build_status_repo,
                    build_log_repo=build_log_repo,
                )
                build_id_for_notification = build_result.get("build_id", "unknown")
                logger.info(
                    f"Background Task: Successfully triggered build {build_id_for_notification} for user {config.user_id} config {config.id}"
                )

                await build_background_tasks()

                triggered_notification_task = asyncio.to_thread(
                    send_discord_build_notification,
                    repo_full_name=repo_full_name,
                    branch_name=branch_name,
                    commit_sha=commit_sha,
                    commit_message=commit_message,
                    pusher_name=pusher_name,
                    build_id=build_id_for_notification,
                    status="Build Triggered",
                )
                final_notification_tasks.append(triggered_notification_task)

            except Exception as e:
                logger.error(
                    f"Background Task: Failed to trigger build for user {config.user_id} config {config.id}: {e}",
                    exc_info=True,
                )

        if final_notification_tasks:
            try:
                await asyncio.gather(*final_notification_tasks)
                logger.info(
                    f"Background Task: Finished processing {len(final_notification_tasks)} 'Build Triggered' Discord notifications."
                )
            except Exception as gather_exc:
                logger.error(
                    f"Background Task: Error waiting for 'Build Triggered' Discord notification tasks: {gather_exc}",
                    exc_info=True,
                )

        logger.info(
            f"Background Task: Finished processing webhook for {repo_full_name} branch {branch_name}."
        )

    except Exception as e:
        logger.error(
            f"Background Task: Unhandled error during webhook processing: {e}",
            exc_info=True,
        )


async def handle_github_push_webhook(
    request: Request,
    background_tasks: BackgroundTasks,
    x_hub_signature_256: Optional[str],
    repo_config_repo: RepositoryConfigRepository = Depends(get_repo_config_repository),
    build_status_repo: BuildStatusRepository = Depends(get_build_status_repository),
    build_log_repo: BuildLogRepository = Depends(get_build_log_repository),
):
    """Valida uma requisição de webhook GitHub (evento push) e agenda o processamento em segundo plano.

    Verifica a assinatura HMAC-SHA256 usando o segredo configurado, analisa o payload JSON,
    verifica se é um evento de push para um branch e, se válido, agenda a função
    `_process_webhook_background` para execução.

    Args:
        request (Request): O objeto da requisição FastAPI.
        background_tasks (BackgroundTasks): Objeto FastAPI para agendar tarefas.
        x_hub_signature_256 (Optional[str]): O cabeçalho da assinatura enviado pelo GitHub.
        repo_config_repo (RepositoryConfigRepository): Instância injetada do repositório de configuração.
        build_status_repo (BuildStatusRepository): Instância injetada do repositório de status de build.
        build_log_repo (BuildLogRepository): Instância injetada do repositório de logs de build.

    Raises:
        ValueError: Se o segredo do webhook não estiver configurado, a assinatura estiver
                    ausente ou inválida, o payload JSON for inválido, ou detalhes básicos
                    do payload não puderem ser analisados.
        RuntimeError: Se ocorrer um erro inesperado durante a verificação da assinatura.
    """
    logger.info("Controller: Received GitHub push webhook request.")

    if not settings.GITHUB_WEBHOOK_SECRET:
        logger.error("Controller: GITHUB_WEBHOOK_SECRET not configured.")
        raise ValueError("Webhook secret not configured.")
    if not x_hub_signature_256:
        logger.warning("Controller: Missing X-Hub-Signature-256 header.")
        raise ValueError("Missing signature header.")

    try:
        body = await request.body()
        signature = hmac.new(
            settings.GITHUB_WEBHOOK_SECRET.encode("utf-8"), body, hashlib.sha256
        ).hexdigest()
        expected_signature = f"sha256={signature}"
        if not hmac.compare_digest(expected_signature, x_hub_signature_256):
            logger.error("Controller: Invalid webhook signature.")
            raise ValueError("Invalid signature.")
        logger.info("Controller: Webhook signature verified successfully.")
    except ValueError as e:
        raise e
    except Exception as e:
        logger.error(
            f"Controller: Error during webhook signature verification: {e}",
            exc_info=True,
        )
        raise RuntimeError("Webhook signature verification failed.") from e

    try:
        payload = json.loads(body.decode("utf-8"))
    except Exception as e:
        logger.error(
            f"Controller: Failed to parse webhook JSON payload: {e}", exc_info=True
        )
        raise ValueError("Invalid JSON payload.") from e

    try:
        event_type = request.headers.get("X-GitHub-Event")
        if event_type != "push":
            logger.info(f"Controller: Ignoring non-push event: {event_type}")
            return

        ref = payload.get("ref", "")
        commit_sha = payload.get("after")

        if (
            not ref.startswith("refs/heads/")
            or not commit_sha
            or commit_sha == "0000000000000000000000000000000000000000"
        ):
            logger.info(
                f"Controller: Ignoring event (not branch push or branch deletion): ref={ref}, commit={commit_sha}"
            )
            return
    except Exception as e:
        logger.error(
            f"Controller: Error during basic payload check: {e}", exc_info=True
        )
        raise ValueError("Could not parse basic webhook payload details.") from e

    logger.info("Controller: Scheduling background processing for valid push event.")
    background_tasks.add_task(
        _process_webhook_background,
        payload=payload,
        repo_config_repo=repo_config_repo,
        build_status_repo=build_status_repo,
        build_log_repo=build_log_repo,
    )

    logger.info("Controller: Background task scheduled. Returning acceptance to view.")
