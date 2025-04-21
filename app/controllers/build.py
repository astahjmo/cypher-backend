import logging
import asyncio
import json
import os
import tempfile
import shutil
from fastapi import Request, BackgroundTasks
from sse_starlette.sse import EventSourceResponse
from typing import List, Dict, Any, Optional, AsyncGenerator
from bson import ObjectId
import git

from models.build.db_models import BuildStatus, BuildLog
from models.auth.db_models import User
from models.github.db_models import RepositoryConfig
from models.base import PyObjectId
from models.build.api_models import TriggerBuildRequestView
from services.docker_build_service import docker_build_service
from services.registry_service import registry_service, are_registry_credentials_set
from repositories.build_status_repository import BuildStatusRepository
from repositories.build_log_repository import BuildLogRepository
from repositories.repository_config_repository import RepositoryConfigRepository
from services.notification_service import send_discord_build_notification

logger = logging.getLogger(__name__)

TEMP_LOG_DIR = tempfile.mkdtemp(prefix="cypher_build_logs_")
logger.info(f"Temporary build log directory created at: {TEMP_LOG_DIR}")

sse_connections: Dict[str, List[asyncio.Queue]] = {}
sse_connections_lock = asyncio.Lock()


async def send_log_update(build_id: str, message: str):
    """Envia uma mensagem de log para todos os clientes SSE conectados para um build específico.

    Args:
        build_id (str): O ID do build para o qual enviar a mensagem.
        message (str): A mensagem de log a ser enviada.
    """
    logger.debug(
        f"SSE Log Callback: Received message for build {build_id}: '{message[:100]}...'"
    )
    async with sse_connections_lock:
        if build_id in sse_connections:
            queues = sse_connections[build_id]
            if queues:
                logger.debug(
                    f"SSE Log Callback: Found {len(queues)} active queue(s) for build {build_id}. Putting message..."
                )
                tasks = [queue.put(message) for queue in queues]
                try:
                    await asyncio.gather(*tasks)
                    logger.debug(
                        f"SSE Log Callback: Successfully put message onto queue(s) for build {build_id}."
                    )
                except Exception as e:
                    logger.error(
                        f"SSE Log Callback: Error putting message onto queue for build {build_id}: {e}",
                        exc_info=True,
                    )
            else:
                logger.warning(
                    f"SSE Log Callback: No active queues/clients found for build {build_id} when trying to send log: {message[:100]}..."
                )
        else:
            logger.warning(
                f"SSE Log Callback: build_id {build_id} not found in sse_connections dict."
            )


async def send_build_complete(build_id: str, final_status: str):
    """Envia um evento de conclusão de build para todos os clientes SSE conectados.

    Args:
        build_id (str): O ID do build que foi concluído.
        final_status (str): O status final do build ('success' ou 'failed').
    """
    logger.debug(
        f"SSE Complete Callback: Sending BUILD_COMPLETE:{final_status} for build {build_id}"
    )
    async with sse_connections_lock:
        if build_id in sse_connections:
            queues = sse_connections[build_id]
            if queues:
                logger.debug(
                    f"SSE Complete Callback: Found {len(queues)} active queue(s) for build {build_id}. Putting message..."
                )
                tasks = [
                    queue.put(f"BUILD_COMPLETE:{final_status}") for queue in queues
                ]
                try:
                    await asyncio.gather(*tasks)
                    logger.debug(
                        f"SSE Complete Callback: Successfully put BUILD_COMPLETE onto queue(s) for build {build_id}."
                    )
                except Exception as e:
                    logger.error(
                        f"SSE Complete Callback: Error putting BUILD_COMPLETE onto queue for build {build_id}: {e}",
                        exc_info=True,
                    )

            else:
                logger.warning(
                    f"SSE Complete Callback: No active queues/clients found for build {build_id} when trying to send BUILD_COMPLETE."
                )
        else:
            logger.warning(
                f"SSE Complete Callback: build_id {build_id} not found in sse_connections dict."
            )


async def cleanup_sse_connection(build_id: str, queue: asyncio.Queue):
    """Remove uma fila da lista de conexões quando um cliente SSE desconecta.

    Args:
        build_id (str): O ID do build associado à conexão.
        queue (asyncio.Queue): A fila específica do cliente a ser removida.
    """
    async with sse_connections_lock:
        if build_id in sse_connections:
            try:
                sse_connections[build_id].remove(queue)
                logger.info(
                    f"SSE Cleanup: Client disconnected for build {build_id}. Remaining clients: {len(sse_connections[build_id])}"
                )
                if not sse_connections[build_id]:
                    del sse_connections[build_id]
                    logger.info(
                        f"SSE Cleanup: Removed build ID {build_id} from SSE connections (no clients left)."
                    )
            except ValueError:
                logger.debug(
                    f"SSE Cleanup: Queue for build {build_id} already removed."
                )
                pass


async def run_build_and_log(
    build_id: str,
    repo_url: str,
    branch: str,
    tag_version: str,
    repo_full_name: str,
    build_status_repo: BuildStatusRepository,
    build_log_repo: BuildLogRepository,
    user_id: PyObjectId,
):
    """Executa o processo de build do Docker em segundo plano, registra a saída e atualiza o status.

    Clona o repositório, executa o build do Docker, opcionalmente faz push para um registro,
    envia atualizações de log via SSE, persiste os logs no banco de dados e envia notificações.

    Args:
        build_id (str): O ID único para este build.
        repo_url (str): A URL do repositório Git a ser clonado.
        branch (str): O branch do repositório a ser buildado.
        tag_version (str): A tag a ser usada para a imagem Docker (ex: 'latest', 'v1.0').
        repo_full_name (str): O nome completo do repositório (formato 'owner/repo').
        build_status_repo (BuildStatusRepository): Instância do repositório para atualizar o status do build.
        build_log_repo (BuildLogRepository): Instância do repositório para salvar os logs do build.
        user_id (PyObjectId): O ID do usuário que iniciou o build.
    """
    log_file_path = os.path.join(TEMP_LOG_DIR, f"{build_id}.log")
    final_status = "pending"
    final_image_tag = None
    commit_sha = None
    commit_message = None
    temp_dir = None

    try:
        if not isinstance(user_id, ObjectId):
            logger.warning(
                f"run_build_and_log received user_id as {type(user_id)}, converting to ObjectId."
            )
            user_id = PyObjectId(user_id)

        await send_log_update(
            build_id, f"Build process initiated for {repo_full_name} branch {branch}..."
        )

        temp_dir = tempfile.mkdtemp(prefix=f"cypher_build_{build_id}_")
        logger.info(f"Created temporary directory for cloning: {temp_dir}")
        await send_log_update(
            build_id, f"Cloning repository {repo_url} branch {branch}..."
        )
        try:
            cloned_repo = await asyncio.to_thread(
                git.Repo.clone_from, repo_url, temp_dir, branch=branch, depth=1
            )
            logger.info(f"Successfully cloned repository for build {build_id}")
            await send_log_update(build_id, "Repository cloned successfully.")

            head_commit = cloned_repo.head.commit
            commit_sha = head_commit.hexsha
            commit_message = head_commit.message.strip()
            logger.info(
                f"Fetched commit info: SHA={commit_sha[:7]}, Message='{commit_message[:50]}...'"
            )
            await send_log_update(
                build_id, f"Using commit: {commit_sha[:7]} ('{commit_message[:50]}...')"
            )

        except git.GitCommandError as e:
            logger.error(
                f"Git clone failed for {repo_url} branch {branch}: {e}", exc_info=True
            )
            error_msg = f"ERROR: Git clone failed: {e.stderr}"
            await send_log_update(build_id, error_msg)
            with open(log_file_path, "a") as log_file:
                log_file.write(error_msg + "\n")
            final_status = "failed"
            build_status_repo.update_status(
                build_id, final_status, message="Git clone failed."
            )
            raise

        logger.info(
            f"Updating build status to 'running' with commit info for build {build_id}"
        )
        build_status_repo.update_status(
            build_id, "running", commit_sha=commit_sha, commit_message=commit_message
        )
        await send_log_update(build_id, "Starting Docker build...")

        success, _, image_id, final_image_tag = (
            await docker_build_service.build_image_from_git(
                repo_url=repo_url,
                branch=branch,
                tag_version=tag_version,
                repo_full_name=repo_full_name,
                build_id=build_id,
                log_file_path=log_file_path,
                log_callback=lambda log_line: send_log_update(build_id, log_line),
            )
        )

        if success and final_image_tag:
            await send_log_update(
                build_id, f"Build successful. Image created: {final_image_tag}"
            )
            push_successful = True
            if are_registry_credentials_set():
                await send_log_update(
                    build_id, f"Pushing image {final_image_tag} to registry..."
                )
                push_success, push_logs = await asyncio.to_thread(
                    registry_service.push_image, final_image_tag
                )
                for line in push_logs.splitlines():
                    await send_log_update(build_id, f"[Push] {line}")
                if push_success:
                    await send_log_update(build_id, "Image push successful.")
                    final_status = "success"
                else:
                    await send_log_update(build_id, "ERROR: Image push failed.")
                    final_status = "failed"
            else:
                await send_log_update(
                    build_id, "Registry not configured, skipping push."
                )
                final_status = "success"
        else:
            await send_log_update(build_id, "ERROR: Build process failed.")
            final_status = "failed"

    except Exception as e:
        logger.error(
            f"Error during build process for build {build_id}: {e}", exc_info=True
        )
        try:
            await send_log_update(
                build_id, f"ERROR: An unexpected error occurred during the build: {e}"
            )
        except Exception as sse_e:
            logger.error(
                f"Failed to send build error via SSE for build {build_id}: {sse_e}"
            )
        final_status = "failed"
    finally:
        logger.info(f"Build {build_id} final status: {final_status}")
        build_status_repo.update_status(
            build_id,
            final_status,
            image_tag=final_image_tag if final_status == "success" else None,
        )
        await send_log_update(
            build_id, f"--- Build finished (Status: {final_status}) ---"
        )
        await send_build_complete(build_id, final_status)

        logger.info(
            f"Sending final Discord notification for build {build_id} with status {final_status}"
        )
        asyncio.create_task(
            asyncio.to_thread(
                send_discord_build_notification,
                repo_full_name=repo_full_name,
                branch_name=branch,
                commit_sha=commit_sha,
                commit_message=commit_message,
                pusher_name=None,
                build_id=build_id,
                status=final_status.capitalize(),
            )
        )

        try:
            if os.path.exists(log_file_path):
                with open(log_file_path, "r") as f:
                    log_entries = []
                    for line in f:
                        log_type = "log"
                        if "ERROR:" in line.upper():
                            log_type = "error"
                        elif "WARNING:" in line.upper():
                            log_type = "warning"
                        log_entries.append(
                            BuildLog(
                                build_id=ObjectId(build_id),
                                message=line.strip(),
                                type=log_type,
                            )
                        )
                    if log_entries:
                        await asyncio.to_thread(build_log_repo.insert_many, log_entries)
                        logger.info(
                            f"Persisted {len(log_entries)} log entries for build {build_id}"
                        )
            else:
                logger.warning(
                    f"Temporary log file not found for build {build_id}: {log_file_path}"
                )
        except Exception as e:
            logger.error(
                f"Error persisting logs for build {build_id}: {e}", exc_info=True
            )

        if temp_dir and os.path.exists(temp_dir):
            try:
                await asyncio.to_thread(shutil.rmtree, temp_dir)
                logger.info(f"Removed temporary directory: {temp_dir}")
            except Exception as e:
                logger.error(
                    f"Failed to remove temporary directory {temp_dir}: {e}",
                    exc_info=True,
                )


async def handle_docker_build_trigger(
    repo_owner: str,
    repo_name: str,
    branch: str,
    background_tasks: BackgroundTasks,
    payload: Optional[TriggerBuildRequestView],
    current_user: User,
    repo_config_repo: RepositoryConfigRepository,
    build_status_repo: BuildStatusRepository,
    build_log_repo: BuildLogRepository,
) -> Dict[str, str]:
    """Processa a solicitação para iniciar um build de imagem Docker.

    Valida a configuração do repositório, cria um registro de status de build inicial
    e agenda a tarefa de build em segundo plano.

    Args:
        repo_owner (str): O proprietário do repositório GitHub.
        repo_name (str): O nome do repositório GitHub.
        branch (str): O branch a ser buildado.
        background_tasks (BackgroundTasks): Objeto FastAPI para agendar tarefas em segundo plano.
        payload (Optional[TriggerBuildRequestView]): Dados opcionais da requisição (ex: tag).
        current_user (User): O usuário autenticado que está iniciando o build.
        repo_config_repo (RepositoryConfigRepository): Repositório para buscar configurações.
        build_status_repo (BuildStatusRepository): Repositório para criar/atualizar status do build.
        build_log_repo (BuildLogRepository): Repositório para salvar logs (passado para a task).

    Returns:
        Dict[str, str]: Um dicionário contendo uma mensagem de sucesso e o ID do build iniciado.

    Raises:
        ValueError: Se a configuração do repositório não for encontrada.
        TypeError: Se o ID do usuário estiver em um formato inválido.
    """
    repo_full_name = f"{repo_owner}/{repo_name}"
    tag_version = payload.tag_version if payload and payload.tag_version else "latest"

    logger.info(
        f"Controller: Build trigger request for {repo_full_name}, branch '{branch}', tag '{tag_version}' by user {current_user.login}"
    )

    if not isinstance(current_user.id, ObjectId):
        logger.error(
            f"CRITICAL: current_user.id is not ObjectId in handle_docker_build_trigger. Type: {type(current_user.id)}"
        )
        try:
            user_object_id = PyObjectId(current_user.id)
        except Exception:
            raise TypeError("Invalid user ID format received.")
    else:
        user_object_id = current_user.id

    repo_config = await asyncio.to_thread(
        repo_config_repo.find_by_repo_and_user, repo_full_name, user_object_id
    )
    if not repo_config:
        logger.warning(
            f"Controller: Repository config not found for {repo_full_name} and user {current_user.login}"
        )
        raise ValueError("Repository configuration not found.")

    repo_url = f"https://github.com/{repo_full_name}.git"
    logger.debug(f"Controller: Using repository URL: {repo_url}")

    build_status = BuildStatus(
        user_id=str(user_object_id),
        repo_full_name=repo_full_name,
        branch=branch,
        status="pending",
        commit_sha=None,
        commit_message=None,
        image_tag=None,
    )
    created_build = await asyncio.to_thread(build_status_repo.create, build_status)
    build_id = str(created_build.id)
    logger.info(f"Controller: Created pending build record with ID: {build_id}")

    background_tasks.add_task(
        run_build_and_log,
        build_id=build_id,
        repo_url=repo_url,
        branch=branch,
        tag_version=tag_version,
        repo_full_name=repo_full_name,
        build_status_repo=build_status_repo,
        build_log_repo=build_log_repo,
        user_id=user_object_id,
    )
    logger.info(f"Controller: Scheduled background build task for build ID: {build_id}")

    return {"message": "Build triggered successfully.", "build_id": build_id}


async def get_build_statuses(
    user_id: Any, repo: BuildStatusRepository, limit: int
) -> List[BuildStatus]:
    """Busca os status de build recentes para um usuário.

    Args:
        user_id (Any): O ID do usuário (será convertido internamente se necessário).
        repo (BuildStatusRepository): O repositório de status de build.
        limit (int): O número máximo de status a serem retornados.

    Returns:
        List[BuildStatus]: Uma lista de objetos BuildStatus (modelo do DB).

    Raises:
        RuntimeError: Se ocorrer um erro ao buscar os status no banco de dados.
    """
    try:
        statuses = await asyncio.to_thread(
            repo.find_by_user, user_id=user_id, limit=limit
        )
        return statuses
    except Exception as e:
        logger.error(
            f"Controller: Error fetching build statuses for user {user_id}: {e}",
            exc_info=True,
        )
        raise RuntimeError("Failed to retrieve build statuses.") from e


async def get_builds_list(
    user_id: Any, repo: BuildStatusRepository
) -> List[BuildStatus]:
    """Busca todos os builds para um usuário.

    Args:
        user_id (Any): O ID do usuário (será convertido internamente se necessário).
        repo (BuildStatusRepository): O repositório de status de build.

    Returns:
        List[BuildStatus]: Uma lista de objetos BuildStatus (modelo do DB).

    Raises:
        RuntimeError: Se ocorrer um erro ao buscar os builds no banco de dados.
    """
    try:
        builds = await asyncio.to_thread(repo.find_by_user, user_id=user_id, limit=0)
        return builds
    except Exception as e:
        logger.error(
            f"Controller: Error fetching builds list for user {user_id}: {e}",
            exc_info=True,
        )
        raise RuntimeError("Failed to retrieve builds list.") from e


async def get_build_detail(
    build_id: str, user_id: Any, repo: BuildStatusRepository
) -> Optional[BuildStatus]:
    """Busca os detalhes de um build específico para um usuário.

    Args:
        build_id (str): O ID do build a ser buscado.
        user_id (Any): O ID do usuário (será convertido internamente se necessário).
        repo (BuildStatusRepository): O repositório de status de build.

    Returns:
        Optional[BuildStatus]: O objeto BuildStatus se encontrado e acessível, senão None.

    Raises:
        ValueError: Se o formato do build_id for inválido.
        RuntimeError: Se ocorrer um erro ao buscar o build no banco de dados.
    """
    try:
        build = await asyncio.to_thread(repo.get_by_id_and_user, build_id, user_id)
        return build
    except ValueError:
        raise ValueError("Invalid Build ID format.")
    except Exception as e:
        logger.error(
            f"Controller: Error fetching build detail for ID {build_id}, user {user_id}: {e}",
            exc_info=True,
        )
        raise RuntimeError("Failed to retrieve build details.") from e


async def get_historical_build_logs(
    build_id: str,
    user_id: Any,
    log_repo: BuildLogRepository,
    status_repo: BuildStatusRepository,
) -> List[BuildLog]:
    """Busca os logs históricos persistidos para um build específico.

    Primeiro verifica se o usuário tem acesso ao build e depois busca os logs.

    Args:
        build_id (str): O ID do build cujos logs devem ser buscados.
        user_id (Any): O ID do usuário (será convertido internamente se necessário).
        log_repo (BuildLogRepository): O repositório de logs de build.
        status_repo (BuildStatusRepository): O repositório de status para verificação de acesso.

    Returns:
        List[BuildLog]: Uma lista de objetos BuildLog (modelo do DB).

    Raises:
        ValueError: Se o build não for encontrado, não for acessível ou o ID for inválido.
        RuntimeError: Se ocorrer um erro ao buscar os logs no banco de dados.
    """
    try:
        build_status = await asyncio.to_thread(
            status_repo.get_by_id_and_user, build_id, user_id
        )
        if not build_status:
            raise ValueError("Build not found or not accessible.")

        logs = await asyncio.to_thread(log_repo.find_by_build_id, build_id)
        return logs
    except ValueError as e:
        raise e
    except Exception as e:
        logger.error(
            f"Controller: Error fetching historical logs for build {build_id}, user {user_id}: {e}",
            exc_info=True,
        )
        raise RuntimeError("Failed to retrieve build logs.") from e


async def stream_build_logs(
    build_id: str, request: Request
) -> AsyncGenerator[Dict[str, str], None]:
    """Gera eventos Server-Sent Events (SSE) para logs de build em tempo real.

    Mantém uma fila para cada cliente conectado a um build específico e envia
    mensagens de log ou um evento de conclusão quando disponíveis.

    Args:
        build_id (str): O ID do build cujos logs devem ser transmitidos.
        request (Request): O objeto da requisição FastAPI para verificar desconexões.

    Yields:
        Dict[str, str]: Um dicionário representando um evento SSE, contendo 'event'
                        ('message' ou 'BUILD_COMPLETE') e 'data'.
    """
    queue = asyncio.Queue()
    async with sse_connections_lock:
        if build_id not in sse_connections:
            sse_connections[build_id] = []
        sse_connections[build_id].append(queue)
        logger.info(
            f"SSE Stream: Client connected for build {build_id}. Total clients for build: {len(sse_connections[build_id])}"
        )

    try:
        while True:
            disconnected = await request.is_disconnected()
            if disconnected:
                logger.info(
                    f"SSE Stream: Client disconnected (detected by request) for build {build_id}."
                )
                break

            try:
                message = await asyncio.wait_for(queue.get(), timeout=1.0)
                logger.debug(
                    f"SSE Stream: Got message from queue for build {build_id}: {message[:100]}..."
                )
                if message.startswith("BUILD_COMPLETE:"):
                    status = message.split(":", 1)[1]
                    logger.info(
                        f"SSE Stream: Yielding BUILD_COMPLETE event for build {build_id}, status: {status}"
                    )
                    yield {"event": "BUILD_COMPLETE", "data": status}
                    break
                else:
                    yield {"event": "message", "data": message}
                queue.task_done()
            except asyncio.TimeoutError:
                continue
            except Exception as e:
                logger.error(
                    f"SSE Stream: Error getting message from queue for build {build_id}: {e}",
                    exc_info=True,
                )
                break

    except asyncio.CancelledError:
        logger.info(f"SSE Stream: Generator cancelled for build {build_id}.")
    except Exception as e:
        logger.error(
            f"SSE Stream: Unexpected error in generator for build {build_id}: {e}",
            exc_info=True,
        )
    finally:
        logger.debug(f"SSE Stream: Cleaning up connection for build {build_id}")
        await cleanup_sse_connection(build_id, queue)
        logger.info(f"SSE Stream: Finished for build {build_id}")
