import logging
import asyncio
import json
from fastapi import WebSocket, WebSocketDisconnect, Request
from starlette.websockets import WebSocketState
from typing import List, Set, Dict, Optional, Any, AsyncGenerator
from docker.errors import DockerException, NotFound as DockerNotFound

from models.container.db_models import ContainerRuntimeConfig, PortMapping
from models.auth.db_models import User
from models.base import PyObjectId
from models.github.db_models import RepositoryConfig
from models.container.api_models import (
    ContainerStatusInfoView,
    ScaleRequestView,
    ScaleResponseView,
)
from services.docker_service import docker_service
from repositories.container_runtime_config_repository import (
    ContainerRuntimeConfigRepository,
)
from repositories.repository_config_repository import RepositoryConfigRepository
from repositories.build_status_repository import BuildStatusRepository

logger = logging.getLogger(__name__)


class ConnectionManager:
    """Gerencia conexões WebSocket ativas para broadcast de status."""

    def __init__(self):
        self.active_connections: Set[WebSocket] = set()
        self._lock = asyncio.Lock()

    async def connect(self, websocket: WebSocket):
        """Aceita uma nova conexão WebSocket e a adiciona ao conjunto de conexões ativas.

        Args:
            websocket (WebSocket): O objeto WebSocket da nova conexão.
        """
        await websocket.accept()
        async with self._lock:
            self.active_connections.add(websocket)
        logger.info(
            f"WebSocket connected: {websocket.client}. Total connections: {len(self.active_connections)}"
        )

    async def disconnect(self, websocket: WebSocket):
        """Remove uma conexão WebSocket do conjunto de conexões ativas.

        Args:
            websocket (WebSocket): O objeto WebSocket da conexão a ser removida.
        """
        async with self._lock:
            removed = websocket in self.active_connections
            self.active_connections.discard(websocket)
        if removed:
            logger.info(
                f"WebSocket explicitly disconnected and removed: {websocket.client}. Total connections: {len(self.active_connections)}"
            )

    async def broadcast(self, message: str):
        """Envia uma mensagem para todas as conexões WebSocket ativas.

        Args:
            message (str): A mensagem a ser enviada (geralmente JSON).
        """
        async with self._lock:
            if not self.active_connections:
                return
            connections_to_send = list(self.active_connections)

        if not connections_to_send:
            return

        tasks = [connection.send_text(message) for connection in connections_to_send]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        failed_send_count = 0
        for i, result in enumerate(results):
            connection = connections_to_send[i]
            if isinstance(result, Exception):
                failed_send_count += 1
                if isinstance(
                    result, RuntimeError
                ) and "Cannot call 'send' once a close message has been sent" in str(
                    result
                ):
                    logger.warning(
                        f"Send failed for WebSocket {connection.client} (already closed)."
                    )
                elif isinstance(result, WebSocketDisconnect):
                    logger.warning(
                        f"Send failed for WebSocket {connection.client} (disconnected)."
                    )
                else:
                    logger.error(
                        f"Error broadcasting to WebSocket {connection.client}: {type(result).__name__} - {result}",
                        exc_info=False,
                    )


manager = ConnectionManager()


async def broadcast_status_updates(repo_config_repo: RepositoryConfigRepository):
    """Tarefa em segundo plano que busca periodicamente o status dos contêineres e repositórios configurados
    e transmite a lista combinada para os clientes WebSocket conectados.

    Args:
        repo_config_repo (RepositoryConfigRepository): Instância do repositório para buscar configurações.
    """
    while True:
        connection_count = 0
        try:
            async with manager._lock:
                connection_count = len(manager.active_connections)

            if connection_count > 0:
                logger.debug(
                    "Broadcast task running: Fetching configured repos and container statuses..."
                )

                def get_combined_status_sync():
                    """Função síncrona interna para buscar dados do Docker e DB."""
                    final_status_list: List[Dict] = []
                    try:
                        all_docker_statuses_list = (
                            docker_service.list_managed_containers()
                        )
                        logger.debug(
                            f"Broadcast Sync: Found {len(all_docker_statuses_list)} docker statuses."
                        )
                        docker_status_map: Dict[str, Dict] = {
                            status_info["repo_full_name"]: status_info
                            for status_info in all_docker_statuses_list
                        }

                        all_configs = repo_config_repo.find_all()
                        logger.debug(
                            f"Broadcast Sync: Found {len(all_configs)} configured repos."
                        )
                        configured_repo_names = {
                            config.repo_full_name for config in all_configs
                        }

                        processed_repos = set()

                        for repo_name, docker_status in docker_status_map.items():
                            final_status_list.append(docker_status)
                            processed_repos.add(repo_name)

                        for repo_name in configured_repo_names:
                            if repo_name not in processed_repos:
                                status_dict = {
                                    "repo_full_name": repo_name,
                                    "running": 0,
                                    "stopped": 0,
                                    "paused": 0,
                                    "memory_usage_mb": 0.0,
                                    "cpu_usage_percent": 0.0,
                                    "containers": [],
                                }
                                final_status_list.append(status_dict)

                        logger.debug(
                            f"Broadcast Sync: Combined list size: {len(final_status_list)}"
                        )
                        return final_status_list
                    except Exception as e:
                        logger.error(
                            f"Error during synchronous status fetching for broadcast: {e}",
                            exc_info=True,
                        )
                        return []

                combined_status_list = await asyncio.to_thread(get_combined_status_sync)

                if combined_status_list is not None:
                    message = json.dumps(combined_status_list)
                    logger.debug(f"Broadcasting status update: {message[:200]}...")
                    await manager.broadcast(message)
                else:
                    logger.warning(
                        "Broadcast task: Failed to fetch combined status, skipping broadcast cycle."
                    )

        except Exception as e:
            logger.error(
                f"Error in broadcast_status_updates task loop: {e}", exc_info=True
            )
        finally:
            await asyncio.sleep(5)


async def get_container_status_list_logic(
    user_id: PyObjectId, repo_config_repo: RepositoryConfigRepository
) -> List[Dict]:
    """Busca a lista combinada de status de contêineres para os repositórios configurados por um usuário.

    Combina informações de contêineres Docker existentes com a lista de repositórios
    configurados pelo usuário.

    Args:
        user_id (PyObjectId): O ID do usuário autenticado.
        repo_config_repo (RepositoryConfigRepository): Instância do repositório de configuração.

    Returns:
        List[Dict]: Uma lista de dicionários, cada um representando o status agregado
                    de um repositório (compatível com ContainerStatusInfoView).

    Raises:
        RuntimeError: Se ocorrer um erro ao buscar informações do banco de dados ou Docker.
    """
    final_status_list: List[Dict] = []
    try:

        def get_data_sync():
            """Função síncrona interna para buscar dados do DB e Docker."""
            _configured_repos = repo_config_repo.find_by_user(user_id)
            _all_docker_statuses = docker_service.list_managed_containers()
            return _configured_repos, _all_docker_statuses

        configured_repos, all_docker_statuses_list = await asyncio.to_thread(
            get_data_sync
        )

        docker_status_map: Dict[str, Dict] = {
            status_info["repo_full_name"]: status_info
            for status_info in all_docker_statuses_list
        }

        for repo_config in configured_repos:
            repo_full_name = repo_config.repo_full_name
            docker_status = docker_status_map.get(repo_full_name)

            if docker_status:
                final_status_list.append(docker_status)
            else:
                final_status_list.append(
                    {
                        "repo_full_name": repo_full_name,
                        "running": 0,
                        "stopped": 0,
                        "paused": 0,
                        "memory_usage_mb": 0.0,
                        "cpu_usage_percent": 0.0,
                        "containers": [],
                    }
                )
        return final_status_list
    except Exception as e:
        logger.error(
            f"Controller: Error fetching container status for user {user_id}: {e}",
            exc_info=True,
        )
        raise RuntimeError(
            "Failed to retrieve repository/container status information."
        ) from e


async def get_container_runtime_config_logic(
    repo_full_name: str,
    user_id: PyObjectId,
    repo: ContainerRuntimeConfigRepository,
) -> Optional[ContainerRuntimeConfig]:
    """Busca a configuração de tempo de execução de contêiner para um repositório e usuário específicos.

    Args:
        repo_full_name (str): O nome completo do repositório ('owner/repo').
        user_id (PyObjectId): O ID do usuário proprietário da configuração.
        repo (ContainerRuntimeConfigRepository): Instância do repositório de configuração.

    Returns:
        Optional[ContainerRuntimeConfig]: O objeto de configuração se encontrado, senão None.

    Raises:
        RuntimeError: Se ocorrer um erro ao buscar a configuração no banco de dados.
    """
    try:
        config = await asyncio.to_thread(
            repo.get_by_repo_and_user, repo_full_name=repo_full_name, user_id=user_id
        )
        return config
    except Exception as e:
        logger.error(
            f"Controller: Error fetching runtime config for {repo_full_name}, user {user_id}: {e}",
            exc_info=True,
        )
        raise RuntimeError("Failed to retrieve container runtime configuration.") from e


async def save_container_runtime_config_logic(
    runtime_config_in: ContainerRuntimeConfig,
    repo_full_name: str,
    user_id: PyObjectId,
    repo: ContainerRuntimeConfigRepository,
) -> ContainerRuntimeConfig:
    """Salva (cria ou atualiza) a configuração de tempo de execução de contêiner.

    Args:
        runtime_config_in (ContainerRuntimeConfig): O objeto de configuração a ser salvo (modelo do DB).
        repo_full_name (str): O nome completo do repositório associado.
        user_id (PyObjectId): O ID do usuário proprietário da configuração.
        repo (ContainerRuntimeConfigRepository): Instância do repositório de configuração.

    Returns:
        ContainerRuntimeConfig: O objeto de configuração salvo (com ID atualizado se for criação).

    Raises:
        RuntimeError: Se ocorrer um erro ao salvar a configuração no banco de dados.
    """
    runtime_config_in.user_id = user_id
    runtime_config_in.repo_full_name = repo_full_name
    try:
        saved_config = await asyncio.to_thread(repo.upsert, config=runtime_config_in)
        return saved_config
    except Exception as e:
        logger.error(
            f"Controller: Error saving runtime config for {repo_full_name}, user {user_id}: {e}",
            exc_info=True,
        )
        raise RuntimeError("Failed to save container runtime configuration.") from e


async def start_container_logic(container_id: str):
    """Inicia um contêiner Docker específico pelo seu ID.

    Args:
        container_id (str): O ID do contêiner a ser iniciado.

    Raises:
        ValueError: Se o contêiner não for encontrado ou falhar ao iniciar.
        RuntimeError: Se ocorrer um erro inesperado ou um erro do Docker.
    """
    try:
        success = await asyncio.to_thread(docker_service.start_container, container_id)
        if not success:
            raise ValueError(f"Container {container_id} not found or failed to start.")
    except DockerException as e:
        logger.error(
            f"Controller: Docker error starting container {container_id}: {e}",
            exc_info=True,
        )
        raise RuntimeError(f"Docker error: {e}") from e
    except Exception as e:
        logger.error(
            f"Controller: Unexpected error starting container {container_id}: {e}",
            exc_info=True,
        )
        raise RuntimeError("An unexpected error occurred.") from e


async def stop_container_logic(container_id: str):
    """Para um contêiner Docker específico pelo seu ID.

    Args:
        container_id (str): O ID do contêiner a ser parado.

    Raises:
        ValueError: Se o contêiner não for encontrado ou falhar ao parar.
        RuntimeError: Se ocorrer um erro inesperado ou um erro do Docker.
    """
    try:
        success = await asyncio.to_thread(docker_service.stop_container, container_id)
        if not success:
            raise ValueError(f"Container {container_id} not found or failed to stop.")
    except DockerException as e:
        logger.error(
            f"Controller: Docker error stopping container {container_id}: {e}",
            exc_info=True,
        )
        raise RuntimeError(f"Docker error: {e}") from e
    except Exception as e:
        logger.error(
            f"Controller: Unexpected error stopping container {container_id}: {e}",
            exc_info=True,
        )
        raise RuntimeError("An unexpected error occurred.") from e


async def remove_container_logic(container_id: str):
    """Remove um contêiner Docker específico pelo seu ID.

    Args:
        container_id (str): O ID do contêiner a ser removido.

    Raises:
        ValueError: Se o contêiner não for encontrado ou falhar ao remover.
        RuntimeError: Se ocorrer um erro inesperado ou um erro do Docker.
    """
    try:
        success = await asyncio.to_thread(
            docker_service.remove_container, container_id, force=True
        )
        if not success:
            raise ValueError(f"Container {container_id} not found or failed to remove.")
    except DockerException as e:
        logger.error(
            f"Controller: Docker error removing container {container_id}: {e}",
            exc_info=True,
        )
        raise RuntimeError(f"Docker error: {e}") from e
    except Exception as e:
        logger.error(
            f"Controller: Unexpected error removing container {container_id}: {e}",
            exc_info=True,
        )
        raise RuntimeError("An unexpected error occurred.") from e


async def get_container_logs_logic(container_id: str, tail: int) -> str:
    """Busca os logs de um contêiner Docker específico.

    Args:
        container_id (str): O ID do contêiner cujos logs devem ser buscados.
        tail (int): O número de linhas finais a serem retornadas.

    Returns:
        str: Os logs do contêiner como uma string única.

    Raises:
        ValueError: Se o contêiner não for encontrado (erro DockerNotFound).
        RuntimeError: Se ocorrer um erro inesperado ou outro erro do Docker.
    """
    try:
        logs = await asyncio.to_thread(
            docker_service.get_container_logs, container_id, tail=tail
        )
        return logs
    except DockerNotFound as e:
        logger.warning(
            f"Controller: Logs requested for non-existent container {container_id}: {e}"
        )
        raise ValueError(str(e))
    except DockerException as e:
        logger.error(
            f"Controller: Docker error getting logs for container {container_id}: {e}",
            exc_info=True,
        )
        raise RuntimeError(f"Docker error: {e}") from e
    except Exception as e:
        logger.error(
            f"Controller: Unexpected error getting logs for container {container_id}: {e}",
            exc_info=True,
        )
        raise RuntimeError("An unexpected error occurred while fetching logs.") from e


async def scale_repository_logic(
    repo_full_name: str,
    desired_instances: int,
    user_id: PyObjectId,
    config_repo: ContainerRuntimeConfigRepository,
    build_status_repo: BuildStatusRepository,
) -> Dict[str, int]:
    """Escala o número de instâncias de contêiner para um repositório.

    Busca a configuração de tempo de execução e o último build bem-sucedido,
    depois chama o serviço Docker para ajustar o número de contêineres.

    Args:
        repo_full_name (str): O nome completo do repositório ('owner/repo').
        desired_instances (int): O número desejado de instâncias de contêiner.
        user_id (PyObjectId): O ID do usuário que está realizando a operação.
        config_repo (ContainerRuntimeConfigRepository): Repositório de configuração de runtime.
        build_status_repo (BuildStatusRepository): Repositório de status de build.

    Returns:
        Dict[str, int]: Um dicionário indicando quantos contêineres foram iniciados
                        e quantos foram removidos (ex: {"started": 2, "removed": 1}).

    Raises:
        ValueError: Se a configuração de runtime ou um build bem-sucedido não forem encontrados.
        RuntimeError: Se ocorrer um erro inesperado ou um erro do Docker durante o escalonamento.
    """
    try:

        def get_data_sync():
            """Função síncrona interna para buscar dados do DB."""
            _runtime_config = config_repo.get_by_repo_and_user(
                repo_full_name=repo_full_name, user_id=user_id
            )
            _latest_build = build_status_repo.find_latest_successful_build(
                repo_full_name=repo_full_name, user_id=user_id
            )
            return _runtime_config, _latest_build

        runtime_config, latest_build = await asyncio.to_thread(get_data_sync)

        if not runtime_config:
            raise ValueError(f"Runtime configuration not found for {repo_full_name}.")
        if not latest_build or not latest_build.image_tag:
            raise ValueError(
                f"No successful build with an image tag found for {repo_full_name}."
            )

        image_tag = latest_build.image_tag

        scale_result = await asyncio.to_thread(
            docker_service.scale_repository,
            repo_full_name=repo_full_name,
            image_tag=image_tag,
            runtime_config=runtime_config,
            desired_instances=desired_instances,
        )
        return scale_result
    except ValueError as e:
        raise e
    except DockerException as e:
        logger.error(
            f"Controller: Docker error scaling repository {repo_full_name}: {e}",
            exc_info=True,
        )
        raise RuntimeError(f"Docker scaling error: {e}") from e
    except Exception as e:
        logger.error(
            f"Controller: Unexpected error scaling repository {repo_full_name}: {e}",
            exc_info=True,
        )
        raise RuntimeError("An unexpected error occurred during scaling.") from e


async def list_docker_networks_logic() -> List[Dict[str, Any]]:
    """Lista as redes Docker disponíveis no host.

    Returns:
        List[Dict[str, Any]]: Uma lista de dicionários, cada um representando uma rede Docker.

    Raises:
        RuntimeError: Se ocorrer um erro inesperado ou um erro do Docker.
    """
    try:
        networks = await asyncio.to_thread(docker_service.list_networks)
        return networks
    except DockerException as e:
        logger.error(f"Controller: Docker error listing networks: {e}", exc_info=True)
        raise RuntimeError(f"Docker error: {e}") from e
    except Exception as e:
        logger.error(
            f"Controller: Unexpected error listing networks: {e}", exc_info=True
        )
        raise RuntimeError(
            "An unexpected error occurred while listing networks."
        ) from e
