import logging
from github import Github, GithubException
from typing import List, Dict, Any, Optional, Tuple
from bson import ObjectId
import asyncio
import functools

from repositories.repository_config_repository import RepositoryConfigRepository
from repositories.user_repository import UserRepository
from models.github.db_models import RepositoryConfig
from models.auth.db_models import User
from async_lru import alru_cache

logger = logging.getLogger(__name__)


@alru_cache(maxsize=128, ttl=300)
async def get_user_repositories(
    user_id: ObjectId, user_repo: UserRepository
) -> list[dict]:
    """Busca a lista de repositórios acessíveis por um usuário no GitHub.

    Utiliza o token de acesso GitHub do usuário armazenado para autenticar a requisição.
    Os resultados são cacheados por 5 minutos.

    Args:
        user_id (ObjectId): O ID do usuário no banco de dados.
        user_repo (UserRepository): Instância do repositório de usuários para buscar o token.

    Returns:
        list[dict]: Uma lista de dicionários, cada um contendo informações básicas
                    de um repositório (name, full_name, private, url).

    Raises:
        ValueError: Se o token GitHub do usuário não for encontrado.
        GithubException: Se ocorrer um erro na API do GitHub (ex: token inválido, rate limit).
        RuntimeError: Para erros inesperados durante a busca.
    """
    logger.info(f"Controller: Fetching repositories for user ID: {user_id}")
    token = user_repo.get_user_token_by_id(user_id)
    if not token:
        logger.error(f"Controller: GitHub token not found for user ID: {user_id}")
        raise ValueError("GitHub token not found for user.")

    token_preview = f"{token[:5]}...{token[-5:]}" if len(token) > 10 else token
    logger.info(
        f"Controller: Attempting to use token preview: {token_preview} for user ID: {user_id}"
    )

    try:

        def fetch_repos_sync():
            logger.debug(
                f"Controller: Executing blocking GitHub repo fetch in thread for user {user_id}"
            )
            g = Github(token)
            github_user = g.get_user()
            repos = github_user.get_repos(type="all", sort="updated", direction="desc")
            repo_list_sync = [
                {
                    "name": repo.name,
                    "full_name": repo.full_name,
                    "private": repo.private,
                    "url": repo.html_url,
                }
                for repo in repos
            ]
            logger.debug(
                f"Controller: Finished blocking GitHub repo fetch for user {user_id}"
            )
            return repo_list_sync

        repo_list = await asyncio.to_thread(fetch_repos_sync)
        logger.info(
            f"Controller: Fetched {len(repo_list)} repositories for user ID: {user_id}"
        )
        return repo_list
    except GithubException as e:
        logger.error(
            f"Controller: GitHub API error listing repositories for user ID {user_id} (token preview: {token_preview}): {e.status} - {e.data}",
            exc_info=False,
        )
        raise e
    except Exception as e:
        logger.error(
            f"Controller: Unexpected error listing repositories for user ID {user_id} (token preview: {token_preview}): {e}",
            exc_info=True,
        )
        raise RuntimeError(
            "An unexpected error occurred while fetching repositories."
        ) from e


async def get_repository_branches(
    owner: str, repo_name: str, user_id: ObjectId, user_repo: UserRepository
) -> list[dict]:
    """Busca a lista de branches para um repositório específico no GitHub.

    Utiliza o token de acesso GitHub do usuário para autenticar a requisição.

    Args:
        owner (str): O nome do proprietário (usuário ou organização) do repositório.
        repo_name (str): O nome do repositório.
        user_id (ObjectId): O ID do usuário no banco de dados.
        user_repo (UserRepository): Instância do repositório de usuários para buscar o token.

    Returns:
        list[dict]: Uma lista de dicionários, cada um contendo o nome de um branch.

    Raises:
        ValueError: Se o token GitHub do usuário não for encontrado, ou se o repositório
                    não for encontrado (404) ou acessível (401).
        GithubException: Se ocorrer outro erro na API do GitHub.
        RuntimeError: Para erros inesperados durante a busca ou erros genéricos da API.
    """
    repo_full_name = f"{owner}/{repo_name}"
    logger.info(
        f"Controller: Fetching branches for repository: {repo_full_name} for user ID: {user_id}"
    )

    token = user_repo.get_user_token_by_id(user_id)
    if not token:
        logger.error(
            f"Controller: GitHub token not found for user ID: {user_id} when fetching branches for {repo_full_name}"
        )
        raise ValueError("GitHub token not found for user.")

    token_preview = f"{token[:5]}...{token[-5:]}" if len(token) > 10 else token
    logger.info(
        f"Attempting to use token preview: {token_preview} for user ID: {user_id} to fetch branches for {repo_full_name}"
    )

    try:

        def fetch_branches_sync():
            logger.debug(
                f"Executing blocking GitHub branch fetch in thread for {repo_full_name}"
            )
            g = Github(token)
            repo = g.get_repo(repo_full_name)
            branches = repo.get_branches()
            branch_list_sync = [{"name": branch.name} for branch in branches]
            logger.debug(f"Finished blocking GitHub branch fetch for {repo_full_name}")
            return branch_list_sync

        branch_list = await asyncio.to_thread(fetch_branches_sync)
        logger.info(f"Fetched {len(branch_list)} branches for {repo_full_name}")
        return branch_list
    except GithubException as e:
        logger.error(
            f"GitHub API error fetching branches for {repo_full_name} by user ID {user_id} (token preview: {token_preview}): {e.status} - {e.data}",
            exc_info=True,
        )
        if e.status == 404:
            raise ValueError(
                f"Repository '{repo_full_name}' not found or not accessible."
            )
        if e.status == 401:
            raise ValueError(
                f"GitHub API error: Bad credentials accessing {repo_full_name}. Please re-authenticate."
            )
        raise RuntimeError(
            f"GitHub API error: {e.data.get('message', 'Unknown error')}"
        ) from e
    except Exception as e:
        logger.error(
            f"Unexpected error fetching branches for {repo_full_name} by user ID {user_id} (token preview: {token_preview}): {e}",
            exc_info=True,
        )
        raise RuntimeError(
            f"An unexpected error occurred while fetching branches for {repo_full_name}."
        ) from e


async def set_branch_configuration(
    owner: str,
    repo_name: str,
    branches_to_configure: list[str],
    repo_config_repo: RepositoryConfigRepository,
    user_repo: UserRepository,
    user_id: ObjectId,
) -> RepositoryConfig:
    """Verifica o acesso ao repositório e salva a configuração dos branches para build automático.

    Primeiro, verifica se o usuário tem acesso ao repositório usando seu token GitHub.
    Depois, salva ou atualiza a configuração no banco de dados. Invalida o cache da
    lista de repositórios do usuário após a atualização.

    Args:
        owner (str): O nome do proprietário do repositório.
        repo_name (str): O nome do repositório.
        branches_to_configure (list[str]): Lista dos nomes dos branches a serem configurados.
        repo_config_repo (RepositoryConfigRepository): Instância do repositório de configuração.
        user_repo (UserRepository): Instância do repositório de usuários para buscar o token.
        user_id (ObjectId): O ID do usuário que está configurando.

    Returns:
        RepositoryConfig: O objeto de configuração salvo ou atualizado.

    Raises:
        ValueError: Se o token do usuário não for encontrado, ou se o repositório não for
                    encontrado (404) ou acessível (401).
        GithubException: Se ocorrer outro erro na API do GitHub durante a verificação.
        TypeError: Se o user_id não for do tipo ObjectId (erro interno).
        RuntimeError: Para erros inesperados ou erros ao salvar no banco de dados.
    """
    repo_full_name = f"{owner}/{repo_name}"
    logger.info(
        f"User ID: {user_id} request to configure branches for {repo_full_name}: {branches_to_configure}"
    )

    token = user_repo.get_user_token_by_id(user_id)
    if not token:
        logger.error(
            f"GitHub token not found for user ID: {user_id} when configuring {repo_full_name}"
        )
        raise ValueError("GitHub token not found for user. Please re-authenticate.")

    token_preview = f"{token[:5]}...{token[-5:]}" if len(token) > 10 else token
    logger.info(
        f"Attempting to use token preview: {token_preview} for user ID: {user_id} to verify access for {repo_full_name}"
    )

    try:

        def verify_repo_access_sync():
            logger.debug(
                f"Executing blocking GitHub repo access check in thread for {repo_full_name}"
            )
            g = Github(token)
            repo = g.get_repo(repo_full_name)
            logger.debug(
                f"Finished blocking GitHub repo access check for {repo_full_name}"
            )
            return repo.full_name

        verified_repo_name = await asyncio.to_thread(verify_repo_access_sync)
        logger.info(
            f"Verified access to repository: {verified_repo_name} for user ID: {user_id}"
        )
    except GithubException as e:
        logger.error(
            f"GitHub API error verifying repository {repo_full_name} for user ID {user_id} (token preview: {token_preview}): {e.status} - {e.data}",
            exc_info=True,
        )
        if e.status == 404:
            raise ValueError(
                f"Repository '{repo_full_name}' not found or not accessible."
            )
        if e.status == 401:
            raise ValueError(
                f"GitHub API error: Bad credentials verifying {repo_full_name}. Please re-authenticate."
            )
        raise RuntimeError(
            f"GitHub API error: {e.data.get('message', 'Unknown error')}"
        ) from e
    except Exception as e:
        logger.error(
            f"Unexpected error verifying repository {repo_full_name} for user ID {user_id} (token preview: {token_preview}): {e}",
            exc_info=True,
        )
        raise RuntimeError(
            f"An unexpected error occurred while verifying the repository."
        ) from e

    try:
        if not isinstance(user_id, ObjectId):
            logger.error(
                f"User ID {user_id} is not ObjectId type when saving config for {repo_full_name}. Type: {type(user_id)}"
            )
            raise TypeError("Internal server error: Invalid user ID format.")

        upsert_func = functools.partial(
            repo_config_repo.upsert_config,
            user_id,
            repo_full_name,
            branches_to_configure,
        )
        saved_config = await asyncio.to_thread(upsert_func)

        logger.info(
            f"Successfully saved branch configuration for {repo_full_name} by user ID: {user_id}"
        )
        try:
            await get_user_repositories.cache_invalidate(user_id, user_repo)
            logger.info(f"Invalidated repo list cache for user {user_id}")
        except Exception as cache_err:
            logger.warning(
                f"Could not invalidate cache for user {user_id}: {cache_err}"
            )

        return saved_config
    except Exception as e:
        logger.error(
            f"Error saving configuration for {repo_full_name} by user ID {user_id}: {e}",
            exc_info=True,
        )
        raise RuntimeError("Database error saving configuration.") from e


async def get_user_repo_configs(
    user_id: ObjectId, repo_config_repo: RepositoryConfigRepository
) -> List[RepositoryConfig]:
    """Recupera todas as configurações de repositório para um usuário específico.

    Args:
        user_id (ObjectId): O ID do usuário cujas configurações devem ser buscadas.
        repo_config_repo (RepositoryConfigRepository): Instância do repositório de configuração.

    Returns:
        List[RepositoryConfig]: Uma lista dos objetos de configuração encontrados.

    Raises:
        TypeError: Se o user_id não for do tipo ObjectId (erro interno).
        RuntimeError: Se ocorrer um erro ao buscar as configurações no banco de dados.
    """
    logger.info(f"Fetching repository configurations for user ID: {user_id}")
    try:
        if not isinstance(user_id, ObjectId):
            logger.error(
                f"User ID {user_id} is not ObjectId type when fetching configs. Type: {type(user_id)}"
            )
            raise TypeError("Internal server error: Invalid user ID format.")

        find_func = functools.partial(repo_config_repo.find_by_user, user_id)
        configs = await asyncio.to_thread(find_func)
        return configs
    except Exception as e:
        logger.error(
            f"Controller error fetching configurations for user ID {user_id}: {e}",
            exc_info=True,
        )
        raise RuntimeError("Database error retrieving configurations.") from e
