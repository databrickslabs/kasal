"""
Embedder configuration builder for different providers.

Handles embedder setup for:
- Databricks (with custom authentication)
- OpenAI
- Google (Gemini)
- Ollama
- Other providers
"""

from typing import Dict, Any, Optional, Tuple
import logging

from src.core.logger import LoggerManager
from src.utils.databricks_url_utils import DatabricksURLUtils

logger = LoggerManager.get_instance().crew


class EmbedderConfigBuilder:
    """Handles embedder configuration for different providers"""

    def __init__(self, config: Dict[str, Any], user_token: Optional[str] = None):
        """
        Initialize the EmbedderConfigBuilder

        Args:
            config: Crew configuration dictionary (must include group_id for multi-tenant isolation)
            user_token: Optional user access token for OBO authentication
        """
        self.config = config
        self.user_token = user_token
        self.custom_embedder = None
        self.embedder_config = None
        # SECURITY: Extract group_id from config for multi-tenant isolation
        self.group_id = config.get('group_id')

    def get_embedder_config_from_agents(self) -> Optional[Dict[str, Any]]:
        """
        Extract embedder configuration from agent configs

        Returns:
            Embedder configuration dict or None
        """
        embedder_config = None
        for agent_config in self.config.get('agents', []):
            if 'embedder_config' in agent_config and agent_config['embedder_config']:
                ec = agent_config['embedder_config']
                if isinstance(ec, dict) and 'provider' in ec:
                    embedder_config = ec
                    logger.info(f"Found valid embedder configuration: {embedder_config}")
                    break
                else:
                    logger.warning(f"Found invalid embedder config (missing provider): {ec}")

        # Default to Databricks if no valid config found
        if not embedder_config:
            embedder_config = {
                'provider': 'databricks',
                'config': {'model': 'databricks-gte-large-en'}
            }
            logger.info("No valid embedder config found, using default Databricks configuration")

        return embedder_config

    async def configure_embedder(self, crew_kwargs: Dict[str, Any]) -> Tuple[Dict[str, Any], Any, Any]:
        """
        Configure embedder based on provider

        Args:
            crew_kwargs: Crew keyword arguments to update

        Returns:
            Tuple of (updated crew_kwargs, custom_embedder, embedder_config)
        """
        embedder_config = self.get_embedder_config_from_agents()
        if not embedder_config:
            return crew_kwargs, None, None

        provider = embedder_config.get('provider', 'openai')
        config = embedder_config.get('config', {})

        if provider == 'databricks':
            crew_kwargs, custom_embedder, emb_config = await self._configure_databricks_embedder(crew_kwargs, config)
            self.custom_embedder = custom_embedder
            self.embedder_config = emb_config
        elif provider == 'openai':
            crew_kwargs = await self._configure_openai_embedder(crew_kwargs, config)
            self.embedder_config = crew_kwargs.get('embedder')
        elif provider == 'ollama':
            crew_kwargs = self._configure_ollama_embedder(crew_kwargs, config)
            self.embedder_config = crew_kwargs.get('embedder')
        elif provider == 'google':
            crew_kwargs = await self._configure_google_embedder(crew_kwargs, config)
            self.embedder_config = crew_kwargs.get('embedder')
        elif provider != 'databricks':
            # Other providers - pass through config as-is
            crew_kwargs['embedder'] = embedder_config
            logger.info(f"Configured CrewAI embedder for {provider}: {crew_kwargs['embedder']}")
            self.embedder_config = crew_kwargs.get('embedder')

        logger.info(f"Final embedder configuration: {crew_kwargs.get('embedder', 'None (default)')}")

        return crew_kwargs, self.custom_embedder, self.embedder_config

    async def _configure_databricks_embedder(self, crew_kwargs: Dict[str, Any], config: Dict[str, Any]) -> Tuple[Dict[str, Any], Any, Any]:
        """Configure Databricks embedder with custom authentication"""
        try:
            from src.utils.databricks_auth import get_databricks_auth_headers
            from src.services.api_keys_service import ApiKeysService
            from chromadb import EmbeddingFunction, Documents, Embeddings
            from typing import cast

            databricks_key = None
            auth_headers = None

            logger.info("Using unified Databricks auth for embeddings")
            logger.info(f"User token available: {bool(self.user_token)}, token length: {len(self.user_token) if self.user_token else 0}")

            # Get headers using unified auth
            auth_headers, error = await get_databricks_auth_headers(user_token=self.user_token)
            if error:
                logger.warning(f"Unified auth failed, falling back to API key: {error}")
                # SECURITY: Pass group_id for multi-tenant isolation
                databricks_key = await ApiKeysService.get_provider_api_key("DATABRICKS", group_id=self.group_id)

            if not databricks_key and not auth_headers:
                logger.warning("No Databricks API key found, falling back to default embedder")
                return crew_kwargs, None, None

            # Get Databricks endpoint
            databricks_endpoint = await self._get_databricks_endpoint()
            if not databricks_endpoint:
                logger.error("No Databricks endpoint found for embeddings - cannot create embedder")
                raise Exception("No Databricks workspace URL available for embeddings")

            model_name = config.get('model', 'databricks-gte-large-en')

            # Create custom embedding function
            class DatabricksEmbeddingFunction(EmbeddingFunction):
                def __init__(self, api_key: str = None, api_base: str = None, model: str = None,
                             auth_headers: dict = None, user_token: str = None):
                    self.api_key = api_key
                    self.api_base = api_base
                    self.model = model
                    self.auth_headers = auth_headers
                    self.user_token = user_token

                def __call__(self, input: Documents) -> Embeddings:
                    try:
                        import requests

                        workspace_url = DatabricksURLUtils.extract_workspace_from_endpoint(self.api_base)
                        endpoint_url = DatabricksURLUtils.construct_model_invocation_url(workspace_url, self.model)

                        if not endpoint_url:
                            raise Exception("Failed to construct valid endpoint URL")

                        logger.debug(f"Databricks embedding endpoint URL: {endpoint_url}")
                        payload = {"input": input if isinstance(input, list) else [input]}

                        # Prepare headers - prioritize user token for OBO auth
                        if self.user_token:
                            headers = {
                                "Authorization": f"Bearer {self.user_token}",
                                "Content-Type": "application/json"
                            }
                            logger.debug("Using OBO token for embeddings")
                        elif self.auth_headers:
                            headers = self.auth_headers
                        elif self.api_key:
                            headers = {
                                "Authorization": f"Bearer {self.api_key}",
                                "Content-Type": "application/json"
                            }
                        else:
                            logger.error("No authentication method available for Databricks embeddings")
                            raise Exception("No authentication method available")

                        response = requests.post(endpoint_url, headers=headers, json=payload, timeout=30)
                        if response.status_code == 200:
                            result = response.json()
                            if 'data' in result and len(result['data']) > 0:
                                embeddings = [item.get('embedding', item) for item in result['data']]
                                return cast(Embeddings, embeddings)
                            else:
                                raise Exception(f"Unexpected response format: {result}")
                        else:
                            raise Exception(f"Embedding API error {response.status_code}: {response.text}")

                    except Exception as e:
                        logger.error(f"Error in Databricks embedding function: {e}")
                        raise e

            # Construct URLs
            api_base_url = DatabricksURLUtils.construct_serving_endpoints_url(databricks_endpoint)
            if not api_base_url:
                logger.error(f"Failed to construct serving endpoints URL from workspace: {databricks_endpoint}")
                raise Exception("Failed to construct serving endpoints URL")

            logger.info(f"Databricks embedding api_base: {api_base_url}, model: {model_name}")

            databricks_embedder = DatabricksEmbeddingFunction(
                api_key=databricks_key,
                api_base=api_base_url,
                model=model_name,
                auth_headers=auth_headers,
                user_token=self.user_token
            )

            embedder_config = {
                'provider': 'custom',
                'config': {'embedder': databricks_embedder}
            }

            logger.info("Databricks custom embedder stored for DEFAULT backend manual memory configuration")

            return crew_kwargs, databricks_embedder, embedder_config

        except Exception as e:
            logger.error(f"Error configuring Databricks embedder: {e}")
            return crew_kwargs, None, None

    async def _get_databricks_endpoint(self) -> Optional[str]:
        """Get Databricks endpoint from unified auth or database"""
        databricks_endpoint = ''

        # Try unified auth first
        try:
            from src.utils.databricks_auth import get_auth_context
            auth = await get_auth_context()
            if auth and auth.workspace_url:
                databricks_endpoint = auth.workspace_url
                logger.info(f"Using workspace URL from unified {auth.auth_method} auth for embeddings: {databricks_endpoint}")
            else:
                logger.warning(f"get_auth_context() returned: {auth}")
        except Exception as e:
            logger.error(f"Failed to get unified auth for embeddings: {e}", exc_info=True)

        # If no endpoint, get from database
        if not databricks_endpoint:
            try:
                from src.services.databricks_service import DatabricksService
                from src.db.session import async_session_factory

                async with async_session_factory() as session:
                    databricks_service = DatabricksService(session)
                    db_config = await databricks_service.get_databricks_config()
                    if db_config and db_config.workspace_url:
                        databricks_endpoint = DatabricksURLUtils.normalize_workspace_url(db_config.workspace_url)
                        if databricks_endpoint:
                            logger.info(f"Using Databricks workspace URL from database: {databricks_endpoint}")
            except Exception as e:
                logger.warning(f"Could not get Databricks workspace URL from database: {e}")

        return databricks_endpoint

    async def _configure_openai_embedder(self, crew_kwargs: Dict[str, Any], config: Dict[str, Any]) -> Dict[str, Any]:
        """Configure OpenAI embedder"""
        try:
            from src.services.api_keys_service import ApiKeysService

            # SECURITY: Pass group_id for multi-tenant isolation
            openai_key = await ApiKeysService.get_provider_api_key("OPENAI", group_id=self.group_id)
            if openai_key:
                crew_kwargs['embedder'] = {
                    'provider': 'openai',
                    'config': {
                        'api_key': openai_key,
                        'model': config.get('model', 'text-embedding-3-small')
                    }
                }
                logger.info(f"Configured CrewAI embedder for OpenAI: {crew_kwargs['embedder']}")
        except Exception as e:
            logger.error(f"Error configuring OpenAI embedder: {e}")

        return crew_kwargs

    def _configure_ollama_embedder(self, crew_kwargs: Dict[str, Any], config: Dict[str, Any]) -> Dict[str, Any]:
        """Configure Ollama embedder"""
        crew_kwargs['embedder'] = {
            'provider': 'ollama',
            'config': {
                'model': config.get('model', 'nomic-embed-text')
            }
        }
        logger.info(f"Configured CrewAI embedder for Ollama: {crew_kwargs['embedder']}")
        return crew_kwargs

    async def _configure_google_embedder(self, crew_kwargs: Dict[str, Any], config: Dict[str, Any]) -> Dict[str, Any]:
        """Configure Google (Gemini) embedder"""
        try:
            from src.services.api_keys_service import ApiKeysService
            from src.schemas.model_provider import ModelProvider

            # SECURITY: Pass group_id for multi-tenant isolation
            google_key = await ApiKeysService.get_provider_api_key(ModelProvider.GEMINI, group_id=self.group_id)
            if google_key:
                crew_kwargs['embedder'] = {
                    'provider': 'google',
                    'config': {
                        'api_key': google_key,
                        'model': config.get('model', 'text-embedding-004')
                    }
                }
                logger.info(f"Configured CrewAI embedder for Google: {crew_kwargs['embedder']}")
        except Exception as e:
            logger.error(f"Error configuring Google embedder: {e}")

        return crew_kwargs
