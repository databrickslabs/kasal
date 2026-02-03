from typing import Dict, Optional, Union, Any, List
from crewai.tools import BaseTool
import logging
import os
import asyncio
import json

# Import only the CrewAI tools we're keeping
from crewai_tools import (
    DallETool,
    SerperDevTool,
    ScrapeWebsiteTool,
)

# Import custom tools - Using proper import paths
try:
    from .custom.perplexity_tool import PerplexitySearchTool
except ImportError:
    try:
        from .custom.perplexity_tool import PerplexitySearchTool
    except ImportError:
        PerplexitySearchTool = None
        logging.warning("Could not import PerplexitySearchTool")

try:
    from .custom.genie_tool import GenieTool
except ImportError:
    try:
        from .custom.genie_tool import GenieTool
    except ImportError:
        GenieTool = None
        logging.warning("Could not import GenieTool")

try:
    from .custom.agentbricks_tool import AgentBricksTool
except ImportError:
    try:
        from .custom.agentbricks_tool import AgentBricksTool
    except ImportError:
        AgentBricksTool = None
        logging.warning("Could not import AgentBricksTool")

try:
    from .custom.databricks_jobs_tool import DatabricksJobsTool
except ImportError:
    try:
        from .custom.databricks_jobs_tool import DatabricksJobsTool
    except ImportError:
        DatabricksJobsTool = None
        logging.warning("Could not import DatabricksJobsTool")

try:
    from .custom.databricks_knowledge_search_tool import DatabricksKnowledgeSearchTool
except ImportError:
    try:
        from .custom.databricks_knowledge_search_tool import DatabricksKnowledgeSearchTool
    except ImportError:
        DatabricksKnowledgeSearchTool = None
        logging.warning("Could not import DatabricksKnowledgeSearchTool")

try:
    from .custom.powerbi_analysis_tool import PowerBIAnalysisTool
except ImportError:
    try:
        from .custom.powerbi_analysis_tool import PowerBIAnalysisTool
    except ImportError:
        PowerBIAnalysisTool = None
        logging.warning("Could not import PowerBIAnalysisTool")

# MCPTool - Import from mcp_adapter
try:
    from src.engines.common.mcp_adapter import MCPTool
except ImportError:
    MCPTool = None
    logging.warning("Could not import MCPTool - MCP integration may not be available")

# Converter tools - Power BI connector and universal pipeline
try:
    from .custom.powerbi_connector_tool import PowerBIConnectorTool
    from .custom.measure_conversion_pipeline_tool import MeasureConversionPipelineTool
except ImportError as e:
    PowerBIConnectorTool = None
    MeasureConversionPipelineTool = None
    logging.warning(f"Could not import converter tools: {e}")

# M-Query Conversion Pipeline Tool
try:
    from .custom.mquery_conversion_pipeline_tool import MqueryConversionPipelineTool
except ImportError as e:
    MqueryConversionPipelineTool = None
    logging.warning(f"Could not import MqueryConversionPipelineTool: {e}")

# Power BI Relationships Tool
try:
    from .custom.powerbi_relationships_tool import PowerBIRelationshipsTool
except ImportError as e:
    PowerBIRelationshipsTool = None
    logging.warning(f"Could not import PowerBIRelationshipsTool: {e}")

# Power BI Hierarchies Tool
try:
    from .custom.powerbi_hierarchies_tool import PowerBIHierarchiesTool
except ImportError as e:
    PowerBIHierarchiesTool = None
    logging.warning(f"Could not import PowerBIHierarchiesTool: {e}")

# Power BI Field Parameters & Calculation Groups Tool
try:
    from .custom.powerbi_field_parameters_calculation_groups_tool import PowerBIFieldParametersCalculationGroupsTool
except ImportError as e:
    PowerBIFieldParametersCalculationGroupsTool = None
    logging.warning(f"Could not import PowerBIFieldParametersCalculationGroupsTool: {e}")

# Power BI Report References Tool
try:
    from .custom.powerbi_report_references_tool import PowerBIReportReferencesTool
except ImportError as e:
    PowerBIReportReferencesTool = None
    logging.warning(f"Could not import PowerBIReportReferencesTool: {e}")

# Setup logger
logger = logging.getLogger(__name__)

# Import async session factory once
from src.db.session import async_session_factory
from src.services.tool_service import ToolService
from src.services.api_keys_service import ApiKeysService
from src.schemas.tool import ToolUpdate
from src.utils.encryption_utils import EncryptionUtils

class ToolFactory:
    def __init__(self, config, api_keys_service=None, user_token=None):
        """
        Initialize the tool factory with configuration

        Args:
            config: Configuration dictionary for the factory
            api_keys_service: Optional ApiKeysService for retrieving API keys
            user_token: User access token for OAuth authentication
        """
        self.config = config
        self.api_keys_service = api_keys_service
        self.user_token = user_token
        # Store tools by both ID and title for easy lookup
        self._available_tools: Dict[str, object] = {}
        self._tool_implementations = {}

        # Map tool names to their implementations - ONLY THE TOOLS WE'RE KEEPING
        self._tool_implementations = {
            "PerplexityTool": PerplexitySearchTool,
            "Dall-E Tool": DallETool,
            "SerperDevTool": SerperDevTool,
            "ScrapeWebsiteTool": ScrapeWebsiteTool,
            "GenieTool": GenieTool,
            "AgentBricksTool": AgentBricksTool,
            "DatabricksJobsTool": DatabricksJobsTool,
            "DatabricksKnowledgeSearchTool": DatabricksKnowledgeSearchTool,
            "PowerBIAnalysisTool": PowerBIAnalysisTool,
        }

        # Add MCPTool if it was successfully imported
        if MCPTool is not None:
            self._tool_implementations["MCPTool"] = MCPTool

        # Add converter tools if successfully imported
        if PowerBIConnectorTool is not None:
            self._tool_implementations["PowerBIConnectorTool"] = PowerBIConnectorTool
        if MeasureConversionPipelineTool is not None:
            self._tool_implementations["Measure Conversion Pipeline"] = MeasureConversionPipelineTool
        if MqueryConversionPipelineTool is not None:
            self._tool_implementations["M-Query Conversion Pipeline"] = MqueryConversionPipelineTool
        if PowerBIRelationshipsTool is not None:
            self._tool_implementations["Power BI Relationships Tool"] = PowerBIRelationshipsTool
        if PowerBIHierarchiesTool is not None:
            self._tool_implementations["Power BI Hierarchies Tool"] = PowerBIHierarchiesTool
        if PowerBIFieldParametersCalculationGroupsTool is not None:
            self._tool_implementations["Power BI Field Parameters & Calculation Groups Tool"] = PowerBIFieldParametersCalculationGroupsTool
        if PowerBIReportReferencesTool is not None:
            self._tool_implementations["Power BI Report References Tool"] = PowerBIReportReferencesTool

        # Initialize _initialized flag
        self._initialized = False

    async def _validate_databricks_auth(self) -> tuple[bool, str]:
        """
        Validate that Databricks authentication is properly configured.
        
        Returns:
            Tuple of (is_valid, error_message)
        """
        try:
            from src.utils.databricks_auth import get_auth_context
            
            # Check for user token (OBO authentication)
            if self.user_token:
                logger.info("[AUTH VALIDATION] User token available for OBO authentication")
                return (True, "OBO authentication available")
            
            # Check for unified auth
            try:
                auth = await get_auth_context()
                if auth and (auth.token or auth.workspace_url):
                    logger.info("[AUTH VALIDATION] Unified auth context available")
                    return (True, "Unified authentication available")
            except Exception as e:
                logger.debug(f"[AUTH VALIDATION] Unified auth not available: {e}")
            
            # Check for Databricks config in database
            try:
                from src.services.databricks_service import DatabricksService
                from src.db.session import async_session_factory
                
                group_id = self.config.get('group_id', 'default') if isinstance(self.config, dict) else 'default'
                
                async with async_session_factory() as session:
                    service = DatabricksService(session)
                    config = await service.get_databricks_config(group_id=group_id)
                    
                    if config and config.workspace_url:
                        # Check if we have any auth method configured
                        has_auth = bool(
                            config.api_key or 
                            config.client_id or 
                            config.oauth_enabled
                        )
                        if has_auth:
                            logger.info(f"[AUTH VALIDATION] Databricks config found for group {group_id}")
                            return (True, "Database configuration available")
                        else:
                            logger.warning(f"[AUTH VALIDATION] Databricks config exists but no auth method configured")
                            return (False, "No authentication method configured in database")
                    else:
                        logger.warning(f"[AUTH VALIDATION] No Databricks config found for group {group_id}")
                        return (False, f"No Databricks configuration for group {group_id}")
                        
            except Exception as e:
                logger.debug(f"[AUTH VALIDATION] Database config check failed: {e}")
            
            # No authentication method available
            error_msg = (
                "No Databricks authentication method available. "
                "Configure one of: user token (OBO), API key, or OAuth credentials"
            )
            logger.error(f"[AUTH VALIDATION] {error_msg}")
            return (False, error_msg)
            
        except Exception as e:
            logger.error(f"[AUTH VALIDATION] Error during validation: {e}", exc_info=True)
            return (False, f"Authentication validation error: {str(e)}")

    @classmethod
    async def create(cls, config, api_keys_service=None, user_token=None):
        """
        Async factory method to create and initialize a ToolFactory instance.

        Args:
            config: Configuration dictionary for the factory
            api_keys_service: Optional ApiKeysService for retrieving API keys
            user_token: User access token for OAuth authentication

        Returns:
            Initialized ToolFactory instance
        """
        instance = cls(config, api_keys_service, user_token)
        await instance.initialize()
        return instance

    async def initialize(self):
        """Initialize the tool factory asynchronously"""
        if not self._initialized:
            try:
                await self._load_available_tools_async()

                # Setup API keys if we have the service
                if self.api_keys_service:
                    # Pre-load common API keys into environment
                    api_keys_to_load = ["SERPER_API_KEY", "PERPLEXITY_API_KEY", "OPENAI_API_KEY", "DATABRICKS_API_KEY"]
                    for key_name in api_keys_to_load:
                        try:
                            # Use utility function to avoid event loop issues
                            from src.utils.asyncio_utils import execute_db_operation_with_fresh_engine

                            # Get group_id from config or api_keys_service
                            group_id = None
                            try:
                                group_id = self.config.get("group_id") if isinstance(self.config, dict) else None
                            except Exception:
                                pass

                            # If not in config, try to get from api_keys_service
                            if not group_id and self.api_keys_service:
                                group_id = getattr(self.api_keys_service, 'group_id', None)

                            async def _get_key_operation(session):
                                # SECURITY: Re-use the api_keys_service with group_id for multi-tenant isolation
                                from src.services.api_keys_service import ApiKeysService
                                api_keys_service = ApiKeysService(session, group_id=group_id)
                                return await api_keys_service.find_by_name(key_name)

                            api_key_obj = await execute_db_operation_with_fresh_engine(_get_key_operation)

                            if api_key_obj and api_key_obj.encrypted_value:
                                # Decrypt the value
                                api_key = EncryptionUtils.decrypt_value(api_key_obj.encrypted_value)
                                os.environ[key_name] = api_key
                                logger.info(f"Pre-loaded {key_name} from ApiKeysService")
                        except Exception as e:
                            logger.error(f"Error pre-loading {key_name}: {str(e)}")

                self._initialized = True
            except Exception as e:
                logger.error(f"Error during async initialization: {e}")
                raise

    def _sync_load_available_tools(self):
        """
        Synchronous method to load available tools
        This uses a new event loop - DO NOT CALL from inside an async context
        """
        try:
            # Check if we're already in an event loop
            try:
                loop = asyncio.get_running_loop()
                # If we're here, we're already in an event loop
                logger.warning("Already in event loop, using a workaround to load tools")
                # Create a new thread to run a new event loop
                import concurrent.futures
                with concurrent.futures.ThreadPoolExecutor() as pool:
                    future = pool.submit(self._run_in_new_loop, self._load_available_tools_async)
                    future.result()
            except RuntimeError:
                # No running event loop, safe to create a new one
                loop = asyncio.new_event_loop()
                try:
                    asyncio.set_event_loop(loop)
                    loop.run_until_complete(self._load_available_tools_async())

                    # Also pre-load API keys if we have the service
                    if self.api_keys_service:
                        # Pre-load common API keys into environment
                        api_keys_to_load = ["SERPER_API_KEY", "PERPLEXITY_API_KEY", "OPENAI_API_KEY", "DATABRICKS_API_KEY"]
                        for key_name in api_keys_to_load:
                            try:
                                api_key = loop.run_until_complete(
                                    self._get_api_key_async(key_name)
                                )
                                if api_key:
                                    os.environ[key_name] = api_key
                                    logger.info(f"Pre-loaded {key_name} from ApiKeysService (sync)")
                            except Exception as e:
                                logger.error(f"Error pre-loading {key_name} (sync): {str(e)}")
                finally:
                    loop.close()
        except Exception as e:
            logger.error(f"Error in _sync_load_available_tools: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())

    async def _load_available_tools_async(self):
        """Load all available tools from the service asynchronously"""
        try:
            # Get services using session factory
            from src.services.tool_service import ToolService
            from src.db.session import async_session_factory
            from src.utils.user_context import GroupContext

            async with async_session_factory() as session:
                # Create tool service with session
                tool_service = ToolService(session)

                # Group-aware: prefer loading tools for the current group if provided in config
                group_id = None
                try:
                    group_id = self.config.get("group_id") if isinstance(self.config, dict) else None
                except Exception:
                    group_id = None

                if group_id:
                    group_context = GroupContext(group_ids=[group_id])
                    tools_response = await tool_service.get_enabled_tools_for_group(group_context)
                else:
                    tools_response = await tool_service.get_all_tools()
                tools = tools_response.tools

                # Store tools by both title and ID
                self._available_tools = {}
                for tool in tools:
                    self._available_tools[tool.title] = tool
                    self._available_tools[str(tool.id)] = tool  # Convert ID to string since it might come as string from config

                logger.info(f"Loaded {len(tools)} tools from service (group_id={group_id})")
                logger.debug(f"Available tools: {[f'{t.id}:{t.title}' for t in tools]}")
        except Exception as e:
            logger.error(f"Error loading available tools: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())

    def get_tool_info(self, tool_identifier: Union[str, int]) -> Optional[object]:
        """
        Get tool information by ID or title

        Args:
            tool_identifier: Either the tool's ID (int or str) or title (str)

        Returns:
            Tool object if found, None otherwise
        """
        # Convert integer IDs to strings for dictionary lookup
        if isinstance(tool_identifier, int):
            tool_identifier = str(tool_identifier)

        tool = self._available_tools.get(tool_identifier)

        if tool:
            logger.info(f"Found tool: ID={getattr(tool, 'id', 'N/A')}, title={getattr(tool, 'title', 'N/A')}")
        else:
            logger.warning(f"Tool '{tool_identifier}' not found in available tools. Available IDs and titles are: {list(self._available_tools.keys())}")

        return tool

    async def _get_api_key_async(self, key_name: str) -> Optional[str]:
        """Get an API key asynchronously through the service"""
        try:
            if self.api_keys_service:
                # Use the provided API keys service properly through its methods
                try:
                    api_key = await self.api_keys_service.find_by_name(key_name)
                    if api_key and api_key.encrypted_value:
                        # Decrypt the value
                        decrypted_value = EncryptionUtils.decrypt_value(api_key.encrypted_value)

                        # Log first and last 4 characters of the key for debugging
                        key_preview = f"{decrypted_value[:4]}...{decrypted_value[-4:]}" if len(decrypted_value) > 8 else "***"
                        logger.info(f"Using {key_name} from service directly: {key_preview}")
                        return decrypted_value
                    else:
                        logger.warning(f"{key_name} not found via service")
                        return None
                except Exception as e:
                    logger.error(f"Error with existing API keys service for {key_name}: {str(e)}")
                    # Fall through to the alternative method

            # Fallback to creating a new API keys service instance using isolated UnitOfWork
            # Import necessary modules here to avoid circular imports
            from src.utils.asyncio_utils import execute_db_operation_with_fresh_engine

            # Get group_id from config or api_keys_service
            group_id = None
            try:
                group_id = self.config.get("group_id") if isinstance(self.config, dict) else None
            except Exception:
                pass

            # If not in config, try to get from api_keys_service
            if not group_id and self.api_keys_service:
                group_id = getattr(self.api_keys_service, 'group_id', None)

            async def _get_key_with_fresh_engine(session):
                from src.services.api_keys_service import ApiKeysService
                # SECURITY: Create service with group_id for multi-tenant isolation
                api_keys_service = ApiKeysService(session, group_id=group_id)
                api_key = await api_keys_service.find_by_name(key_name)

                if api_key and api_key.encrypted_value:
                    # Decrypt the value
                    return EncryptionUtils.decrypt_value(api_key.encrypted_value)
                return None

            # Use a fresh engine to avoid transaction conflicts
            decrypted_value = await execute_db_operation_with_fresh_engine(_get_key_with_fresh_engine)

            if decrypted_value:
                # Log first and last 4 characters of the key for debugging
                key_preview = f"{decrypted_value[:4]}...{decrypted_value[-4:]}" if len(decrypted_value) > 8 else "***"
                logger.info(f"Using {key_name} from isolated database operation: {key_preview}")
                return decrypted_value
            else:
                logger.warning(f"{key_name} not found via isolated database operation")
                return None

        except Exception as e:
            logger.error(f"Error getting {key_name} from service: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())
            return None

    def _get_api_key(self, key_name: str) -> Optional[str]:
        """
        Get an API key through the service layer synchronously
        Only use this method when not in an async context
        """
        try:
            # Check if we're already in an event loop
            try:
                loop = asyncio.get_running_loop()
                # If we're here, we're already in an event loop
                logger.warning("Already in event loop, creating new thread for API key retrieval")
                # Create a new thread to run a new event loop
                import concurrent.futures
                with concurrent.futures.ThreadPoolExecutor() as pool:
                    future = pool.submit(self._run_in_new_loop, self._get_api_key_async, key_name)
                    return future.result()
            except RuntimeError:
                # No running event loop, safe to create a new one
                loop = asyncio.new_event_loop()
                try:
                    asyncio.set_event_loop(loop)
                    return loop.run_until_complete(self._get_api_key_async(key_name))
                finally:
                    loop.close()
        except Exception as e:
            logger.error(f"Error getting {key_name} from service: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())
            # Don't halt execution completely if the API key retrieval fails
            logger.warning(f"Continuing without {key_name}")
            return None

    def _run_in_new_loop(self, async_func, *args, **kwargs):
        """Run an async function in a new event loop in a separate thread"""
        loop = asyncio.new_event_loop()
        try:
            asyncio.set_event_loop(loop)
            return loop.run_until_complete(async_func(*args, **kwargs))
        finally:
            loop.close()

    def update_tool_config(self, tool_identifier: Union[str, int], config_update: Dict[str, any]) -> bool:
        """
        Update a tool's configuration through the service layer

        Args:
            tool_identifier: Either the tool's ID (int or str) or title (str)
            config_update: Dictionary with configuration updates

        Returns:
            True if successful, False otherwise
        """
        try:
            # Get tool info
            tool_info = self.get_tool_info(tool_identifier)
            if not tool_info:
                logger.error(f"Tool '{tool_identifier}' not found. Cannot update config.")
                return False

            # Check if we're already in an event loop
            try:
                loop = asyncio.get_running_loop()
                # If we're here, we're already in an event loop
                logger.warning("Already in event loop, using a workaround to update tool config")
                # Create a new thread to run a new event loop
                import concurrent.futures
                with concurrent.futures.ThreadPoolExecutor() as pool:
                    future = pool.submit(self._run_in_new_loop, self._update_tool_config_async,
                                        tool_identifier, tool_info, config_update)
                    return future.result()
            except RuntimeError:
                # No running event loop, safe to create a new one
                loop = asyncio.new_event_loop()
                try:
                    asyncio.set_event_loop(loop)
                    return loop.run_until_complete(self._update_tool_config_async(
                        tool_identifier, tool_info, config_update))
                finally:
                    loop.close()
        except Exception as e:
            logger.error(f"Error updating tool configuration: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())
            return False

    async def _update_tool_config_async(self, tool_identifier, tool_info, config_update):
        """Async implementation of tool config update"""
        # Get services using session factory
        from src.db.session import async_session_factory
        from src.services.tool_service import ToolService

        async with async_session_factory() as session:
            # Create tool service with session
            tool_service = ToolService(session)

            # If we found by ID, use ID for update, otherwise use title
            if isinstance(tool_identifier, (int, str)) and str(tool_identifier).isdigit():
                # Update by ID
                tool_id = int(tool_identifier)

                # Prepare update data
                if hasattr(tool_info, 'config') and isinstance(tool_info.config, dict):
                    # Merge existing config with updates
                    updated_config = {**tool_info.config, **config_update}
                else:
                    updated_config = config_update

                update_data = ToolUpdate(config=updated_config)

                # Update the tool using the service instance
                result = await tool_service.update_tool(tool_id, update_data)
                logger.info(f"Updated tool {tool_id} configuration using UnitOfWork")

                # Refresh available tools
                await self._load_available_tools_async()
                return True
            else:
                # Update by title
                title = tool_info.title
                # Update the tool using the service instance
                result = await tool_service.update_tool_configuration_by_title(title, config_update)
                logger.info(f"Updated tool '{title}' configuration using UnitOfWork")

                # Refresh available tools
                await self._load_available_tools_async()
                return True

    def create_tool(
        self,
        tool_identifier: Union[str, int],
        result_as_answer: bool = False,
        tool_config_override: Optional[Dict[str, Any]] = None
    ) -> Optional[Union[BaseTool, list]]:
        """
        Create a tool instance based on its identifier.

        Args:
            tool_identifier: Either the tool's ID (int or str) or title (str)
            result_as_answer: Whether the tool's result should be treated as the final answer
            tool_config_override: Optional configuration overrides for this specific tool instance

        Returns:
            Tool instance if successfully created, None otherwise
        """
        # Get tool info from our cached tools obtained from the service
        tool_info = self.get_tool_info(tool_identifier)
        if not tool_info:
            logger.error(f"Tool '{tool_identifier}' not found. Please ensure the tool is registered.")
            return None

        # Log found tool details
        tool_id = getattr(tool_info, 'id', None)
        tool_title = getattr(tool_info, 'title', None)
        logger.info(f"Creating tool with ID={tool_id}, title={tool_title}")

        # Look up the implementation class based on the tool's title
        if not hasattr(self, '_tool_implementations') or not self._tool_implementations:
            logger.error("Tool implementations dictionary not initialized")
            return None

        tool_name = tool_info.title
        tool_class = self._tool_implementations.get(tool_name)

        if not tool_class:
            logger.warning(f"No implementation found for tool '{tool_name}'")
            return None

        try:
            # Get base tool config from tool info
            base_config = tool_info.config if hasattr(tool_info, 'config') and tool_info.config is not None else {}

            # Log what we're merging
            logger.info(f"[ToolFactory] {tool_name} - base_config from tool_info: {base_config}")
            logger.info(f"[ToolFactory] {tool_name} - tool_config_override received: {tool_config_override}")

            # Merge with override config if provided
            # The override takes precedence over base_config
            tool_config = {**base_config, **(tool_config_override or {})}

            # Inject execution inputs if available in the main config (for dynamic parameter resolution)
            # Handle both direct inputs and nested inputs structure
            execution_inputs = None
            if hasattr(self, 'config') and self.config:
                # Check for nested inputs structure: config['inputs']['inputs']
                if 'inputs' in self.config and isinstance(self.config['inputs'], dict):
                    if 'inputs' in self.config['inputs']:
                        execution_inputs = self.config['inputs']['inputs']
                        logger.info(f"[ToolFactory] Found nested execution_inputs for {tool_name}: {list(execution_inputs.keys())}")
                    else:
                        # Fallback: try direct inputs (might contain agents_yaml, tasks_yaml, etc.)
                        # Filter out non-user inputs
                        user_inputs = {k: v for k, v in self.config['inputs'].items()
                                      if k not in ['agents_yaml', 'tasks_yaml', 'planning', 'model', 'execution_type',
                                                   'schema_detection_enabled', 'process', 'run_name']}
                        if user_inputs:
                            execution_inputs = user_inputs
                            logger.info(f"[ToolFactory] Found direct execution_inputs for {tool_name}: {list(execution_inputs.keys())}")

                if execution_inputs:
                    tool_config['execution_inputs'] = execution_inputs
                    # Log keys only (don't log sensitive values like client_secret)
                    logger.info(f"[ToolFactory] ✓ Injected execution_inputs into {tool_name} with keys: {list(execution_inputs.keys())}")

                    # RESOLVE PLACEHOLDERS: Replace {placeholder} with actual values from execution_inputs
                    import re
                    resolved_count = 0
                    for key, value in list(tool_config.items()):
                        if isinstance(value, str) and '{' in value:
                            placeholders = re.findall(r'\{(\w+)\}', value)
                            if placeholders:
                                resolved_value = value
                                for placeholder in placeholders:
                                    if placeholder in execution_inputs:
                                        replacement = str(execution_inputs[placeholder])
                                        resolved_value = resolved_value.replace(f'{{{placeholder}}}', replacement)
                                        # Log resolution (mask sensitive values)
                                        if 'secret' in key.lower() or 'password' in key.lower() or 'token' in key.lower():
                                            logger.info(f"[ToolFactory RESOLVE] {key}: {{{placeholder}}} → [REDACTED]")
                                        else:
                                            logger.info(f"[ToolFactory RESOLVE] {key}: {{{placeholder}}} → {replacement}")
                                        resolved_count += 1
                                tool_config[key] = resolved_value

                    if resolved_count > 0:
                        logger.info(f"[ToolFactory] ✓ Resolved {resolved_count} placeholders in {tool_name} config")

            logger.info(f"[ToolFactory] {tool_name} config (after merge): {tool_config}")

            # For critical tools, verify override was applied
            if tool_config_override and tool_name == "Measure Conversion Pipeline":
                logger.info(f"[ToolFactory] {tool_name} - Verifying override was applied:")
                for key in ['inbound_connector', 'outbound_format', 'powerbi_semantic_model_id', 'powerbi_group_id']:
                    if key in tool_config_override:
                        base_val = base_config.get(key, 'NOT IN BASE')
                        override_val = tool_config_override.get(key, 'NOT IN OVERRIDE')
                        merged_val = tool_config.get(key, 'NOT IN MERGED')
                        logger.info(f"[ToolFactory]   {key}: base='{base_val}' → override='{override_val}' → merged='{merged_val}'")

            # Verify override for Power BI Field Parameters tool
            if tool_name == "Power BI Field Parameters & Calculation Groups Tool":
                logger.info(f"[ToolFactory] {tool_name} - Verifying config:")
                for key in ['workspace_id', 'dataset_id', 'tenant_id', 'client_id', 'client_secret', 'mode']:
                    base_val = base_config.get(key, 'NOT IN BASE')
                    override_val = (tool_config_override or {}).get(key, 'NOT IN OVERRIDE')
                    merged_val = tool_config.get(key, 'NOT IN MERGED')
                    # Mask secrets
                    if 'secret' in key.lower():
                        base_val = '***' if base_val and base_val != 'NOT IN BASE' else base_val
                        override_val = '***' if override_val and override_val != 'NOT IN OVERRIDE' else override_val
                        merged_val = '***' if merged_val and merged_val != 'NOT IN MERGED' else merged_val
                    logger.info(f"[ToolFactory]   {key}: base='{base_val}' → override='{override_val}' → merged='{merged_val}'")

            # Handle specific tool types
            if tool_name == "PerplexityTool":
                # Use parameters directly from tool config
                api_key = tool_config.get('api_key', '')

                # Try to get the key from environment first
                perplexity_api_key = os.environ.get("PERPLEXITY_API_KEY")

                # If not found in environment, try to get it from the service
                if not perplexity_api_key and not api_key:
                    # Use the API keys service if provided, otherwise use the normal methods
                    if self.api_keys_service is not None:
                        logger.info("Using ApiKeysService to get PERPLEXITY_API_KEY")
                        try:
                            # Check if we're in an async context
                            asyncio.get_running_loop()
                            # Use ThreadPoolExecutor to call async method from sync context
                            import concurrent.futures
                            with concurrent.futures.ThreadPoolExecutor() as pool:
                                db_api_key = pool.submit(
                                    self._run_in_new_loop,
                                    self._get_api_key_async,
                                    "PERPLEXITY_API_KEY"
                                ).result()
                        except RuntimeError:
                            # Not in async context
                            loop = asyncio.new_event_loop()
                            try:
                                asyncio.set_event_loop(loop)
                                db_api_key = loop.run_until_complete(
                                    self._get_api_key_async("PERPLEXITY_API_KEY")
                                )
                            finally:
                                loop.close()

                        # Assign the retrieved key to perplexity_api_key
                        if db_api_key:
                            os.environ["PERPLEXITY_API_KEY"] = db_api_key
                            perplexity_api_key = db_api_key
                            logger.info("Retrieved PERPLEXITY_API_KEY from ApiKeysService")
                    else:
                        # Fallback to original method
                        logger.info("No ApiKeysService provided, using fallback method for PERPLEXITY_API_KEY")
                        try:
                            # Check if we're already in an event loop
                            current_loop = asyncio.get_running_loop()
                            # We're in an async context, use ThreadPoolExecutor
                            import concurrent.futures
                            with concurrent.futures.ThreadPoolExecutor() as pool:
                                db_api_key = pool.submit(self._run_in_new_loop,
                                                        self._get_api_key_async,
                                                        "PERPLEXITY_API_KEY").result()
                        except RuntimeError:
                            # We're not in an async context, use direct method
                            db_api_key = self._get_api_key("PERPLEXITY_API_KEY")

                        if db_api_key:
                            # Set in environment for tools that read from there
                            os.environ["PERPLEXITY_API_KEY"] = db_api_key
                            perplexity_api_key = db_api_key

                # Use tool configuration or environment
                final_api_key = api_key or perplexity_api_key

                # Add api key to config and create with all parameters from config
                tool_config_with_key = {**tool_config}
                if final_api_key:
                    # Use 'api_key' as that's what PerplexitySearchTool expects
                    tool_config_with_key['api_key'] = final_api_key
                    # Remove 'perplexity_api_key' if it exists to avoid unexpected keyword arg error
                    if 'perplexity_api_key' in tool_config_with_key:
                        del tool_config_with_key['perplexity_api_key']

                # Add result_as_answer to tool configuration
                tool_config_with_key['result_as_answer'] = result_as_answer

                logger.info(f"Creating PerplexityTool with config: {tool_config_with_key}")
                return tool_class(**tool_config_with_key)

            elif tool_name == "SerperDevTool":
                # Log the incoming tool config for SerperDevTool
                logger.info(f"SerperDevTool - incoming tool_config: {tool_config}")

                # Map frontend 'endpoint_type' to SerperDevTool's 'search_type' parameter
                if 'endpoint_type' in tool_config and 'search_type' not in tool_config:
                    endpoint_type = tool_config['endpoint_type']
                    # Only map if it's a supported type (search or news)
                    if endpoint_type in ['search', 'news']:
                        tool_config['search_type'] = endpoint_type
                        logger.info(f"SerperDevTool - Mapped endpoint_type '{endpoint_type}' to search_type")
                    else:
                        logger.warning(f"SerperDevTool - Unsupported endpoint_type '{endpoint_type}', defaulting to 'search'")
                        tool_config['search_type'] = 'search'

                # Get API key from tool config
                api_key = tool_config.get('serper_api_key', '')

                # Try to get the key from environment first
                serper_api_key = os.environ.get("SERPER_API_KEY")

                # If not found in environment, try to get it from the service
                if not serper_api_key and not api_key:
                    # Use the API keys service if provided, otherwise use the normal methods
                    if self.api_keys_service is not None:
                        logger.info("Using ApiKeysService to get SERPER_API_KEY")
                        try:
                            # Check if we're in an async context
                            asyncio.get_running_loop()
                            # Use ThreadPoolExecutor to call async method from sync context
                            import concurrent.futures
                            with concurrent.futures.ThreadPoolExecutor() as pool:
                                db_api_key = pool.submit(
                                    self._run_in_new_loop,
                                    self._get_api_key_async,
                                    "SERPER_API_KEY"
                                ).result()
                        except RuntimeError:
                            # Not in async context
                            loop = asyncio.new_event_loop()
                            try:
                                asyncio.set_event_loop(loop)
                                db_api_key = loop.run_until_complete(
                                    self._get_api_key_async("SERPER_API_KEY")
                                )
                            finally:
                                loop.close()
                    else:
                        # Fallback to original method
                        logger.info("No ApiKeysService provided, using fallback method for SERPER_API_KEY")
                        try:
                            # Check if we're already in an event loop
                            current_loop = asyncio.get_running_loop()
                            # We're in an async context, use ThreadPoolExecutor
                            import concurrent.futures
                            with concurrent.futures.ThreadPoolExecutor() as pool:
                                db_api_key = pool.submit(self._run_in_new_loop,
                                                        self._get_api_key_async,
                                                        "SERPER_API_KEY").result()
                        except RuntimeError:
                            # We're not in an async context, use direct method
                            db_api_key = self._get_api_key("SERPER_API_KEY")

                        if db_api_key:
                            # Set in environment for tools that read from there
                            os.environ["SERPER_API_KEY"] = db_api_key
                            serper_api_key = db_api_key

                # Use tool configuration or environment
                final_api_key = api_key or serper_api_key

                # Add api key to config and create with all parameters from config
                tool_config_with_key = {**tool_config}
                if final_api_key:
                    tool_config_with_key['api_key'] = final_api_key

                # Remove frontend-specific fields that SerperDevTool doesn't recognize
                fields_to_remove = ['endpoint_type', 'search_url', 'serper_api_key']
                for field in fields_to_remove:
                    tool_config_with_key.pop(field, None)

                # Add result_as_answer to tool configuration
                tool_config_with_key['result_as_answer'] = result_as_answer

                # Log the final config being passed to SerperDevTool
                logger.info(f"SerperDevTool - final tool_config_with_key: {tool_config_with_key}")
                logger.info(f"SerperDevTool - search_type in config: {tool_config_with_key.get('search_type', 'NOT SET')}")

                return tool_class(**tool_config_with_key)

            elif tool_name == "DatabricksJobsTool":
                # Create a copy of the config (same pattern as other Databricks tools)
                databricks_jobs_config = {**tool_config}

                # Try to get user token from multiple sources for OAuth/OBO authentication
                user_token = tool_config.get('user_token') or self.user_token

                # If no user token in config or factory, try to get from context
                if not user_token:
                    try:
                        from src.utils.user_context import UserContext
                        user_token = UserContext.get_user_token()
                        if user_token:
                            logger.info(f"Extracted user token from context for DatabricksJobsTool OBO authentication: {user_token[:10]}...")
                        else:
                            logger.warning("No user token found in context for DatabricksJobsTool")
                    except Exception as e:
                        logger.error(f"Could not extract user token from context: {e}")

                # Get DATABRICKS_HOST from tool_config or environment
                databricks_host = tool_config.get('DATABRICKS_HOST')

                # If DATABRICKS_HOST is not in tool_config, try to get it from unified auth
                if not databricks_host:
                    # Use unified authentication
                    try:
                        from src.utils.databricks_auth import get_auth_context
                        # Check if we're in an async context
                        try:
                            asyncio.get_running_loop()
                            # We're in an event loop, use thread pool
                            import concurrent.futures
                            with concurrent.futures.ThreadPoolExecutor() as pool:
                                auth = pool.submit(self._run_in_new_loop, get_auth_context).result()
                        except RuntimeError:
                            # No running event loop, safe to use asyncio.run
                            auth = asyncio.run(get_auth_context())
                        databricks_host = auth.workspace_url if auth else None
                    except Exception as e:
                        logger.debug(f"Unified auth not available: {e}")
                        databricks_host = None

                    # If not in environment, try to get from DatabricksService
                    if not databricks_host:
                        try:
                            # Try to get from DatabricksService configuration
                            from src.services.databricks_service import DatabricksService
                            from src.db.session import async_session_factory

                            async def get_databricks_config():
                                async with async_session_factory() as session:
                                    service = DatabricksService(session)
                                    config = await service.get_databricks_config()
                                    if config and config.workspace_url:
                                        workspace_url = config.workspace_url.rstrip('/')
                                        if not workspace_url.startswith('https://'):
                                            workspace_url = f"https://{workspace_url}"
                                        return workspace_url
                                return None

                            # Execute the async function
                            try:
                                # Check if we're in an async context
                                asyncio.get_running_loop()
                                # Use ThreadPoolExecutor to call async method from sync context
                                import concurrent.futures
                                with concurrent.futures.ThreadPoolExecutor() as pool:
                                    databricks_host = pool.submit(
                                        self._run_in_new_loop,
                                        get_databricks_config
                                    ).result()
                            except RuntimeError:
                                # Not in async context
                                loop = asyncio.new_event_loop()
                                try:
                                    asyncio.set_event_loop(loop)
                                    databricks_host = loop.run_until_complete(get_databricks_config())
                                finally:
                                    loop.close()

                            if databricks_host:
                                logger.info(f"Retrieved DATABRICKS_HOST from DatabricksService: {databricks_host}")
                                # Add to the tool config copy
                                databricks_jobs_config['DATABRICKS_HOST'] = databricks_host
                            else:
                                logger.warning("Could not retrieve DATABRICKS_HOST from DatabricksService")

                        except Exception as e:
                            logger.error(f"Error getting DATABRICKS_HOST from service: {e}")

                # Create the tool with the same pattern as other Databricks tools
                logger.info(f"Creating DatabricksJobsTool with tool_config: {databricks_jobs_config}")
                return tool_class(
                    databricks_host=databricks_host,
                    tool_config=databricks_jobs_config,
                    user_token=user_token,
                    result_as_answer=result_as_answer
                )


            # NOTE: DatabricksKnowledgeSearchTool is handled later in the method (see line ~1190)
            # This block was a duplicate and has been removed to avoid confusion

            elif tool_name == "GenieTool":
                # Get tool ID if any
                tool_id = tool_config.get('tool_id', None)

                # Log the raw tool_config to debug spaceId issue
                logger.info(f"GenieTool raw tool_config: {tool_config}")
                logger.info(f"GenieTool tool_config_override: {tool_config_override}")

                # Create a copy of the config
                genie_tool_config = {**tool_config}

                # Try to get user token from multiple sources for OAuth/OBO authentication
                user_token = tool_config.get('user_token') or self.user_token

                # CRITICAL: Extract group_id from config for PAT authentication fallback
                # This is essential for tools running in CrewAI threads where UserContext is unavailable
                group_id = None
                if isinstance(self.config, dict):
                    group_id = self.config.get('group_id')
                    if group_id:
                        logger.info(f"Extracted group_id from factory config for GenieTool: {group_id}")
                    else:
                        logger.warning("No group_id in factory config - PAT authentication may fail")

                # If no user token in config or factory, try to get from context
                if not user_token:
                    try:
                        from src.utils.user_context import UserContext
                        user_token = UserContext.get_user_token()
                        if user_token:
                            logger.info(f"Extracted user token from context for GenieTool OBO authentication: {user_token[:10]}...")
                        else:
                            logger.warning("No user token found in context for GenieTool")
                            # Also check if group context has a token
                            group_context = UserContext.get_group_context()
                            if group_context and group_context.access_token:
                                user_token = group_context.access_token
                                logger.info(f"Found user token in group context: {user_token[:10]}...")
                            else:
                                logger.warning("No user token in group context either")
                    except Exception as e:
                        logger.error(f"Could not extract user token from context: {e}")
                        import traceback
                        logger.error(traceback.format_exc())

                # Check if we should use OAuth authentication (Databricks Apps environment)
                use_oauth = bool(user_token)

                # If we don't have a user token, try traditional API key approach
                if not use_oauth:
                    # Get API key from tool config
                    api_key = tool_config.get('api_key', '')

                    # Get API key from unified auth
                    databricks_api_key = None
                    try:
                        from src.utils.databricks_auth import get_auth_context
                        # Check if we're in an async context
                        try:
                            asyncio.get_running_loop()
                            # We're in an event loop, use thread pool
                            import concurrent.futures
                            with concurrent.futures.ThreadPoolExecutor() as pool:
                                auth = pool.submit(self._run_in_new_loop, get_auth_context).result()
                        except RuntimeError:
                            # No running event loop, safe to use asyncio.run
                            auth = asyncio.run(get_auth_context())
                        databricks_api_key = auth.token if auth else None
                    except Exception as e:
                        logger.debug(f"Unified auth not available: {e}")

                    # If not found in environment, try to get it from the service
                    if not databricks_api_key and not api_key:
                        # Use the API keys service if provided, otherwise use the normal methods
                        if self.api_keys_service is not None:
                            logger.info("Using ApiKeysService to get DATABRICKS_API_KEY")
                            try:
                                # Check if we're in an async context
                                asyncio.get_running_loop()
                                # Use ThreadPoolExecutor to call async method from sync context
                                import concurrent.futures
                                with concurrent.futures.ThreadPoolExecutor() as pool:
                                    db_api_key = pool.submit(
                                        self._run_in_new_loop,
                                        self._get_api_key_async,
                                        "DATABRICKS_API_KEY"
                                    ).result()
                            except RuntimeError:
                                # Not in async context
                                loop = asyncio.new_event_loop()
                                try:
                                    asyncio.set_event_loop(loop)
                                    db_api_key = loop.run_until_complete(
                                        self._get_api_key_async("DATABRICKS_API_KEY")
                                    )
                                finally:
                                    loop.close()
                        else:
                            # Fallback to original method
                            logger.warning("DATABRICKS_API_KEY not found via service")
                            try:
                                # Check if we're already in an event loop
                                current_loop = asyncio.get_running_loop()
                                # We're in an async context, use ThreadPoolExecutor
                                import concurrent.futures
                                with concurrent.futures.ThreadPoolExecutor() as pool:
                                    db_api_key = pool.submit(self._run_in_new_loop,
                                                            self._get_api_key_async,
                                                            "DATABRICKS_API_KEY").result()
                            except RuntimeError:
                                # We're not in an async context, use direct method
                                db_api_key = self._get_api_key("DATABRICKS_API_KEY")

                            if db_api_key:
                                # NOTE: Do NOT set os.environ["DATABRICKS_API_KEY"] as it causes race conditions
                                # The API key is passed directly to tool config below instead
                                databricks_api_key = db_api_key

                    # Use tool configuration or environment
                    final_api_key = api_key or databricks_api_key

                    # Add api key to config
                    if final_api_key:
                        genie_tool_config['DATABRICKS_API_KEY'] = final_api_key

                # DATABRICKS_HOST - check config first, then environment variable
                if 'DATABRICKS_HOST' in tool_config:
                    genie_tool_config['DATABRICKS_HOST'] = tool_config['DATABRICKS_HOST']
                    logger.info(f"Using DATABRICKS_HOST from config: {tool_config['DATABRICKS_HOST']}")
                else:
                    # Try to get from unified auth
                    try:
                        from src.utils.databricks_auth import get_auth_context
                        # Check if we're in an async context
                        try:
                            asyncio.get_running_loop()
                            # We're in an event loop, use thread pool
                            import concurrent.futures
                            with concurrent.futures.ThreadPoolExecutor() as pool:
                                auth = pool.submit(self._run_in_new_loop, get_auth_context).result()
                        except RuntimeError:
                            # No running event loop, safe to use asyncio.run
                            auth = asyncio.run(get_auth_context())
                        databricks_host = auth.workspace_url if auth else None
                        if databricks_host:
                            genie_tool_config['DATABRICKS_HOST'] = databricks_host
                            logger.info(f"Using DATABRICKS_HOST from unified auth: {databricks_host}")
                    except Exception as e:
                        logger.debug(f"Unified auth not available: {e}")
                        databricks_host = None
                    else:
                        logger.info("DATABRICKS_HOST not in config or environment - GenieTool will auto-detect if in Databricks Apps")

                # Check for spaceId in tool_config_override first (task/agent specific), then in base tool_config
                if tool_config_override and 'spaceId' in tool_config_override:
                    genie_tool_config['spaceId'] = tool_config_override['spaceId']
                    logger.info(f"Using spaceId from tool_config_override: {tool_config_override['spaceId']}")
                elif tool_config_override and 'space_id' in tool_config_override:
                    # Also check for space_id with underscore
                    genie_tool_config['spaceId'] = tool_config_override['space_id']
                    logger.info(f"Using space_id (underscore) from tool_config_override: {tool_config_override['space_id']}")
                elif 'spaceId' in tool_config:
                    genie_tool_config['spaceId'] = tool_config['spaceId']
                    logger.info(f"Using spaceId from base tool_config: {tool_config['spaceId']}")
                elif 'space_id' in tool_config:
                    # Also check for space_id with underscore in base config
                    genie_tool_config['spaceId'] = tool_config['space_id']
                    logger.info(f"Using space_id (underscore) from base tool_config: {tool_config['space_id']}")
                else:
                    logger.warning("No spaceId or space_id found in tool_config_override or base tool_config")
                    logger.warning(f"tool_config keys: {list(tool_config.keys())}")
                    logger.warning(f"tool_config_override keys: {list(tool_config_override.keys()) if tool_config_override else 'None'}")
                # No default spaceId - must be configured in agent/task

                # Create the GenieTool instance
                try:
                    logger.info(f"Creating GenieTool with config, OBO: {bool(user_token)}, token preview: {user_token[:10] + '...' if user_token else 'None'}, group_id: {group_id}")
                    logger.info(f"GenieTool config being passed: {genie_tool_config}")
                    return tool_class(
                        tool_config=genie_tool_config,
                        tool_id=tool_id,
                        token_required=False,
                        user_token=user_token,
                        group_id=group_id,  # CRITICAL: Pass group_id for PAT authentication
                        result_as_answer=result_as_answer
                    )
                except Exception as e:
                    logger.error(f"Error creating GenieTool: {e}")
                    return None

            elif tool_name == "AgentBricksTool":
                # Get tool ID if any
                tool_id = tool_config.get('tool_id', None)

                # Log the raw tool_config to debug endpointName issue
                logger.info(f"AgentBricksTool raw tool_config: {tool_config}")
                logger.info(f"AgentBricksTool tool_config_override: {tool_config_override}")

                # Create a copy of the config
                agentbricks_tool_config = {**tool_config}

                # Try to get user token from multiple sources for OAuth/OBO authentication
                user_token = tool_config.get('user_token') or self.user_token

                # CRITICAL: Extract group_id from config for PAT authentication fallback
                # This is essential for tools running in CrewAI threads where UserContext is unavailable
                group_id = None
                if isinstance(self.config, dict):
                    group_id = self.config.get('group_id')
                    if group_id:
                        logger.info(f"Extracted group_id from factory config for AgentBricksTool: {group_id}")
                    else:
                        logger.warning("No group_id in factory config - PAT authentication may fail")

                # If no user token in config or factory, try to get from context
                if not user_token:
                    try:
                        from src.utils.user_context import UserContext
                        user_token = UserContext.get_user_token()
                        if user_token:
                            logger.info(f"Extracted user token from context for AgentBricksTool OBO authentication: {user_token[:10]}...")
                        else:
                            logger.warning("No user token found in context for AgentBricksTool")
                            # Also check if group context has a token
                            group_context = UserContext.get_group_context()
                            if group_context and group_context.access_token:
                                user_token = group_context.access_token
                                logger.info(f"Found user token in group context: {user_token[:10]}...")
                            else:
                                logger.warning("No user token in group context either")
                    except Exception as e:
                        logger.error(f"Could not extract user token from context: {e}")
                        import traceback
                        logger.error(traceback.format_exc())

                # Check for endpointName in tool_config_override first (task/agent specific), then in base tool_config
                if tool_config_override and 'endpointName' in tool_config_override:
                    agentbricks_tool_config['endpointName'] = tool_config_override['endpointName']
                    logger.info(f"Using endpointName from tool_config_override: {tool_config_override['endpointName']}")
                elif tool_config_override and 'endpoint_name' in tool_config_override:
                    # Also check for endpoint_name with underscore
                    agentbricks_tool_config['endpointName'] = tool_config_override['endpoint_name']
                    logger.info(f"Using endpoint_name (underscore) from tool_config_override: {tool_config_override['endpoint_name']}")
                elif 'endpointName' in tool_config:
                    agentbricks_tool_config['endpointName'] = tool_config['endpointName']
                    logger.info(f"Using endpointName from base tool_config: {tool_config['endpointName']}")
                elif 'endpoint_name' in tool_config:
                    # Also check for endpoint_name with underscore in base config
                    agentbricks_tool_config['endpointName'] = tool_config['endpoint_name']
                    logger.info(f"Using endpoint_name (underscore) from base tool_config: {tool_config['endpoint_name']}")
                else:
                    logger.warning("No endpointName or endpoint_name found in tool_config_override or base tool_config")
                    logger.warning(f"tool_config keys: {list(tool_config.keys())}")
                    logger.warning(f"tool_config_override keys: {list(tool_config_override.keys()) if tool_config_override else 'None'}")
                # No default endpointName - must be configured in agent/task

                # Create the AgentBricksTool instance
                try:
                    logger.info(f"Creating AgentBricksTool with config, OBO: {bool(user_token)}, token preview: {user_token[:10] + '...' if user_token else 'None'}, group_id: {group_id}")
                    logger.info(f"AgentBricksTool config being passed: {agentbricks_tool_config}")
                    return tool_class(
                        tool_config=agentbricks_tool_config,
                        tool_id=tool_id,
                        token_required=False,
                        user_token=user_token,
                        group_id=group_id,  # CRITICAL: Pass group_id for PAT authentication
                        result_as_answer=result_as_answer
                    )
                except Exception as e:
                    logger.error(f"Error creating AgentBricksTool: {e}")
                    return None

            elif tool_name == "DatabricksKnowledgeSearchTool":
                # Create the tool with group_id and user_token
                # NOTE: We do NOT pass execution_id as it prevents searching across all knowledge documents
                # The tool should search ALL documents for the group, not just current execution

                # Validate Databricks authentication before creating tool
                try:
                    # Check if we're in an async context
                    try:
                        asyncio.get_running_loop()
                        # We're in an event loop, use thread pool
                        import concurrent.futures
                        with concurrent.futures.ThreadPoolExecutor() as pool:
                            auth_valid, auth_message = pool.submit(
                                self._run_in_new_loop,
                                self._validate_databricks_auth
                            ).result()
                    except RuntimeError:
                        # No running event loop, safe to use asyncio.run
                        loop = asyncio.new_event_loop()
                        try:
                            asyncio.set_event_loop(loop)
                            auth_valid, auth_message = loop.run_until_complete(self._validate_databricks_auth())
                        finally:
                            loop.close()

                    if not auth_valid:
                        logger.warning(f"[TOOL_FACTORY] Databricks authentication validation failed: {auth_message}")
                        logger.warning(f"[TOOL_FACTORY] Proceeding with tool creation - authentication may be available through environment variables or fallback methods")
                    else:
                        logger.info(f"[TOOL_FACTORY] Databricks authentication validated: {auth_message}")

                except Exception as e:
                    logger.error(f"[TOOL_FACTORY] Error validating Databricks auth: {e}", exc_info=True)
                    logger.warning(f"[TOOL_FACTORY] Proceeding with tool creation despite validation error")

                # CRITICAL DEBUG: Print to stdout
                print(f"[TOOL_FACTORY] ========================================")
                print(f"[TOOL_FACTORY] Creating DatabricksKnowledgeSearchTool")
                print(f"[TOOL_FACTORY]   - tool_config received: {tool_config}")
                print(f"[TOOL_FACTORY]   - tool_config type: {type(tool_config)}")

                tool_args = {
                    "group_id": self.config.get('group_id', 'default'),
                    # DO NOT PASS execution_id - we want to search all documents!
                    # "execution_id": self.config.get('execution_id') or self.config.get('run_id'),
                    "user_token": self.user_token
                }
                # Add any tool-specific config (includes file_paths and agent_id from task tool_configs)
                if tool_config and isinstance(tool_config, dict):
                    print(f"[TOOL_FACTORY]   - Merging tool_config into tool_args")
                    tool_args.update(tool_config)
                else:
                    print(f"[TOOL_FACTORY]   - tool_config is empty or not a dict, NOT merging")

                print(f"[TOOL_FACTORY] Final tool_args: {tool_args}")
                print(f"[TOOL_FACTORY]   - group_id: {tool_args.get('group_id')}")
                print(f"[TOOL_FACTORY]   - file_paths: {tool_args.get('file_paths')}")
                print(f"[TOOL_FACTORY]   - agent_id: {tool_args.get('agent_id')}")
                print(f"[TOOL_FACTORY] ========================================")

                tool = DatabricksKnowledgeSearchTool(**tool_args)
                return tool

            elif tool_name == "PowerBIAnalysisTool":
                # Create PowerBIAnalysisTool with Power BI and LLM configuration
                # This tool converts business questions into DAX queries and executes them
                tool_args = {}

                try:
                    # Extract PowerBI config from tool_config (merged base + override)
                    if tool_config and isinstance(tool_config, dict):
                        # Power BI Configuration
                        tool_args["workspace_id"] = tool_config.get("workspace_id")
                        tool_args["dataset_id"] = tool_config.get("dataset_id")

                        # Service Principal Authentication
                        tool_args["tenant_id"] = tool_config.get("tenant_id")
                        tool_args["client_id"] = tool_config.get("client_id")
                        tool_args["client_secret"] = tool_config.get("client_secret")

                        # OAuth Authentication (alternative)
                        tool_args["access_token"] = tool_config.get("access_token")

                        # LLM Configuration for DAX generation
                        tool_args["llm_workspace_url"] = tool_config.get("llm_workspace_url")
                        tool_args["llm_token"] = tool_config.get("llm_token")
                        tool_args["llm_model"] = tool_config.get("llm_model", "databricks-claude-sonnet-4")

                        # Options
                        tool_args["include_visual_references"] = tool_config.get("include_visual_references", True)
                        tool_args["skip_system_tables"] = tool_config.get("skip_system_tables", True)
                        tool_args["output_format"] = tool_config.get("output_format", "markdown")

                        # User Question (pre-configured question from frontend)
                        tool_args["user_question"] = tool_config.get("user_question")

                    # Allow tool_config_override to override specific fields
                    if isinstance(tool_config_override, dict):
                        for key in ["workspace_id", "dataset_id", "tenant_id", "client_id",
                                    "client_secret", "access_token", "llm_workspace_url",
                                    "llm_token", "llm_model", "include_visual_references",
                                    "skip_system_tables", "output_format", "user_question"]:
                            if key in tool_config_override:
                                tool_args[key] = tool_config_override[key]

                    # Filter out None values
                    tool_args = {k: v for k, v in tool_args.items() if v is not None}

                except Exception as e:
                    logger.error(f"Error extracting PowerBI Analysis config: {e}")
                    tool_args = {}

                logger.info(f"Creating PowerBIAnalysisTool with workspace_id: {tool_args.get('workspace_id')}, "
                           f"dataset_id: {tool_args.get('dataset_id')}, "
                           f"tenant_id: {'***' if tool_args.get('tenant_id') else None}, "
                           f"has_access_token: {bool(tool_args.get('access_token'))}, "
                           f"llm_configured: {bool(tool_args.get('llm_workspace_url'))}, "
                           f"user_question: {tool_args.get('user_question', 'NOT SET')}")
                return tool_class(**tool_args)

            elif tool_name == "MCPTool":
                # MCPTool might need special configuration
                # Check if MCPTool exists and can be created
                if MCPTool is None:
                    logger.error("MCPTool is not available - MCP integration may not be installed")
                    return None

                # Create MCPTool with configuration
                tool_config['result_as_answer'] = result_as_answer
                logger.info(f"Creating MCPTool with config: {tool_config}")
                return tool_class(**tool_config)

            # Power BI Connector Tool
            elif tool_name == "PowerBIConnectorTool":
                # PowerBIConnectorTool accepts configuration directly
                tool_config['result_as_answer'] = result_as_answer
                logger.info(f"Creating PowerBIConnectorTool with config: {tool_config}")
                return tool_class(**tool_config)

            # Universal Measure Conversion Pipeline
            elif tool_name == "Measure Conversion Pipeline":
                # MeasureConversionPipelineTool accepts configuration directly
                tool_config['result_as_answer'] = result_as_answer

                # Enhanced logging to track tool configuration
                logger.info(f"[ToolFactory] Creating Measure Conversion Pipeline with merged config")
                logger.info(f"[ToolFactory]   - inbound_connector: {tool_config.get('inbound_connector', 'NOT SET')}")
                logger.info(f"[ToolFactory]   - outbound_format: {tool_config.get('outbound_format', 'NOT SET')}")
                logger.info(f"[ToolFactory]   - powerbi_semantic_model_id: {tool_config.get('powerbi_semantic_model_id', 'NOT SET')[:30] if tool_config.get('powerbi_semantic_model_id') else 'NOT SET'}...")
                logger.info(f"[ToolFactory]   - powerbi_group_id: {tool_config.get('powerbi_group_id', 'NOT SET')[:30] if tool_config.get('powerbi_group_id') else 'NOT SET'}...")
                logger.info(f"[ToolFactory]   - powerbi_client_id: {tool_config.get('powerbi_client_id', 'NOT SET')[:20] if tool_config.get('powerbi_client_id') else 'NOT SET'}...")
                logger.info(f"[ToolFactory]   - powerbi_tenant_id: {tool_config.get('powerbi_tenant_id', 'NOT SET')[:20] if tool_config.get('powerbi_tenant_id') else 'NOT SET'}...")

                # Verify that Service Principal credentials are present before creating the tool
                has_powerbi_creds = bool(
                    tool_config.get('powerbi_semantic_model_id') and
                    tool_config.get('powerbi_group_id') and
                    tool_config.get('powerbi_client_id') and
                    tool_config.get('powerbi_tenant_id') and
                    tool_config.get('powerbi_client_secret')
                )
                logger.info(f"[ToolFactory]   - Power BI Service Principal credentials present: {has_powerbi_creds}")

                # Create the tool with the merged configuration
                try:
                    tool_instance = tool_class(**tool_config)
                    logger.info(f"[ToolFactory] ✓ Successfully created Measure Conversion Pipeline tool instance")
                    return tool_instance
                except Exception as e:
                    logger.error(f"[ToolFactory] ✗ Failed to create Measure Conversion Pipeline: {e}")
                    import traceback
                    logger.error(f"[ToolFactory] Traceback: {traceback.format_exc()}")
                    raise

            # M-Query Conversion Pipeline
            elif tool_name == "M-Query Conversion Pipeline":
                # MqueryConversionPipelineTool accepts configuration directly
                tool_config['result_as_answer'] = result_as_answer

                # Enhanced logging to track tool configuration
                logger.info(f"[ToolFactory] Creating M-Query Conversion Pipeline with merged config")
                logger.info(f"[ToolFactory]   - workspace_id: {tool_config.get('workspace_id', 'NOT SET')[:30] if tool_config.get('workspace_id') else 'NOT SET'}...")
                logger.info(f"[ToolFactory]   - dataset_id: {tool_config.get('dataset_id', 'NOT SET')[:30] if tool_config.get('dataset_id') else 'NOT SET'}...")
                logger.info(f"[ToolFactory]   - tenant_id: {tool_config.get('tenant_id', 'NOT SET')[:20] if tool_config.get('tenant_id') else 'NOT SET'}...")
                logger.info(f"[ToolFactory]   - client_id: {tool_config.get('client_id', 'NOT SET')[:20] if tool_config.get('client_id') else 'NOT SET'}...")
                logger.info(f"[ToolFactory]   - target_catalog: {tool_config.get('target_catalog', 'NOT SET')}")
                logger.info(f"[ToolFactory]   - target_schema: {tool_config.get('target_schema', 'NOT SET')}")

                # Verify that Service Principal credentials are present before creating the tool
                has_admin_api_creds = bool(
                    tool_config.get('workspace_id') and
                    tool_config.get('client_id') and
                    tool_config.get('tenant_id') and
                    tool_config.get('client_secret')
                )
                logger.info(f"[ToolFactory]   - Power BI Admin API credentials present: {has_admin_api_creds}")

                # Create the tool with the merged configuration
                try:
                    tool_instance = tool_class(**tool_config)
                    logger.info(f"[ToolFactory] ✓ Successfully created M-Query Conversion Pipeline tool instance")
                    return tool_instance
                except Exception as e:
                    logger.error(f"[ToolFactory] ✗ Failed to create M-Query Conversion Pipeline: {e}")
                    import traceback
                    logger.error(f"[ToolFactory] Traceback: {traceback.format_exc()}")
                    raise

            # Power BI Relationships Tool
            elif tool_name == "Power BI Relationships Tool":
                # PowerBIRelationshipsTool accepts configuration directly
                tool_config['result_as_answer'] = result_as_answer

                # Enhanced logging to track tool configuration
                logger.info(f"[ToolFactory] Creating Power BI Relationships Tool with merged config")
                logger.info(f"[ToolFactory]   - workspace_id: {tool_config.get('workspace_id', 'NOT SET')[:30] if tool_config.get('workspace_id') else 'NOT SET'}...")
                logger.info(f"[ToolFactory]   - dataset_id: {tool_config.get('dataset_id', 'NOT SET')[:30] if tool_config.get('dataset_id') else 'NOT SET'}...")
                logger.info(f"[ToolFactory]   - tenant_id: {tool_config.get('tenant_id', 'NOT SET')[:20] if tool_config.get('tenant_id') else 'NOT SET'}...")
                logger.info(f"[ToolFactory]   - client_id: {tool_config.get('client_id', 'NOT SET')[:20] if tool_config.get('client_id') else 'NOT SET'}...")
                logger.info(f"[ToolFactory]   - target_catalog: {tool_config.get('target_catalog', 'NOT SET')}")
                logger.info(f"[ToolFactory]   - target_schema: {tool_config.get('target_schema', 'NOT SET')}")

                # Verify that Service Principal credentials are present
                has_sp_creds = bool(
                    tool_config.get('workspace_id') and
                    tool_config.get('dataset_id') and
                    tool_config.get('client_id') and
                    tool_config.get('tenant_id') and
                    tool_config.get('client_secret')
                )
                logger.info(f"[ToolFactory]   - Service Principal credentials present: {has_sp_creds}")

                # Create the tool with the merged configuration
                try:
                    tool_instance = tool_class(**tool_config)
                    logger.info(f"[ToolFactory] ✓ Successfully created Power BI Relationships Tool instance")
                    return tool_instance
                except Exception as e:
                    logger.error(f"[ToolFactory] ✗ Failed to create Power BI Relationships Tool: {e}")
                    import traceback
                    logger.error(f"[ToolFactory] Traceback: {traceback.format_exc()}")
                    raise

            # Power BI Hierarchies Tool
            elif tool_name == "Power BI Hierarchies Tool":
                # PowerBIHierarchiesTool accepts configuration directly
                tool_config['result_as_answer'] = result_as_answer

                # Enhanced logging to track tool configuration
                logger.info(f"[ToolFactory] Creating Power BI Hierarchies Tool with merged config")
                logger.info(f"[ToolFactory]   - workspace_id: {tool_config.get('workspace_id', 'NOT SET')[:30] if tool_config.get('workspace_id') else 'NOT SET'}...")
                logger.info(f"[ToolFactory]   - dataset_id: {tool_config.get('dataset_id', 'NOT SET')[:30] if tool_config.get('dataset_id') else 'NOT SET'}...")
                logger.info(f"[ToolFactory]   - tenant_id: {tool_config.get('tenant_id', 'NOT SET')[:20] if tool_config.get('tenant_id') else 'NOT SET'}...")
                logger.info(f"[ToolFactory]   - client_id: {tool_config.get('client_id', 'NOT SET')[:20] if tool_config.get('client_id') else 'NOT SET'}...")
                logger.info(f"[ToolFactory]   - target_catalog: {tool_config.get('target_catalog', 'NOT SET')}")
                logger.info(f"[ToolFactory]   - target_schema: {tool_config.get('target_schema', 'NOT SET')}")

                # Verify that Service Principal credentials are present
                has_sp_creds = bool(
                    tool_config.get('workspace_id') and
                    tool_config.get('dataset_id') and
                    tool_config.get('client_id') and
                    tool_config.get('tenant_id') and
                    tool_config.get('client_secret')
                )
                logger.info(f"[ToolFactory]   - Service Principal credentials present: {has_sp_creds}")

                # Create the tool with the merged configuration
                try:
                    tool_instance = tool_class(**tool_config)
                    logger.info(f"[ToolFactory] ✓ Successfully created Power BI Hierarchies Tool instance")
                    return tool_instance
                except Exception as e:
                    logger.error(f"[ToolFactory] ✗ Failed to create Power BI Hierarchies Tool: {e}")
                    import traceback
                    logger.error(f"[ToolFactory] Traceback: {traceback.format_exc()}")
                    raise

            # Power BI Field Parameters & Calculation Groups Tool
            elif tool_name == "Power BI Field Parameters & Calculation Groups Tool":
                # Tool accepts configuration directly
                tool_config['result_as_answer'] = result_as_answer

                # Enhanced logging to track tool configuration
                logger.info(f"[ToolFactory] Creating Power BI Field Parameters & Calculation Groups Tool with merged config")
                logger.info(f"[ToolFactory]   - workspace_id: {tool_config.get('workspace_id', 'NOT SET')[:30] if tool_config.get('workspace_id') else 'NOT SET'}...")
                logger.info(f"[ToolFactory]   - dataset_id: {tool_config.get('dataset_id', 'NOT SET')[:30] if tool_config.get('dataset_id') else 'NOT SET'}...")
                logger.info(f"[ToolFactory]   - tenant_id: {tool_config.get('tenant_id', 'NOT SET')[:20] if tool_config.get('tenant_id') else 'NOT SET'}...")
                logger.info(f"[ToolFactory]   - client_id: {tool_config.get('client_id', 'NOT SET')[:20] if tool_config.get('client_id') else 'NOT SET'}...")
                logger.info(f"[ToolFactory]   - target_catalog: {tool_config.get('target_catalog', 'NOT SET')}")
                logger.info(f"[ToolFactory]   - target_schema: {tool_config.get('target_schema', 'NOT SET')}")

                # Verify that Service Principal credentials are present
                has_sp_creds = bool(
                    tool_config.get('workspace_id') and
                    tool_config.get('dataset_id') and
                    tool_config.get('client_id') and
                    tool_config.get('tenant_id') and
                    tool_config.get('client_secret')
                )
                logger.info(f"[ToolFactory]   - Service Principal credentials present: {has_sp_creds}")

                # Create the tool with the merged configuration
                try:
                    tool_instance = tool_class(**tool_config)
                    logger.info(f"[ToolFactory] ✓ Successfully created Power BI Field Parameters & Calculation Groups Tool instance")
                    return tool_instance
                except Exception as e:
                    logger.error(f"[ToolFactory] ✗ Failed to create Power BI Field Parameters & Calculation Groups Tool: {e}")
                    import traceback
                    logger.error(f"[ToolFactory] Traceback: {traceback.format_exc()}")
                    raise

            # For all other tools (ScrapeWebsiteTool, DallETool), try to create with config parameters
            else:
                # Check if the config has any data
                if tool_config and isinstance(tool_config, dict):
                    # Add result_as_answer to tool configuration
                    tool_config['result_as_answer'] = result_as_answer

                    # Create the tool with the config as kwargs
                    logger.info(f"Creating {tool_name} with config parameters: {tool_config}")
                    return tool_class(**tool_config)
                else:
                    # Create with default parameters if no config
                    logger.info(f"Creating {tool_name} with default parameters and result_as_answer={result_as_answer}")
                    return tool_class(result_as_answer=result_as_answer)

        except Exception as e:
            logger.error(f"Error creating tool '{tool_name}': {e}")
            import traceback
            logger.error(traceback.format_exc())
            return None

    def register_tool_implementation(self, tool_name: str, tool_class):
        """Register a tool implementation class for a given tool name"""
        self._tool_implementations[tool_name] = tool_class
        logger.info(f"Registered tool implementation for {tool_name}")

    def register_tool_implementations(self, implementations_dict: Dict[str, object]):
        """Register multiple tool implementations at once"""
        self._tool_implementations.update(implementations_dict)
        logger.info(f"Registered {len(implementations_dict)} tool implementations")

    def cleanup(self):
        """
        Clean up resources used by the factory
        """
        logger.info("Cleaning up tool factory resources")

    def __del__(self):
        """Cleanup resources when the object is garbage collected"""
        self.cleanup()

    async def cleanup_after_crew_execution(self):
        """
        Clean up resources after a crew execution.
        This is intended to be called after a crew has finished its work.
        """
        logger.info("Cleaning up resources after crew execution")

        # Make sure we run the cleanup safely with respect to event loops
        try:
            # Check if we're already in an event loop
            try:
                # We're in an event loop, need to run cleanup carefully
                running_loop = asyncio.get_running_loop()
                logger.info("Running cleanup in existing event loop")

                # Run cleanup in a way that won't block the current event loop
                from concurrent.futures import ThreadPoolExecutor
                with ThreadPoolExecutor() as pool:
                    def run_cleanup():
                        try:
                            self.cleanup()
                            logger.info("Cleanup completed in background thread")
                        except Exception as e:
                            logger.error(f"Error during cleanup in background thread: {str(e)}")

                    # Submit the cleanup task to run in a separate thread
                    pool.submit(run_cleanup)

            except RuntimeError:
                # No running event loop, can clean up directly
                logger.info("Running cleanup directly (no event loop)")
                self.cleanup()

            # Refresh available tools
            await self._load_available_tools_async()

            logger.info("Cleanup after crew execution completed")
        except Exception as e:
            logger.error(f"Error during cleanup after crew execution: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())