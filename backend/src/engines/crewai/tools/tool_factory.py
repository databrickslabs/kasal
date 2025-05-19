from typing import Dict, Optional, Union
from crewai.tools import BaseTool
import logging
import os
import asyncio
import json

# Import all the CrewAI tools
from crewai_tools import (
    DallETool, 
    GithubSearchTool, 
    ScrapeWebsiteTool, 
    CodeInterpreterTool, 
    CSVSearchTool, 
    YoutubeChannelSearchTool,
    YoutubeVideoSearchTool,
    ComposioTool, 
    SerperDevTool,
    FirecrawlScrapeWebsiteTool,
    SpiderTool, 
    WebsiteSearchTool,
    DirectoryReadTool,
    FileWriterTool,
    BrowserbaseLoadTool,
    CodeDocsSearchTool,
    DirectorySearchTool,
    DOCXSearchTool,
    EXASearchTool,
    FileReadTool,
    FirecrawlCrawlWebsiteTool,
    FirecrawlSearchTool,
    TXTSearchTool,
    JSONSearchTool,
    LlamaIndexTool,
    MDXSearchTool,
    PDFSearchTool,
    PGSearchTool,
    RagTool,
    ScrapeElementFromWebsiteTool,
    XMLSearchTool,
    VisionTool,
    AIMindTool,
    ApifyActorsTool,
    BraveSearchTool,
    DatabricksQueryTool,
    HyperbrowserLoadTool,
    LinkupSearchTool,
    MultiOnTool,
    MySQLSearchTool,
    NL2SQLTool,
    PatronusEvalTool,
    PatronusLocalEvaluatorTool,
    PatronusPredefinedCriteriaEvalTool,
    QdrantVectorSearchTool,
    ScrapegraphScrapeTool,
    ScrapflyScrapeWebsiteTool,
    SeleniumScrapingTool,
    SerpApiGoogleSearchTool,
    SerpApiGoogleShoppingTool,
    SerplyJobSearchTool,
    SerplyNewsSearchTool,
    SerplyScholarSearchTool,
    SerplyWebSearchTool,
    SerplyWebpageToMarkdownTool,
    SnowflakeSearchTool,
    WeaviateVectorSearchTool
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
    from .custom.google_slides_tool import GoogleSlidesTool
except ImportError:
    try:
        from .custom.google_slides_tool import GoogleSlidesTool
    except ImportError:
        GoogleSlidesTool = None
        logging.warning("Could not import GoogleSlidesTool")

try:
    from .custom.genie_tool import GenieTool
except ImportError:
    try:
        from .custom.genie_tool import GenieTool
    except ImportError:
        GenieTool = None
        logging.warning("Could not import GenieTool")

try:
    from .custom.sendpulse_tool import SendPulseEmailTool
except ImportError:
    try:
        from .custom.sendpulse_tool import SendPulseEmailTool
    except ImportError:
        SendPulseEmailTool = None
        logging.warning("Could not import SendPulseEmailTool")

try:
    from .custom.zefix_scraping_tool import ZefixScrapingTool
except ImportError:
    try:
        from .custom.zefix_scraping_tool import ZefixScrapingTool
    except ImportError:
        ZefixScrapingTool = None
        logging.warning("Could not import ZefixScrapingTool")

try:
    from .custom.browser_use_tool import BrowserUseTool
except ImportError:
    try:
        from .custom.browser_use_tool import BrowserUseTool
    except ImportError:
        BrowserUseTool = None
        logging.warning("Could not import BrowserUseTool")

try:
    from .custom.nixtla_tool import NixtlaTimeGPTTool
except ImportError:
    try:
        from .custom.nixtla_tool import NixtlaTimeGPTTool
    except ImportError:
        NixtlaTimeGPTTool = None
        logging.warning("Could not import NixtlaTimeGPTTool")

try:
    from .custom.cro_scraping_tool import CROScrapingTool
except ImportError:
    try:
        from .custom.cro_scraping_tool import CROScrapingTool
    except ImportError:
        CROScrapingTool = None
        logging.warning("Could not import CROScrapingTool")

try:
    from .custom.databricks_custom_tool import DatabricksCustomTool
except ImportError:
    try:
        from .custom.databricks_custom_tool import DatabricksCustomTool
    except ImportError:
        DatabricksCustomTool = None
        logging.warning("Could not import DatabricksCustomTool")

try:
    from .custom.python_pptx_tool import PythonPPTXTool
except ImportError:
    try:
        from .custom.python_pptx_tool import PythonPPTXTool
    except ImportError:
        PythonPPTXTool = None
        logging.warning("Could not import PythonPPTXTool")


# Setup logger
logger = logging.getLogger(__name__)

# Import async session factory once
from src.db.session import async_session_factory
from src.services.tool_service import ToolService
from src.services.api_keys_service import ApiKeysService
from src.schemas.tool import ToolUpdate
from src.utils.encryption_utils import EncryptionUtils

class ToolFactory:
    def __init__(self, config, api_keys_service=None):
        """
        Initialize the tool factory with configuration
        
        Args:
            config: Configuration dictionary for the factory
            api_keys_service: Optional ApiKeysService for retrieving API keys
        """
        self.config = config
        self.api_keys_service = api_keys_service
        # Store tools by both ID and title for easy lookup
        self._available_tools: Dict[str, object] = {}
        self._tool_implementations = {}
        
        # Map tool names to their implementations
        self._tool_implementations = {
            "PerplexityTool": PerplexitySearchTool,
            "GoogleSlidesTool": GoogleSlidesTool,
            "Dall-E Tool": DallETool,
            "Vision Tool": VisionTool,
            "GithubSearchTool": GithubSearchTool,
            "ScrapeWebsiteTool": ScrapeWebsiteTool,
            "CodeInterpreterTool": CodeInterpreterTool,
            "CSVSearchTool": CSVSearchTool,
            "NixtlaTimeGPTTool": NixtlaTimeGPTTool,
            "YoutubeChannelSearchTool": YoutubeChannelSearchTool,
            "YoutubeVideoSearchTool": YoutubeVideoSearchTool,
            "GenieTool": GenieTool,
            "ZefixScrapingTool": ZefixScrapingTool,
            "CROScrapingTool": CROScrapingTool,
            "ComposioTool": ComposioTool,
            "SerperDevTool": SerperDevTool,
            "FirecrawlScrapeWebsiteTool": FirecrawlScrapeWebsiteTool,
            "SpiderTool": SpiderTool,
            "WebsiteSearchTool": WebsiteSearchTool,
            "SendPulseEmailTool": SendPulseEmailTool,
            "DirectoryReadTool": DirectoryReadTool,
            "BrowserUseTool": BrowserUseTool,
            "FileWriterTool": FileWriterTool,
            "BrowserbaseLoadTool": BrowserbaseLoadTool,
            "CodeDocsSearchTool": CodeDocsSearchTool,
            "DirectorySearchTool": DirectorySearchTool,
            "DOCXSearchTool": DOCXSearchTool,
            "EXASearchTool": EXASearchTool,
            "FileReadTool": FileReadTool,
            "FirecrawlCrawlWebsiteTool": FirecrawlCrawlWebsiteTool,
            "FirecrawlSearchTool": FirecrawlSearchTool,
            "TXTSearchTool": TXTSearchTool,
            "JSONSearchTool": JSONSearchTool,
            "LlamaIndexTool": LlamaIndexTool,
            "MDXSearchTool": MDXSearchTool,
            "PDFSearchTool": PDFSearchTool,
            "PGSearchTool": PGSearchTool,
            "RagTool": RagTool,
            "ScrapeElementFromWebsiteTool": ScrapeElementFromWebsiteTool,
            "XMLSearchTool": XMLSearchTool,
            "AIMindTool": AIMindTool,
            "ApifyActorsTool": ApifyActorsTool,
            "BraveSearchTool": BraveSearchTool,
            "DatabricksQueryTool": DatabricksQueryTool,
            "DatabricksCustomTool": DatabricksCustomTool,
            "HyperbrowserLoadTool": HyperbrowserLoadTool,
            "LinkupSearchTool": LinkupSearchTool,
            "MultiOnTool": MultiOnTool,
            "MySQLSearchTool": MySQLSearchTool,
            "NL2SQLTool": NL2SQLTool,
            "PatronusEvalTool": PatronusEvalTool,
            "PatronusLocalEvaluatorTool": PatronusLocalEvaluatorTool,
            "PatronusPredefinedCriteriaEvalTool": PatronusPredefinedCriteriaEvalTool,
            "QdrantVectorSearchTool": QdrantVectorSearchTool,
            "ScrapegraphScrapeTool": ScrapegraphScrapeTool,
            "ScrapflyScrapeWebsiteTool": ScrapflyScrapeWebsiteTool,
            "SeleniumScrapingTool": SeleniumScrapingTool,
            "SerpApiGoogleSearchTool": SerpApiGoogleSearchTool,
            "SerpApiGoogleShoppingTool": SerpApiGoogleShoppingTool,
            "SerplyJobSearchTool": SerplyJobSearchTool,
            "SerplyNewsSearchTool": SerplyNewsSearchTool,
            "SerplyScholarSearchTool": SerplyScholarSearchTool,
            "SerplyWebSearchTool": SerplyWebSearchTool,
            "SerplyWebpageToMarkdownTool": SerplyWebpageToMarkdownTool,
            "SnowflakeSearchTool": SnowflakeSearchTool,
            "WeaviateVectorSearchTool": WeaviateVectorSearchTool,
            "PythonPPTXTool": PythonPPTXTool
        }
        
        # Initialize _initialized flag
        self._initialized = False
    
    @classmethod
    async def create(cls, config, api_keys_service=None):
        """
        Async factory method to create and initialize a ToolFactory instance.
        
        Args:
            config: Configuration dictionary for the factory
            api_keys_service: Optional ApiKeysService for retrieving API keys
            
        Returns:
            Initialized ToolFactory instance
        """
        instance = cls(config, api_keys_service)
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
                    api_keys_to_load = ["SERPER_API_KEY", "PERPLEXITY_API_KEY", "OPENAI_API_KEY", "FIRECRAWL_API_KEY", "LINKUP_API_KEY", "DATABRICKS_API_KEY"]
                    for key_name in api_keys_to_load:
                        try:
                            # Use utility function to avoid event loop issues
                            from src.utils.asyncio_utils import execute_db_operation_with_fresh_engine
                            
                            async def _get_key_operation(session):
                                # Re-use the api_keys_service but with a fresh session
                                from src.services.api_keys_service import ApiKeysService
                                api_keys_service = ApiKeysService(session)
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
                        api_keys_to_load = ["SERPER_API_KEY", "PERPLEXITY_API_KEY", "OPENAI_API_KEY", "FIRECRAWL_API_KEY", "LINKUP_API_KEY", "DATABRICKS_API_KEY"]
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
            # Get services using Unit of Work pattern
            from src.core.unit_of_work import UnitOfWork
            from src.services.tool_service import ToolService
            
            async with UnitOfWork() as uow:
                # Create tool service from UnitOfWork
                tool_service = await ToolService.from_unit_of_work(uow)
                
                # Get tools through service
                tools_response = await tool_service.get_all_tools()
                tools = tools_response.tools
                
                # Store tools by both title and ID
                self._available_tools = {}
                for tool in tools:
                    self._available_tools[tool.title] = tool
                    self._available_tools[str(tool.id)] = tool  # Convert ID to string since it might come as string from config
                
                logger.info(f"Loaded {len(tools)} tools from service using UnitOfWork")
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
            
            async def _get_key_with_fresh_engine(session):
                from src.services.api_keys_service import ApiKeysService
                api_keys_service = ApiKeysService(session)
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
        # Get services using Unit of Work pattern
        from src.core.unit_of_work import UnitOfWork
        from src.services.tool_service import ToolService
        
        async with UnitOfWork() as uow:
            # Create tool service from UnitOfWork
            tool_service = await ToolService.from_unit_of_work(uow)
            
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
    
    def create_tool(self, tool_identifier: Union[str, int], result_as_answer: bool = False) -> Optional[Union[BaseTool, list]]:
        """
        Create a tool instance based on its identifier.
        
        Args:
            tool_identifier: Either the tool's ID (int or str) or title (str)
            result_as_answer: Whether the tool's result should be treated as the final answer
            
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
            # Get tool config from tool info
            tool_config = tool_info.config if hasattr(tool_info, 'config') else {}
            logger.info(f"{tool_name} config: {tool_config}")
            
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
                
                # Add result_as_answer to tool configuration (only for tools that support it)
                if tool_name != "LinkupSearchTool":
                    tool_config_with_key['result_as_answer'] = result_as_answer
                
                logger.info(f"Creating PerplexityTool with config: {tool_config_with_key}")
                return tool_class(**tool_config_with_key)
            
            elif tool_name == "SerperDevTool":
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
                
                # Add result_as_answer to tool configuration (only for tools that support it)
                if tool_name != "LinkupSearchTool":
                    tool_config_with_key['result_as_answer'] = result_as_answer
                
                return tool_class(**tool_config_with_key)
            
            elif tool_name == "FirecrawlCrawlWebsiteTool":
                # Get API key from tool config
                api_key = tool_config.get('api_key', '')
                
                # Try to get the key from environment first
                firecrawl_api_key = os.environ.get("FIRECRAWL_API_KEY")
                
                # If not found in environment, try to get it from the service
                if not firecrawl_api_key and not api_key:
                    # Use the API keys service if provided, otherwise use the normal methods
                    if self.api_keys_service is not None:
                        logger.info("Using ApiKeysService to get FIRECRAWL_API_KEY")
                        try:
                            # Check if we're in an async context
                            asyncio.get_running_loop()
                            # Use ThreadPoolExecutor to call async method from sync context
                            import concurrent.futures
                            with concurrent.futures.ThreadPoolExecutor() as pool:
                                db_api_key = pool.submit(
                                    self._run_in_new_loop,
                                    self._get_api_key_async, 
                                    "FIRECRAWL_API_KEY"
                                ).result()
                        except RuntimeError:
                            # Not in async context
                            loop = asyncio.new_event_loop()
                            try:
                                asyncio.set_event_loop(loop)
                                db_api_key = loop.run_until_complete(
                                    self._get_api_key_async("FIRECRAWL_API_KEY")
                                )
                            finally:
                                loop.close()
                    else:
                        # Fallback to original method
                        logger.info("No ApiKeysService provided, using fallback method for FIRECRAWL_API_KEY")
                        try:
                            # Check if we're already in an event loop
                            current_loop = asyncio.get_running_loop()
                            # We're in an async context, use ThreadPoolExecutor
                            import concurrent.futures
                            with concurrent.futures.ThreadPoolExecutor() as pool:
                                db_api_key = pool.submit(self._run_in_new_loop, 
                                                        self._get_api_key_async, 
                                                        "FIRECRAWL_API_KEY").result()
                        except RuntimeError:
                            # We're not in an async context, use direct method
                            db_api_key = self._get_api_key("FIRECRAWL_API_KEY")
                        
                        if db_api_key:
                            # Set in environment for tools that read from there
                            os.environ["FIRECRAWL_API_KEY"] = db_api_key
                            firecrawl_api_key = db_api_key
                
                # Use tool configuration or environment
                final_api_key = api_key or firecrawl_api_key
                
                # Add api key to config and create with all parameters from config
                tool_config_with_key = {**tool_config}
                if final_api_key:
                    tool_config_with_key['api_key'] = final_api_key
                
                # Add result_as_answer to tool configuration (only for tools that support it)
                if tool_name != "LinkupSearchTool":
                    tool_config_with_key['result_as_answer'] = result_as_answer
                
                return tool_class(**tool_config_with_key)
            
            elif tool_name == "FileWriterTool":
                # Ensure FileWriterTool uses the configuration provided
                logger.info(f"Creating FileWriterTool with config: {tool_config}")
                
                # Ensure all config parameters are properly passed to the tool
                tool_config_with_defaults = {
                    'default_directory': tool_config.get('default_directory', './file_outputs'),
                    'overwrite': tool_config.get('overwrite', True),
                    'encoding': tool_config.get('encoding', 'utf-8'),
                    'result_as_answer': result_as_answer
                }
                
                logger.info(f"Final FileWriterTool config: {tool_config_with_defaults}")
                return tool_class(**tool_config_with_defaults)
            
            elif tool_name == "NL2SQLTool":
                # Get database URI from tool config
                db_uri = tool_config.get('db_uri', '')
                
                # If db_uri is not provided or empty, use the complete default URI
                if not db_uri:
                    db_uri = "postgresql://postgres:postgres@localhost:5432/app"
                    logger.info(f"No db_uri provided, using default URI: {db_uri}")
                # If db_uri is provided but needs parsing for defaults
                elif '://' in db_uri:
                    try:
                        # Parse the URI to check for username and password
                        protocol_part, rest = db_uri.split('://', 1)
                        
                        # Handle authentication part
                        if '@' in rest:
                            auth_part, host_part = rest.split('@', 1)
                        else:
                            auth_part = ''
                            host_part = rest
                        
                        # Default auth to postgres:postgres if not provided or incomplete
                        if ':' not in auth_part or auth_part == '':
                            auth_part = 'postgres:postgres'
                        
                        # Check for database name
                        if '/' in host_part:
                            server_part, db_name = host_part.rsplit('/', 1)
                            # If database name is empty, use default 'kasal'
                            if not db_name:
                                db_name = 'kasal'
                        else:
                            server_part = host_part
                            db_name = 'kasal'  # Default database name
                        
                        # Reconstruct the URI with defaults
                        db_uri = f"{protocol_part}://{auth_part}@{server_part}/{db_name}"
                        logger.info(f"Using default credentials for NL2SQLTool: {db_uri}")
                    except Exception as e:
                        logger.warning(f"Error parsing db_uri for NL2SQLTool: {e}. Using as is.")
                
                # Create config with the db_uri
                tool_config_with_uri = {**tool_config}
                tool_config_with_uri['db_uri'] = db_uri
                
                # Add result_as_answer to tool configuration
                tool_config_with_uri['result_as_answer'] = result_as_answer
                
                logger.info(f"Creating NL2SQLTool with config: {tool_config_with_uri}")
                return tool_class(**tool_config_with_uri)
            
            elif tool_name == "BrowserUseTool":
                # Pass browser_use_api_url from config
                browser_use_api_url = tool_config.get('browser_use_api_url')
                tool_instance = tool_class.from_config(browser_use_api_url=browser_use_api_url)
                # Set result_as_answer if needed
                if hasattr(tool_instance, 'result_as_answer'):
                    tool_instance.result_as_answer = result_as_answer
                return tool_instance
            
            elif tool_name == "DatabricksCustomTool":
                # Get configuration from tool_config or use defaults
                default_catalog = tool_config.get('catalog')
                default_schema = tool_config.get('schema')
                default_warehouse_id = tool_config.get('warehouse_id')
                
                # Create the tool with parameters
                tool_config_with_defaults = {
                    'default_catalog': default_catalog,
                    'default_schema': default_schema,
                    'default_warehouse_id': default_warehouse_id,
                    'result_as_answer': result_as_answer
                }
                
                logger.info(f"Creating DatabricksCustomTool with config: {tool_config_with_defaults}")
                return tool_class(**tool_config_with_defaults)
            
            elif tool_name == "GenieTool":
                # Get tool ID if any
                tool_id = tool_config.get('tool_id', None)
                
                # Create a copy of the config
                genie_tool_config = {**tool_config}
                
                # Get API key from tool config
                api_key = tool_config.get('api_key', '')
                
                # Try to get the key from environment first
                databricks_api_key = os.environ.get("DATABRICKS_API_KEY")
                
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
                        logger.info("No ApiKeysService provided, using fallback method for DATABRICKS_API_KEY")
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
                            # Set in environment for tools that read from there
                            os.environ["DATABRICKS_API_KEY"] = db_api_key
                            databricks_api_key = db_api_key
                
                # Use tool configuration or environment
                final_api_key = api_key or databricks_api_key
                
                # Add api key to config
                if final_api_key:
                    genie_tool_config['DATABRICKS_API_KEY'] = final_api_key
                
                # Ensure DATABRICKS_HOST and spaceId are in config
                if 'DATABRICKS_HOST' in tool_config:
                    genie_tool_config['DATABRICKS_HOST'] = tool_config['DATABRICKS_HOST']
                    logger.info(f"Using DATABRICKS_HOST from config: {tool_config['DATABRICKS_HOST']}")
                
                if 'spaceId' in tool_config:
                    genie_tool_config['spaceId'] = tool_config['spaceId']
                    logger.info(f"Using spaceId from config: {tool_config['spaceId']}")
                
                # Create the GenieTool instance
                try:
                    logger.info(f"Creating GenieTool with config")
                    return tool_class(tool_config=genie_tool_config, tool_id=tool_id, token_required=False)
                except Exception as e:
                    logger.error(f"Error creating GenieTool: {e}")
                    return None
            
            elif tool_name == "LinkupSearchTool":
                # Get API key from tool config
                api_key = tool_config.get('api_key', '')
                
                # Try to get the key from environment first
                linkup_api_key = os.environ.get("LINKUP_API_KEY")
                
                # If not found in environment, try to get it from the service
                if not linkup_api_key and not api_key:
                    # Use the API keys service if provided, otherwise use the normal methods
                    if self.api_keys_service is not None:
                        logger.info("Using ApiKeysService to get LINKUP_API_KEY")
                        try:
                            # Check if we're in an async context
                            asyncio.get_running_loop()
                            # Use ThreadPoolExecutor to call async method from sync context
                            import concurrent.futures
                            with concurrent.futures.ThreadPoolExecutor() as pool:
                                db_api_key = pool.submit(
                                    self._run_in_new_loop,
                                    self._get_api_key_async, 
                                    "LINKUP_API_KEY"
                                ).result()
                        except RuntimeError:
                            # Not in async context
                            loop = asyncio.new_event_loop()
                            try:
                                asyncio.set_event_loop(loop)
                                db_api_key = loop.run_until_complete(
                                    self._get_api_key_async("LINKUP_API_KEY")
                                )
                            finally:
                                loop.close()
                    else:
                        # Fallback to original method
                        logger.info("No ApiKeysService provided, using fallback method for LINKUP_API_KEY")
                        try:
                            # Check if we're already in an event loop
                            current_loop = asyncio.get_running_loop()
                            # We're in an async context, use ThreadPoolExecutor
                            import concurrent.futures
                            with concurrent.futures.ThreadPoolExecutor() as pool:
                                db_api_key = pool.submit(self._run_in_new_loop, 
                                                        self._get_api_key_async, 
                                                        "LINKUP_API_KEY").result()
                        except RuntimeError:
                            # We're not in an async context, use direct method
                            db_api_key = self._get_api_key("LINKUP_API_KEY")
                        
                        if db_api_key:
                            # Set in environment for tools that read from there
                            os.environ["LINKUP_API_KEY"] = db_api_key
                            linkup_api_key = db_api_key
                
                # Use tool configuration or environment
                final_api_key = api_key or linkup_api_key
                
                # Create a class that wraps the LinkupSearchTool to enforce parameters at runtime
                # This prevents invalid parameters being passed by the agent
                class EnforcedLinkupSearchTool(LinkupSearchTool):
                    def _run(self, query: str, **kwargs):
                        # Looking at the source code, LinkupSearchTool._run accepts these parameters:
                        # query: str, depth: str = "standard", output_type: str = "searchResults"
                        
                        # Get valid values from config or use defaults
                        depth = tool_config.get('depth', 'standard')
                        if depth not in ['standard', 'deep']:
                            logger.warning(f"Invalid depth value '{depth}' in config. Using 'standard' instead.")
                            depth = 'standard'
                        
                        output_type = tool_config.get('output_type', 'searchResults') 
                        if output_type not in ['sourcedAnswer', 'searchResults', 'structured']:
                            logger.warning(f"Invalid output_type value '{output_type}' in config. Using 'searchResults' instead.")
                            output_type = 'searchResults'
                        
                        logger.info(f"Enforcing LinkupSearchTool parameters: depth={depth}, output_type={output_type}")
                        
                        # Call the parent _run method with the correct parameter names
                        # This is critical - we're using output_type (snake_case) here as expected by the method signature
                        return super()._run(query=query, depth=depth, output_type=output_type)
                
                # Create LinkupSearchTool with minimal parameters
                logger.info(f"Creating enforced LinkupSearchTool with API key and enforced parameters")
                tool_args = {"api_key": final_api_key} if final_api_key else {}
                
                # Create the enforced tool rather than the standard one
                return EnforcedLinkupSearchTool(**tool_args)
            
            elif tool_name == "PythonPPTXTool":
                # Create the tool with any specified configuration
                tool = PythonPPTXTool(**tool_config)
                return tool
            
            # For all other tools, try to create with config parameters
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
            import asyncio
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