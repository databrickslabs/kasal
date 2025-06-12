"""
Async wrapper for MCP Server Adapter to eliminate blocking operations.

This module provides an async wrapper around the MCPServerAdapter from crewai_tools
to ensure all operations are non-blocking and compatible with async environments.
"""

import asyncio
import logging
from typing import Optional, List, Any
from concurrent.futures import ThreadPoolExecutor

logger = logging.getLogger(__name__)


class AsyncMCPAdapter:
    """
    Async wrapper for MCPServerAdapter to eliminate blocking operations.
    
    This class wraps the synchronous MCPServerAdapter and runs blocking operations
    in a thread pool to maintain async compatibility throughout the application.
    """
    
    def __init__(self, server_params: dict):
        """
        Initialize the async MCP adapter.
        
        Args:
            server_params: Dictionary containing server configuration parameters
        """
        self.server_params = server_params
        self._adapter = None
        self._tools = None
        self._executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="mcp_adapter")
        
    async def initialize(self):
        """
        Async initialization of the MCP adapter.
        
        This runs the blocking MCPServerAdapter creation in a thread pool
        to avoid blocking the main event loop.
        
        Returns:
            self: Returns self for method chaining
        """
        try:
            logger.info("Initializing AsyncMCPAdapter")
            
            # Import here to avoid import issues
            from crewai_tools import MCPServerAdapter
            
            # Run the blocking adapter creation in a thread pool
            loop = asyncio.get_event_loop()
            
            # Add timeout to prevent indefinite blocking
            try:
                self._adapter = await asyncio.wait_for(
                    loop.run_in_executor(
                        self._executor,
                        lambda: MCPServerAdapter(self.server_params)
                    ),
                    timeout=30.0  # 30 second timeout
                )
            except asyncio.TimeoutError:
                logger.error("AsyncMCPAdapter initialization timed out after 30 seconds")
                raise TimeoutError("MCP adapter initialization timed out")
            
            # Get tools from the adapter
            self._tools = self._adapter.tools
            logger.info(f"AsyncMCPAdapter initialized with {len(self._tools)} tools")
            
            return self
            
        except Exception as e:
            logger.error(f"Error initializing AsyncMCPAdapter: {e}")
            # Don't re-raise the exception to avoid blocking the entire process
            # Instead, set tools to empty list and continue
            self._tools = []
            logger.warning("AsyncMCPAdapter initialization failed, continuing with empty tools")
            return self
    
    @property
    def tools(self) -> Optional[List[Any]]:
        """
        Get the tools from the MCP adapter.
        
        Returns:
            List of tools if adapter is initialized, None otherwise
        """
        return self._tools
    
    async def stop(self):
        """
        Async cleanup of the MCP adapter.
        
        This runs the blocking stop operation in a thread pool
        to avoid blocking the main event loop.
        """
        try:
            if self._adapter:
                logger.info("Stopping AsyncMCPAdapter")
                
                # Run the blocking stop operation in a thread pool
                loop = asyncio.get_event_loop()
                await loop.run_in_executor(
                    self._executor,
                    self._adapter.stop
                )
                
                logger.info("AsyncMCPAdapter stopped successfully")
                
        except Exception as e:
            logger.error(f"Error stopping AsyncMCPAdapter: {e}")
        finally:
            # Shutdown the thread pool executor
            self._executor.shutdown(wait=False)
    
    async def close(self):
        """
        Alias for stop() for compatibility.
        """
        await self.stop()
    
    def __getattr__(self, name):
        """
        Delegate attribute access to the underlying adapter.
        
        This allows the AsyncMCPAdapter to behave like the original adapter
        for any methods or attributes not explicitly overridden.
        """
        if self._adapter:
            return getattr(self._adapter, name)
        raise AttributeError(f"AsyncMCPAdapter has no attribute '{name}' (adapter not initialized)")


class MCPAdapter:
    """
    Legacy compatibility class that creates an AsyncMCPAdapter.
    
    This maintains backward compatibility with existing code that expects
    an MCPAdapter class.
    """
    
    def __init__(self, mcp_url: str, headers: dict):
        """
        Initialize the MCP adapter with URL and headers.
        
        Args:
            mcp_url: The MCP server URL
            headers: Authentication headers
        """
        self.mcp_url = mcp_url
        self.headers = headers
        self._async_adapter = None
    
    async def initialize(self):
        """
        Initialize the async adapter.
        
        Returns:
            self: Returns self for method chaining
        """
        server_params = {"url": self.mcp_url}
        if self.headers:
            server_params["headers"] = self.headers
            
        self._async_adapter = AsyncMCPAdapter(server_params)
        await self._async_adapter.initialize()
        
        return self
    
    @property
    def tools(self):
        """Get tools from the async adapter."""
        if self._async_adapter:
            return self._async_adapter.tools
        return None
    
    async def stop(self):
        """Stop the async adapter."""
        if self._async_adapter:
            await self._async_adapter.stop()
    
    async def close(self):
        """Close the async adapter."""
        await self.stop()
    
    def __getattr__(self, name):
        """Delegate to the async adapter."""
        if self._async_adapter:
            return getattr(self._async_adapter, name)
        raise AttributeError(f"MCPAdapter has no attribute '{name}' (not initialized)")