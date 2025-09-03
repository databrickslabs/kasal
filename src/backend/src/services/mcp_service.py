from typing import List, Optional, Dict, Any
import logging
import aiohttp
import asyncio
import json

from fastapi import HTTPException, status

from src.repositories.mcp_repository import MCPServerRepository, MCPSettingsRepository
from src.schemas.mcp import (
    MCPServerCreate, 
    MCPServerUpdate, 
    MCPServerResponse, 
    MCPServerListResponse,
    MCPToggleResponse,
    MCPTestConnectionRequest,
    MCPTestConnectionResponse,
    MCPSettingsResponse,
    MCPSettingsUpdate
)
from src.utils.encryption_utils import EncryptionUtils

logger = logging.getLogger(__name__)

class MCPService:
    """
    Service for MCP business logic and error handling.
    Acts as an intermediary between the API routers and the repository.
    """
    
    def __init__(self, session=None, server_repository=None, settings_repository=None):
        """
        Initialize service with database session or repositories.
        
        Args:
            session: SQLAlchemy async session (for backwards compatibility)
            server_repository: MCPServerRepository instance (preferred way)
            settings_repository: MCPSettingsRepository instance
        """
        if server_repository is not None and settings_repository is not None:
            self.server_repository = server_repository
            self.settings_repository = settings_repository
        elif session is not None:
            self.server_repository = MCPServerRepository(session)
            self.settings_repository = MCPSettingsRepository(session)
        else:
            raise ValueError("Either session or repositories must be provided")
    
    @classmethod
    async def from_unit_of_work(cls, uow):
        """
        Create a service instance from a UnitOfWork.
        
        Args:
            uow: UnitOfWork instance
            
        Returns:
            MCPService: Service instance using the UnitOfWork's repositories
        """
        return cls(
            server_repository=uow.mcp_server_repository,
            settings_repository=uow.mcp_settings_repository
        )
    
    async def get_all_servers(self) -> MCPServerListResponse:
        """
        Get all MCP servers.
        
        Returns:
            MCPServerListResponse with list of all servers and count
        """
        servers = await self.server_repository.list()
        server_responses = []
        
        for server in servers:
            server_response = MCPServerResponse.model_validate(server)
            # Don't include API key in list response
            server_response.api_key = ""
            server_responses.append(server_response)
            
        return MCPServerListResponse(
            servers=server_responses,
            count=len(servers)
        )
    
    async def get_enabled_servers(self) -> MCPServerListResponse:
        """
        Get all enabled MCP servers.
        
        Returns:
            MCPServerListResponse with list of enabled servers and count
        """
        servers = await self.server_repository.find_enabled()
        server_responses = []
        
        for server in servers:
            server_response = MCPServerResponse.model_validate(server)
            # Don't include API key in list response
            server_response.api_key = ""
            server_responses.append(server_response)
            
        return MCPServerListResponse(
            servers=server_responses,
            count=len(servers)
        )
    
    async def get_global_servers(self) -> MCPServerListResponse:
        """
        Get all globally enabled MCP servers.
        
        Returns:
            MCPServerListResponse with list of globally enabled servers and count
        """
        servers = await self.server_repository.find_global_enabled()
        server_responses = []
        
        for server in servers:
            server_response = MCPServerResponse.model_validate(server)
            # Don't include API key in list response
            server_response.api_key = ""
            server_responses.append(server_response)
            
        return MCPServerListResponse(
            servers=server_responses,
            count=len(servers)
        )
    
    async def get_servers_by_names(self, names: List[str]) -> List[MCPServerResponse]:
        """
        Get MCP servers by a list of names.
        
        Args:
            names: List of server names to retrieve
            
        Returns:
            List of MCPServerResponse objects
        """
        if not names:
            return []
            
        servers = await self.server_repository.find_by_names(names)
        server_responses = []
        
        for server in servers:
            server_response = MCPServerResponse.model_validate(server)
            
            # Decrypt the API key for the response
            try:
                if server.encrypted_api_key:
                    server_response.api_key = EncryptionUtils.decrypt_value(server.encrypted_api_key)
            except Exception as e:
                logger.error(f"Error decrypting API key for server {server.id}: {str(e)}")
                server_response.api_key = ""
                
            server_responses.append(server_response)
            
        return server_responses
    
    async def get_effective_servers(self, explicit_servers: List[str]) -> List[MCPServerResponse]:
        """
        Get effective MCP servers combining global and explicit selections.
        
        Args:
            explicit_servers: List of explicitly selected server names
            
        Returns:
            List of effective MCPServerResponse objects (global + explicit, deduplicated)
        """
        # Get global servers
        global_response = await self.get_global_servers()
        global_names = {server.name for server in global_response.servers}
        
        # Combine global and explicit server names (deduplicated)
        all_server_names = list(global_names.union(set(explicit_servers)))
        
        # Get all servers by names
        return await self.get_servers_by_names(all_server_names)
    
    async def get_server_by_id(self, server_id: int) -> MCPServerResponse:
        """
        Get a MCP server by ID.
        
        Args:
            server_id: ID of the server to retrieve
            
        Returns:
            MCPServerResponse if found
            
        Raises:
            HTTPException: If server not found
        """
        server = await self.server_repository.get(server_id)
        if not server:
            logger.warning(f"MCP server with ID {server_id} not found")
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"MCP server with ID {server_id} not found"
            )
            
        server_response = MCPServerResponse.model_validate(server)
        
        try:
            # Decrypt the API key for the response
            if server.encrypted_api_key:
                server_response.api_key = EncryptionUtils.decrypt_value(server.encrypted_api_key)
        except Exception as e:
            logger.error(f"Error decrypting API key for server {server_id}: {str(e)}")
            server_response.api_key = ""
            
        return server_response
    
    async def create_server(self, server_data: MCPServerCreate) -> MCPServerResponse:
        """
        Create a new MCP server.
        
        Args:
            server_data: Server data for creation
            
        Returns:
            MCPServerResponse of the created server
            
        Raises:
            HTTPException: If server creation fails
        """
        try:
            # Check if a server with the same name already exists
            existing_server = await self.server_repository.find_by_name(server_data.name)
            if existing_server:
                logger.warning(f"MCP server with name '{server_data.name}' already exists")
                raise HTTPException(
                    status_code=status.HTTP_409_CONFLICT,
                    detail=f"MCP server with name '{server_data.name}' already exists"
                )
            
            # Encrypt the API key
            encrypted_api_key = EncryptionUtils.encrypt_value(server_data.api_key)
            
            # Convert server_data to dictionary excluding api_key
            server_dict = server_data.model_dump(exclude={"api_key"})
            
            # Add encrypted API key
            server_dict["encrypted_api_key"] = encrypted_api_key
            
            # Create server
            server = await self.server_repository.create(server_dict)
            
            # Prepare response
            server_response = MCPServerResponse.model_validate(server)
            server_response.api_key = server_data.api_key  # Include the original API key in the response
            
            return server_response
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Failed to create MCP server: {str(e)}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to create MCP server: {str(e)}"
            )
    
    async def update_server(self, server_id: int, server_data: MCPServerUpdate) -> MCPServerResponse:
        """
        Update an existing MCP server.
        
        Args:
            server_id: ID of server to update
            server_data: Server data for update
            
        Returns:
            MCPServerResponse of the updated server
            
        Raises:
            HTTPException: If server not found or update fails
        """
        # Check if server exists
        server = await self.server_repository.get(server_id)
        if not server:
            logger.warning(f"MCP server with ID {server_id} not found for update")
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"MCP server with ID {server_id} not found"
            )
        
        try:
            # Prepare update data
            update_data = server_data.model_dump(exclude_unset=True, exclude={"api_key"})
            
            # If API key is provided and not empty, encrypt it
            # Only update encrypted_api_key if a new API key is actually provided
            if server_data.api_key and server_data.api_key.strip():
                update_data["encrypted_api_key"] = EncryptionUtils.encrypt_value(server_data.api_key)
            
            # Update server
            updated_server = await self.server_repository.update(server_id, update_data)
            
            # Prepare response
            server_response = MCPServerResponse.model_validate(updated_server)
            
            # Decrypt API key for response if one exists
            if updated_server.encrypted_api_key:
                try:
                    server_response.api_key = EncryptionUtils.decrypt_value(updated_server.encrypted_api_key)
                except Exception as e:
                    logger.error(f"Error decrypting API key for server {server_id}: {str(e)}")
                    server_response.api_key = ""
            
            return server_response
        except Exception as e:
            logger.error(f"Failed to update MCP server: {str(e)}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to update MCP server: {str(e)}"
            )
    
    async def delete_server(self, server_id: int) -> bool:
        """
        Delete a MCP server by ID.
        
        Args:
            server_id: ID of server to delete
            
        Returns:
            True if deleted successfully
            
        Raises:
            HTTPException: If server not found or deletion fails
        """
        # Check if server exists
        server = await self.server_repository.get(server_id)
        if not server:
            logger.warning(f"MCP server with ID {server_id} not found for deletion")
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"MCP server with ID {server_id} not found"
            )
        
        try:
            # Delete server
            await self.server_repository.delete(server_id)
            return True
        except Exception as e:
            logger.error(f"Failed to delete MCP server: {str(e)}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to delete MCP server: {str(e)}"
            )
    
    async def toggle_server_enabled(self, server_id: int) -> MCPToggleResponse:
        """
        Toggle the enabled status of a MCP server.
        
        Args:
            server_id: ID of server to toggle
            
        Returns:
            MCPToggleResponse with message and current enabled state
            
        Raises:
            HTTPException: If server not found or toggle fails
        """
        try:
            # Toggle server enabled status using repository
            server = await self.server_repository.toggle_enabled(server_id)
            if not server:
                logger.warning(f"MCP server with ID {server_id} not found for toggle")
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail=f"MCP server with ID {server_id} not found"
                )
            
            status_text = "enabled" if server.enabled else "disabled"
            return MCPToggleResponse(
                message=f"MCP server {status_text} successfully",
                enabled=server.enabled
            )
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Failed to toggle MCP server: {str(e)}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to toggle MCP server: {str(e)}"
            )
    
    async def toggle_server_global_enabled(self, server_id: int) -> MCPToggleResponse:
        """
        Toggle the global enabled status of a MCP server.
        
        Args:
            server_id: ID of server to toggle global enablement
            
        Returns:
            MCPToggleResponse with message and current global enabled state
            
        Raises:
            HTTPException: If server not found or toggle fails
        """
        try:
            # Toggle server global enabled status using repository
            server = await self.server_repository.toggle_global_enabled(server_id)
            if not server:
                logger.warning(f"MCP server with ID {server_id} not found for global toggle")
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail=f"MCP server with ID {server_id} not found"
                )
            
            status_text = "globally enabled" if server.global_enabled else "globally disabled"
            return MCPToggleResponse(
                message=f"MCP server {status_text} successfully",
                enabled=server.global_enabled
            )
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Failed to toggle MCP server global status: {str(e)}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to toggle MCP server global status: {str(e)}"
            )
    
    async def test_connection(self, test_data: MCPTestConnectionRequest) -> MCPTestConnectionResponse:
        """
        Test connection to an MCP server.
        
        Args:
            test_data: Connection test data
            
        Returns:
            MCPTestConnectionResponse with success status and message
        """
        logger.info(f"Testing MCP connection - Type: {test_data.server_type}, URL: {test_data.server_url}")
        logger.debug(f"Connection test parameters - Timeout: {test_data.timeout_seconds}s, Has API key: {bool(test_data.api_key)}")
        
        if test_data.server_type.lower() == "sse":
            return await self._test_sse_connection(test_data)
        elif test_data.server_type.lower() == "streamable":
            return await self._test_streamable_connection(test_data)
        else:
            logger.warning(f"Unsupported MCP server type requested: {test_data.server_type}")
            return MCPTestConnectionResponse(
                success=False,
                message=f"Unsupported server type: {test_data.server_type}"
            )
    
    async def _test_sse_connection(self, test_data: MCPTestConnectionRequest) -> MCPTestConnectionResponse:
        """
        Test connection to an SSE MCP server.
        
        Args:
            test_data: Connection test data
            
        Returns:
            MCPTestConnectionResponse with success status and message
        """
        logger.info(f"Starting SSE connection test to: {test_data.server_url}")
        
        timeout = aiohttp.ClientTimeout(total=test_data.timeout_seconds)
        headers = {}
        
        if test_data.api_key:
            # Add API key to headers
            headers["Authorization"] = f"Bearer {test_data.api_key}"
            logger.debug("Added API key authentication to headers")
        
        try:
            async with aiohttp.ClientSession(timeout=timeout) as session:
                # Attempt to connect to the SSE endpoint
                logger.debug(f"Initiating HTTP GET request to {test_data.server_url}")
                try:
                    async with session.get(test_data.server_url, headers=headers) as response:
                        logger.info(f"Received response - Status: {response.status}, Headers: {dict(response.headers)}")
                        if response.status == 200:
                            # Check for SSE headers or try to read a small amount of data
                            content_type = response.headers.get("Content-Type", "")
                            logger.debug(f"Response Content-Type: {content_type}")
                            
                            if "text/event-stream" in content_type:
                                logger.info("✓ SSE server detected - Content-Type is text/event-stream")
                                return MCPTestConnectionResponse(
                                    success=True,
                                    message="Successfully connected to MCP SSE server"
                                )
                            else:
                                # Try to read some data as a secondary check
                                logger.debug(f"Content-Type '{content_type}' is not text/event-stream, attempting to read data")
                                try:
                                    data = await asyncio.wait_for(
                                        response.content.read(1024), 
                                        timeout=5
                                    )
                                    if data:
                                        logger.info(f"✓ Received {len(data)} bytes of data from server")
                                        logger.debug(f"First 100 bytes of data: {data[:100]}")
                                        return MCPTestConnectionResponse(
                                            success=True,
                                            message="Successfully connected to server, but Content-Type is not text/event-stream"
                                        )
                                except asyncio.TimeoutError:
                                    logger.warning("× No data received within 5 seconds")
                                    return MCPTestConnectionResponse(
                                        success=False,
                                        message="Connection established but no data received"
                                    )
                        else:
                            error_text = await response.text()
                            logger.error(f"× HTTP error {response.status}: {error_text[:200]}")
                            return MCPTestConnectionResponse(
                                success=False,
                                message=f"Failed to connect: HTTP {response.status} - {error_text}"
                            )
                
                except aiohttp.ClientConnectorError as e:
                    logger.error(f"× Connection error: {type(e).__name__}: {str(e)}")
                    return MCPTestConnectionResponse(
                        success=False,
                        message=f"Failed to connect: {str(e)}"
                    )
                except asyncio.TimeoutError:
                    logger.error(f"× Connection timeout after {test_data.timeout_seconds} seconds")
                    return MCPTestConnectionResponse(
                        success=False,
                        message=f"Connection timed out after {test_data.timeout_seconds} seconds"
                    )
        except Exception as e:
            logger.error(f"× Unexpected error testing MCP SSE connection: {type(e).__name__}: {str(e)}", exc_info=True)
            return MCPTestConnectionResponse(
                success=False,
                message=f"Error testing connection: {str(e)}"
            )
    
    async def _test_streamable_connection(self, test_data: MCPTestConnectionRequest) -> MCPTestConnectionResponse:
        """
        Test connection to a Streamable API server.
        
        Args:
            test_data: Connection test data
            
        Returns:
            MCPTestConnectionResponse with success status and message
        """
        logger.info(f"Starting Streamable connection test to: {test_data.server_url}")
        
        timeout = aiohttp.ClientTimeout(total=test_data.timeout_seconds)
        headers = {
            "Accept": "application/json",
            "User-Agent": "Kasal-MCP-Client/1.0"
        }
        
        if test_data.api_key:
            # Add API key to headers - Streamable may use different auth patterns
            headers["Authorization"] = f"Bearer {test_data.api_key}"
            logger.debug("Added API key authentication to headers")
        
        try:
            async with aiohttp.ClientSession(timeout=timeout) as session:
                logger.debug(f"Testing Streamable API endpoint: {test_data.server_url}")
                
                # For Streamable, we'll test the API endpoint
                # Streamable API endpoints include /videos/{shortcode} and /oembed.json
                try:
                    # First try a simple GET to the base URL
                    async with session.get(test_data.server_url, headers=headers) as response:
                        logger.info(f"Received response - Status: {response.status}, Headers: {dict(response.headers)}")
                        
                        if response.status == 200:
                            content_type = response.headers.get("Content-Type", "")
                            logger.debug(f"Response Content-Type: {content_type}")
                            
                            # Check if it's a JSON API response
                            if "application/json" in content_type:
                                try:
                                    data = await response.json()
                                    logger.info(f"✓ Streamable API endpoint verified - received JSON response")
                                    logger.debug(f"API response keys: {list(data.keys()) if isinstance(data, dict) else 'non-dict response'}")
                                    return MCPTestConnectionResponse(
                                        success=True,
                                        message="Successfully connected to Streamable API server"
                                    )
                                except Exception as json_error:
                                    logger.warning(f"Failed to parse JSON response: {json_error}")
                                    return MCPTestConnectionResponse(
                                        success=True,
                                        message="Connected to server but response is not valid JSON"
                                    )
                            else:
                                # Try to read some data
                                logger.debug(f"Non-JSON response received, attempting to validate endpoint")
                                data = await response.text()
                                if data:
                                    logger.info(f"✓ Received {len(data)} characters from Streamable endpoint")
                                    return MCPTestConnectionResponse(
                                        success=True,
                                        message="Successfully connected to Streamable server (non-JSON response)"
                                    )
                        elif response.status == 401:
                            logger.error("× Authentication failed - check API key")
                            return MCPTestConnectionResponse(
                                success=False,
                                message="Authentication failed - please check your API key"
                            )
                        elif response.status == 404:
                            logger.error("× Endpoint not found - check URL")
                            return MCPTestConnectionResponse(
                                success=False,
                                message="Endpoint not found - please check the server URL"
                            )
                        else:
                            error_text = await response.text()
                            logger.error(f"× HTTP error {response.status}: {error_text[:200]}")
                            return MCPTestConnectionResponse(
                                success=False,
                                message=f"Failed to connect: HTTP {response.status} - {error_text}"
                            )
                
                except aiohttp.ClientConnectorError as e:
                    logger.error(f"× Connection error: {type(e).__name__}: {str(e)}")
                    return MCPTestConnectionResponse(
                        success=False,
                        message=f"Failed to connect: {str(e)}"
                    )
                except asyncio.TimeoutError:
                    logger.error(f"× Connection timeout after {test_data.timeout_seconds} seconds")
                    return MCPTestConnectionResponse(
                        success=False,
                        message=f"Connection timed out after {test_data.timeout_seconds} seconds"
                    )
        except Exception as e:
            logger.error(f"× Unexpected error testing Streamable connection: {type(e).__name__}: {str(e)}", exc_info=True)
            return MCPTestConnectionResponse(
                success=False,
                message=f"Error testing connection: {str(e)}"
            )
    
    async def get_settings(self) -> MCPSettingsResponse:
        """
        Get global MCP settings.
        
        Returns:
            MCPSettingsResponse with global settings
        """
        try:
            settings = await self.settings_repository.get_settings()
            return MCPSettingsResponse.model_validate(settings)
        except Exception as e:
            logger.error(f"Error getting MCP settings: {str(e)}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Error getting MCP settings: {str(e)}"
            )
    
    async def update_settings(self, settings_data: MCPSettingsUpdate) -> MCPSettingsResponse:
        """
        Update global MCP settings.
        
        Args:
            settings_data: Settings data for update
            
        Returns:
            MCPSettingsResponse with updated settings
        """
        try:
            # Get current settings
            settings = await self.settings_repository.get_settings()
            
            # Update settings
            update_data = settings_data.model_dump()
            updated_settings = await self.settings_repository.update(settings.id, update_data)
            
            return MCPSettingsResponse.model_validate(updated_settings)
        except Exception as e:
            logger.error(f"Error updating MCP settings: {str(e)}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Error updating MCP settings: {str(e)}"
            ) 