from typing import List, Optional, Dict, Any
import logging
import aiohttp
import asyncio
import requests
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
            
            # If API key is provided, encrypt it
            if server_data.api_key:
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
    
    async def test_connection(self, test_data: MCPTestConnectionRequest) -> MCPTestConnectionResponse:
        """
        Test connection to an MCP server.
        
        Args:
            test_data: Connection test data
            
        Returns:
            MCPTestConnectionResponse with success status and message
        """
        if test_data.server_type.lower() == "sse":
            return await self._test_sse_connection(test_data)
        elif test_data.server_type.lower() == "stdio":
            return await self._test_stdio_connection(test_data)
        else:
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
        timeout = aiohttp.ClientTimeout(total=test_data.timeout_seconds)
        headers = {}
        
        if test_data.api_key:
            # Add API key to headers
            headers["Authorization"] = f"Bearer {test_data.api_key}"
        
        try:
            async with aiohttp.ClientSession(timeout=timeout) as session:
                # Attempt to connect to the SSE endpoint
                try:
                    async with session.get(test_data.server_url, headers=headers) as response:
                        if response.status == 200:
                            # Check for SSE headers or try to read a small amount of data
                            content_type = response.headers.get("Content-Type", "")
                            if "text/event-stream" in content_type:
                                return MCPTestConnectionResponse(
                                    success=True,
                                    message="Successfully connected to MCP SSE server"
                                )
                            else:
                                # Try to read some data as a secondary check
                                try:
                                    data = await asyncio.wait_for(
                                        response.content.read(1024), 
                                        timeout=5
                                    )
                                    if data:
                                        return MCPTestConnectionResponse(
                                            success=True,
                                            message="Successfully connected to server, but Content-Type is not text/event-stream"
                                        )
                                except asyncio.TimeoutError:
                                    return MCPTestConnectionResponse(
                                        success=False,
                                        message="Connection established but no data received"
                                    )
                        else:
                            error_text = await response.text()
                            return MCPTestConnectionResponse(
                                success=False,
                                message=f"Failed to connect: HTTP {response.status} - {error_text}"
                            )
                
                except aiohttp.ClientConnectorError as e:
                    return MCPTestConnectionResponse(
                        success=False,
                        message=f"Failed to connect: {str(e)}"
                    )
                except asyncio.TimeoutError:
                    return MCPTestConnectionResponse(
                        success=False,
                        message=f"Connection timed out after {test_data.timeout_seconds} seconds"
                    )
        except Exception as e:
            logger.error(f"Error testing MCP SSE connection: {str(e)}")
            return MCPTestConnectionResponse(
                success=False,
                message=f"Error testing connection: {str(e)}"
            )
    
    async def _test_stdio_connection(self, test_data: MCPTestConnectionRequest) -> MCPTestConnectionResponse:
        """
        Test connection to a stdio MCP server.
        
        Args:
            test_data: Connection test data
            
        Returns:
            MCPTestConnectionResponse with success status and message
        """
        # For stdio servers, we just check if we can parse the server URL as a valid command
        try:
            # The server_url might be something like "python -m mcp_server"
            parts = test_data.server_url.split()
            
            if not parts:
                return MCPTestConnectionResponse(
                    success=False,
                    message="Invalid command: empty command string"
                )
                
            command = parts[0]
            
            # Check if the command exists (this is a very basic check)
            try:
                # Run with timeout in a separate thread to avoid blocking
                def check_command():
                    try:
                        result = requests.get(test_data.server_url, timeout=test_data.timeout_seconds)
                        return result.status_code == 200
                    except:
                        import subprocess
                        try:
                            # Try to run the command with --help or -h to see if it exists
                            subprocess.run(
                                [command, "--help"], 
                                stdout=subprocess.PIPE, 
                                stderr=subprocess.PIPE,
                                timeout=1
                            )
                            return True
                        except:
                            return False
                
                loop = asyncio.get_event_loop()
                command_exists = await loop.run_in_executor(None, check_command)
                
                if command_exists:
                    return MCPTestConnectionResponse(
                        success=True,
                        message=f"Command '{command}' appears to be valid"
                    )
                else:
                    return MCPTestConnectionResponse(
                        success=False,
                        message=f"Command '{command}' appears to be invalid or not found in the system PATH"
                    )
            except Exception as e:
                return MCPTestConnectionResponse(
                    success=False,
                    message=f"Error checking command: {str(e)}"
                )
        except Exception as e:
            logger.error(f"Error testing MCP stdio connection: {str(e)}")
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