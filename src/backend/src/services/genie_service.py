"""
Genie Service Layer

Business logic layer for Genie operations.
Coordinates between router and repository layers.
"""

import logging
from typing import Optional, List

from src.repositories.genie_repository import GenieRepository
from src.schemas.genie import (
    GenieSpace,
    GenieSpacesRequest,
    GenieSpacesResponse,
    GenieStartConversationRequest,
    GenieStartConversationResponse,
    GenieSendMessageRequest,
    GenieSendMessageResponse,
    GenieGetMessageStatusRequest,
    GenieGetQueryResultRequest,
    GenieQueryResult,
    GenieExecutionRequest,
    GenieExecutionResponse,
    GenieAuthConfig,
    GenieMessageStatus
)

logger = logging.getLogger(__name__)


class GenieService:
    """
    Service layer for Genie operations.
    Handles business logic and coordination.
    """
    
    def __init__(self, auth_config: Optional[GenieAuthConfig] = None):
        """
        Initialize Genie Service.
        
        Args:
            auth_config: Optional authentication configuration
        """
        self.auth_config = auth_config
        self.repository = GenieRepository(auth_config)
    
    async def get_spaces(
        self, 
        request: Optional[GenieSpacesRequest] = None
    ) -> GenieSpacesResponse:
        """
        Get available Genie spaces with optional filtering and pagination.
        
        Args:
            request: Optional request with search, filter, and pagination parameters
        
        Returns:
            GenieSpacesResponse containing list of spaces with pagination info
        """
        try:
            # Use default request if none provided
            if request is None:
                request = GenieSpacesRequest()
            
            logger.info(
                f"Fetching Genie spaces (search: {request.search_query}, "
                f"page_size: {request.page_size}, page_token: {request.page_token})"
            )
            
            # Call repository with all parameters
            spaces_response = await self.repository.get_spaces(
                search_query=request.search_query,
                space_ids=request.space_ids,
                enabled_only=request.enabled_only,
                page_token=request.page_token,
                page_size=request.page_size,
                fetch_all=False  # Let pagination work normally
            )
            
            logger.info(
                f"Retrieved {len(spaces_response.spaces)} Genie spaces, "
                f"has_more: {spaces_response.has_more}"
            )
            return spaces_response
            
        except Exception as e:
            logger.error(f"Error in get_spaces service: {e}")
            # Return empty response on error
            return GenieSpacesResponse(spaces=[])
    
    async def search_spaces(
        self, 
        query: Optional[str] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None
    ) -> GenieSpacesResponse:
        """
        Search for Genie spaces by query.
        
        Args:
            query: Search query string
            limit: Maximum number of spaces to return
            offset: Number of spaces to skip
            
        Returns:
            GenieSpacesResponse with matching spaces
        """
        try:
            logger.info(f"Searching spaces with query: {query}")
            request = GenieSpacesRequest(
                search_query=query,
                limit=limit,
                offset=offset
            )
            return await self.get_spaces(request)
            
        except Exception as e:
            logger.error(f"Error searching spaces: {e}")
            return GenieSpacesResponse(spaces=[])
    
    async def get_space_details(self, space_id: str) -> Optional[GenieSpace]:
        """
        Get details for a specific Genie space.
        
        Args:
            space_id: The space ID
            
        Returns:
            GenieSpace object or None if not found
        """
        try:
            logger.info(f"Fetching details for space: {space_id}")
            space = await self.repository.get_space_details(space_id)
            
            if space:
                logger.info(f"Retrieved details for space: {space.name}")
            else:
                logger.warning(f"Space not found: {space_id}")
            
            return space
            
        except Exception as e:
            logger.error(f"Error getting space details: {e}")
            return None
    
    async def start_conversation(
        self,
        space_id: str,
        initial_message: Optional[str] = None,
        title: Optional[str] = None
    ) -> Optional[GenieStartConversationResponse]:
        """
        Start a new Genie conversation.
        
        Args:
            space_id: The space ID
            initial_message: Optional initial message
            title: Optional conversation title
            
        Returns:
            GenieStartConversationResponse or None if failed
        """
        try:
            logger.info(f"Starting conversation in space: {space_id}")
            
            request = GenieStartConversationRequest(
                space_id=space_id,
                initial_message=initial_message,
                title=title
            )
            
            response = await self.repository.start_conversation(request)
            
            if response:
                logger.info(f"Started conversation: {response.conversation_id}")
            else:
                logger.error("Failed to start conversation")
            
            return response
            
        except Exception as e:
            logger.error(f"Error starting conversation: {e}")
            return None
    
    async def send_message(
        self,
        space_id: str,
        message: str,
        conversation_id: Optional[str] = None
    ) -> Optional[GenieSendMessageResponse]:
        """
        Send a message to Genie.
        
        Args:
            space_id: The space ID
            message: The message content
            conversation_id: Optional existing conversation ID
            
        Returns:
            GenieSendMessageResponse or None if failed
        """
        try:
            logger.info(f"Sending message to space: {space_id}")
            
            request = GenieSendMessageRequest(
                space_id=space_id,
                conversation_id=conversation_id,
                message=message
            )
            
            response = await self.repository.send_message(request)
            
            if response:
                logger.info(f"Sent message: {response.message_id}")
            else:
                logger.error("Failed to send message")
            
            return response
            
        except Exception as e:
            logger.error(f"Error sending message: {e}")
            return None
    
    async def get_message_status(
        self,
        space_id: str,
        conversation_id: str,
        message_id: str
    ) -> Optional[GenieMessageStatus]:
        """
        Get the status of a message.
        
        Args:
            space_id: The space ID
            conversation_id: The conversation ID
            message_id: The message ID
            
        Returns:
            GenieMessageStatus or None if failed
        """
        try:
            request = GenieGetMessageStatusRequest(
                space_id=space_id,
                conversation_id=conversation_id,
                message_id=message_id
            )
            
            status = await self.repository.get_message_status(request)
            
            if status:
                logger.debug(f"Message {message_id} status: {status}")
            
            return status
            
        except Exception as e:
            logger.error(f"Error getting message status: {e}")
            return None
    
    async def get_query_result(
        self,
        space_id: str,
        conversation_id: str,
        message_id: str
    ) -> Optional[GenieQueryResult]:
        """
        Get the query result for a message.
        
        Args:
            space_id: The space ID
            conversation_id: The conversation ID
            message_id: The message ID
            
        Returns:
            GenieQueryResult or None if failed
        """
        try:
            request = GenieGetQueryResultRequest(
                space_id=space_id,
                conversation_id=conversation_id,
                message_id=message_id
            )
            
            result = await self.repository.get_query_result(request)
            
            if result:
                logger.debug(f"Retrieved query result for message {message_id}")
            
            return result
            
        except Exception as e:
            logger.error(f"Error getting query result: {e}")
            return None
    
    async def execute_query(
        self,
        space_id: str,
        question: str,
        conversation_id: Optional[str] = None,
        timeout: int = 120
    ) -> GenieExecutionResponse:
        """
        Execute a complete Genie query workflow.
        
        Args:
            space_id: The space ID
            question: The question to ask
            conversation_id: Optional existing conversation ID
            timeout: Timeout in seconds
            
        Returns:
            GenieExecutionResponse with the result
        """
        try:
            logger.info(f"Executing query in space {space_id}: {question[:50]}...")
            
            request = GenieExecutionRequest(
                space_id=space_id,
                question=question,
                conversation_id=conversation_id,
                timeout=timeout
            )
            
            response = await self.repository.execute_query(request)
            
            if response.status == "SUCCESS":
                logger.info(f"Query executed successfully: {response.conversation_id}")
            else:
                logger.error(f"Query failed: {response.error}")
            
            return response
            
        except Exception as e:
            logger.error(f"Error executing query: {e}")
            return GenieExecutionResponse(
                conversation_id=conversation_id or "",
                message_id="",
                status="FAILED",
                error=str(e)
            )
    
    async def validate_space_access(
        self,
        space_id: str,
        auth_config: Optional[GenieAuthConfig] = None
    ) -> bool:
        """
        Validate that the current authentication has access to a space.
        
        Args:
            space_id: The space ID to validate
            auth_config: Optional auth config to use
            
        Returns:
            True if access is valid, False otherwise
        """
        try:
            # Try to get space details as a validation check
            if auth_config:
                self.repository.auth_config = auth_config
            
            space = await self.repository.get_space_details(space_id)
            return space is not None
            
        except Exception as e:
            logger.error(f"Error validating space access: {e}")
            return False