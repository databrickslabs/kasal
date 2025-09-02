"""
CrewAI-compatible wrapper for Databricks Vector Storage.

This module provides a wrapper that makes Databricks Vector Storage compatible
with CrewAI's memory system expectations.
"""
import os

# CRITICAL: Set USE_NULLPOOL immediately at import time to prevent asyncpg connection pool issues
# This must be done before any database connections are created
if not os.environ.get("USE_NULLPOOL"):
    os.environ["USE_NULLPOOL"] = "true"

import logging
from typing import Dict, List, Any, Optional, Union
from datetime import datetime
import json
import uuid

from src.core.logger import LoggerManager
from src.engines.crewai.memory.databricks_vector_storage import DatabricksVectorStorage
from src.schemas.databricks_index_schemas import DatabricksIndexSchemas
from src.engines.crewai.memory.entity_relationship_retriever import EntityRelationshipRetriever

logger = LoggerManager.get_instance().crew
entity_logger = LoggerManager.get_instance().databricks_entity


class CrewAIDatabricksWrapper:
    """
    Wrapper that adapts Databricks memory backend service to CrewAI's expected interface.
    
    This wrapper uses the service layer instead of direct storage access to follow
    clean architecture principles.
    """
    
    def __init__(self, databricks_storage: DatabricksVectorStorage, embedder=None, agent_context=None, enable_relationship_retrieval=False, crew=None):
        """
        Initialize the wrapper.
        
        Args:
            databricks_storage: The underlying Databricks storage instance (for save operations)
            embedder: Optional embedder for generating embeddings
            agent_context: Optional agent context to use when agent is not provided in save calls
            enable_relationship_retrieval: Whether to enable relationship-based retrieval for entity memory
            crew: Optional crew instance to extract LLM model information from agents
        """
        self.storage = databricks_storage  # Still needed for save operations
        self.embedder = embedder
        self.memory_type = databricks_storage.memory_type
        self.agent_context = agent_context  # Store for entity memory when agent not provided
        self.enable_relationship_retrieval = enable_relationship_retrieval
        self.crew = crew  # Store crew reference to extract agent LLM models
        
        # Store connection details for service calls
        self.workspace_url = databricks_storage.workspace_url
        self.index_name = databricks_storage.index_name
        self.endpoint_name = databricks_storage.endpoint_name
        self.user_token = databricks_storage.user_token
        
        # Initialize relationship retriever if enabled for entity memory
        self.relationship_retriever = None
        if self.enable_relationship_retrieval and self.memory_type == "entity":
            try:
                # Import memory backend service for the relationship retriever
                from src.services.memory_backend_service import MemoryBackendService
                from src.core.unit_of_work import UnitOfWork
                # Store UnitOfWork class for creating service instances
                self.unit_of_work_class = UnitOfWork
                self.memory_backend_service_class = MemoryBackendService
                
                self.relationship_retriever = EntityRelationshipRetriever(
                    memory_backend_service=None,  # Will be created per request
                    embedding_model="sentence-transformers/all-MiniLM-L6-v2"
                )
                entity_logger.info("[__init__] Relationship-based entity retrieval enabled")
            except Exception as e:
                entity_logger.warning(f"[__init__] Failed to initialize relationship retriever: {e}")
                self.relationship_retriever = None
    
    def set_agent_context(self, agent):
        """
        Set the current agent context for this wrapper.
        This is used as a fallback when the agent is not provided in save() calls.
        
        Args:
            agent: The current agent object
        """
        self.agent_context = agent
        if self.memory_type == "entity":
            entity_logger.info(f"[set_agent_context] Entity memory wrapper agent context updated to: {agent}")
            if hasattr(agent, 'role'):
                entity_logger.info(f"[set_agent_context] Agent role: {agent.role}")
    
    def _is_memory_enabled_for_current_agent(self) -> bool:
        """
        Check if memory is enabled for the current agent.
        
        Returns:
            True if memory is enabled for the current agent, False otherwise
        """
        # Check if we have an agent context
        if self.agent_context:
            # Check if the agent has a memory attribute
            if hasattr(self.agent_context, 'memory'):
                memory_enabled = self.agent_context.memory
                if not memory_enabled:
                    logger.info(f"[_is_memory_enabled_for_current_agent] Memory is disabled for agent: {getattr(self.agent_context, 'role', 'Unknown')}")
                return memory_enabled
        
        # Default to enabled if we can't determine
        return True
    
    def _service_search(self, query_embedding: List[float], k: int = 5, filters: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        """
        Helper method to search using the service layer instead of direct storage access.
        Creates service instance dynamically to maintain async patterns.
        """
        try:
            import asyncio
            from src.services.memory_backend_service import MemoryBackendService
            from src.core.unit_of_work import UnitOfWork
            
            # Log search details for entity memory
            if self.memory_type == "entity":
                entity_logger.info(f"[_service_search] Performing embedding search on index: {self.index_name}")
                entity_logger.info(f"[_service_search] Endpoint: {self.endpoint_name}")
                entity_logger.info(f"[_service_search] Filters provided: {filters}")
                entity_logger.info(f"[_service_search] Storage crew_id: {self.storage.crew_id if self.storage else 'No storage'}")
                
                # Add crew_id filter if not already present and storage has crew_id
                if self.storage and self.storage.crew_id:
                    if filters is None:
                        filters = {}
                    if 'crew_id' not in filters:
                        filters['crew_id'] = self.storage.crew_id
                        entity_logger.info(f"[_service_search] Added crew_id filter: {self.storage.crew_id}")
                
                entity_logger.info(f"[_service_search] Final filters: {filters}")
            
            async def _async_search():
                async with UnitOfWork() as uow:
                    service = MemoryBackendService(uow)
                    return await service.search_vectors(
                        workspace_url=self.workspace_url,
                        index_name=self.index_name,
                        endpoint_name=self.endpoint_name,
                        query_embedding=query_embedding,
                        memory_type=self.memory_type,
                        k=k,
                        filters=filters,
                        user_token=self.user_token
                    )
            
            # Handle async service call from sync context
            try:
                # Check if we're in an async context
                loop = asyncio.get_running_loop()
                # We're in an async context, create a new event loop in a thread
                import concurrent.futures
                import os
                
                # Ensure USE_NULLPOOL is set before creating new connections
                if not os.environ.get("USE_NULLPOOL"):
                    os.environ["USE_NULLPOOL"] = "true"
                
                def run_in_new_loop():
                    """Run the async function in a completely new event loop."""
                    new_loop = asyncio.new_event_loop()
                    asyncio.set_event_loop(new_loop)
                    try:
                        return new_loop.run_until_complete(_async_search())
                    finally:
                        new_loop.close()
                
                with concurrent.futures.ThreadPoolExecutor() as executor:
                    future = executor.submit(run_in_new_loop)
                    return future.result()
            except RuntimeError:
                # No event loop running, safe to use asyncio.run
                # Ensure USE_NULLPOOL is set
                import os
                if not os.environ.get("USE_NULLPOOL"):
                    os.environ["USE_NULLPOOL"] = "true"
                return asyncio.run(_async_search())
        except Exception as e:
            logger.error(f"Error in service search call: {e}")
            return []
    
    def _async_save(self, data: Dict[str, Any]) -> None:
        """
        Helper method to handle async save operations from sync context.
        
        Args:
            data: Data dictionary to save
        """
        try:
            import asyncio
            
            async def _do_save():
                await self.storage.save(data)
            
            # Handle async call from sync context
            try:
                # Check if we're in an async context
                loop = asyncio.get_running_loop()
                # We're in an async context, create a new event loop in a thread
                import concurrent.futures
                import os
                
                # Ensure USE_NULLPOOL is set before creating new connections
                if not os.environ.get("USE_NULLPOOL"):
                    os.environ["USE_NULLPOOL"] = "true"
                
                def run_in_new_loop():
                    """Run the async function in a completely new event loop."""
                    new_loop = asyncio.new_event_loop()
                    asyncio.set_event_loop(new_loop)
                    try:
                        return new_loop.run_until_complete(_do_save())
                    finally:
                        new_loop.close()
                
                with concurrent.futures.ThreadPoolExecutor() as executor:
                    future = executor.submit(run_in_new_loop)
                    future.result()
            except RuntimeError:
                # No event loop running, safe to use asyncio.run
                # Ensure USE_NULLPOOL is set
                import os
                if not os.environ.get("USE_NULLPOOL"):
                    os.environ["USE_NULLPOOL"] = "true"
                asyncio.run(_do_save())
        except Exception as e:
            logger.error(f"Error in async save: {e}")
    
    def _async_relationship_search(self, query: str, initial_results: List[Dict[str, Any]], 
                                 agent_id: str, group_id: str, max_hops: int = 2, 
                                 max_total: int = 10, relationship_weight: float = 0.3) -> List[Dict[str, Any]]:
        """
        Helper method to handle async relationship search from sync context.
        
        Args:
            query: Search query
            initial_results: Initial semantic search results
            agent_id: Agent identifier
            group_id: Group identifier
            max_hops: Maximum relationship hops
            max_total: Maximum total results
            relationship_weight: Weight for relationship scoring
            
        Returns:
            Enhanced search results
        """
        try:
            import asyncio
            
            async def _do_relationship_search():
                # Create service instance within async context
                async with self.unit_of_work_class() as uow:
                    service = self.memory_backend_service_class(uow)
                    # Temporarily set the service for the retriever
                    self.relationship_retriever.memory_backend_service = service
                    
                    return await self.relationship_retriever.search_with_relationships(
                        query=query,
                        initial_results=initial_results,
                        workspace_url=self.workspace_url,
                        index_name=self.index_name,
                        endpoint_name=self.endpoint_name,
                        user_token=self.user_token,
                        agent_id=agent_id,
                        group_id=group_id,
                        max_hops=max_hops,
                        max_total=max_total,
                        relationship_weight=relationship_weight
                    )
            
            # Handle async call from sync context
            try:
                # Check if we're in an async context
                loop = asyncio.get_running_loop()
                # We're in an async context, create a new event loop in a thread
                import concurrent.futures
                import os
                
                # Ensure USE_NULLPOOL is set before creating new connections
                if not os.environ.get("USE_NULLPOOL"):
                    os.environ["USE_NULLPOOL"] = "true"
                
                def run_in_new_loop():
                    """Run the async function in a completely new event loop."""
                    new_loop = asyncio.new_event_loop()
                    asyncio.set_event_loop(new_loop)
                    try:
                        return new_loop.run_until_complete(_do_relationship_search())
                    finally:
                        new_loop.close()
                
                with concurrent.futures.ThreadPoolExecutor() as executor:
                    future = executor.submit(run_in_new_loop)
                    return future.result()
            except RuntimeError:
                # No event loop running, safe to use asyncio.run
                # Ensure USE_NULLPOOL is set
                import os
                if not os.environ.get("USE_NULLPOOL"):
                    os.environ["USE_NULLPOOL"] = "true"
                return asyncio.run(_do_relationship_search())
        except Exception as e:
            entity_logger.error(f"Error in async relationship search: {e}")
            # Return original results as fallback
            return self._format_results_for_crewai(initial_results)
    
    def _format_results_for_crewai(self, results: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Format search results to match CrewAI's expected format.
        
        CrewAI expects each result to have a 'context' field, but our Databricks
        results have 'data' field. This method ensures compatibility.
        
        Args:
            results: Raw search results from service layer
            
        Returns:
            Formatted results with 'context' field for CrewAI compatibility
        """
        formatted_results = []
        for result in results:
            # Create a copy to avoid modifying original
            formatted_result = result.copy()
            
            # Ensure 'context' field exists - this is what CrewAI expects
            if 'context' not in formatted_result:
                # Map 'data' field to 'context' for CrewAI compatibility
                if 'data' in formatted_result:
                    formatted_result['context'] = formatted_result['data']
                elif 'content' in formatted_result:
                    formatted_result['context'] = formatted_result['content']
                else:
                    # Fallback - use a concatenation of available text fields
                    text_parts = []
                    metadata = formatted_result.get('metadata', {})
                    if isinstance(metadata, dict):
                        for key, value in metadata.items():
                            if isinstance(value, str) and value.strip():
                                text_parts.append(f"{key}: {value}")
                    
                    formatted_result['context'] = ' | '.join(text_parts) if text_parts else "No context available"
            
            formatted_results.append(formatted_result)
            
        logger.debug(f"[_format_results_for_crewai] Formatted {len(formatted_results)} results for CrewAI")
        return formatted_results
        
    def save(self, *args, **kwargs) -> None:
        """
        Save memory with CrewAI-compatible interface.
        
        This method handles different signatures for different memory types:
        - ShortTermMemory: save(value, metadata, **kwargs)
        - LongTermMemory: save(item) where item is LongTermMemoryItem
        - EntityMemory: save(entity_name, content, metadata)
        """
        logger.info(f"[CrewAIDatabricksWrapper.save] Called for {self.memory_type} memory")
        logger.info(f"[CrewAIDatabricksWrapper.save] Args count: {len(args)}, kwargs: {list(kwargs.keys())}")
        
        # Special logging for entity memory to debug
        if self.memory_type == "entity":
            entity_logger.info(f"[CrewAIDatabricksWrapper.save] Entity memory save called!")
            for i, arg in enumerate(args):
                entity_logger.info(f"[CrewAIDatabricksWrapper.save] Arg {i}: type={type(arg)}, value={str(arg)[:200] if hasattr(arg, '__str__') else 'no str'}")
        
        # Handle different call signatures based on memory type
        value = None
        metadata = None
        
        if self.memory_type == "long_term" and len(args) == 1:
            # LongTermMemory passes a LongTermMemoryItem object
            item = args[0]
            logger.info(f"[save] Long-term memory item type: {type(item)}")
            
            # Extract data from LongTermMemoryItem
            if hasattr(item, '__dict__'):
                logger.info(f"[save] Item attributes: {list(item.__dict__.keys())}")
                # Log all attribute values to debug what CrewAI is passing
                for attr_name in item.__dict__.keys():
                    attr_value = getattr(item, attr_name, None)
                    if attr_value:
                        logger.info(f"[save] Item.{attr_name}: {str(attr_value)[:200]}...")
                
                # Extract the task description - this is what CrewAI provides
                task_description = getattr(item, 'task', '') or getattr(item, 'task_description', '')
                
                # CrewAI's long-term memory is designed to store task descriptions and quality evaluations,
                # not actual outputs. This is intentional - see LongTermMemoryItem class definition.
                # The system uses task descriptions to find similar past tasks and retrieve suggestions.
                value = task_description
                logger.info(f"[save] Long-term memory storing task description as designed by CrewAI")
                
                # Build metadata from item attributes
                metadata = {
                    'agent': getattr(item, 'agent', ''),
                    'expected_output': getattr(item, 'expected_output', ''),
                    'datetime': getattr(item, 'datetime', str(datetime.now())),
                    'quality': getattr(item, 'quality', None),
                    'task_description': task_description,  # Always add task description for task_hash generation
                    'task': task_description  # Keep for backward compatibility
                }
                
                # Add any existing metadata
                if hasattr(item, 'metadata') and item.metadata:
                    metadata.update(item.metadata)
                    
                logger.info(f"[save] Extracted value: {value[:100] if value else 'None'}...")
                logger.info(f"[save] Extracted metadata: {metadata}")
                
        elif self.memory_type == "entity" and len(args) >= 1:
            # EntityMemory passes the full entity description as first arg
            # Format: "EntityName(Type): Description"
            # Second arg is metadata dict with relationships
            entity_full_text = args[0] if isinstance(args[0], str) else str(args[0])
            metadata = args[1] if len(args) > 1 and isinstance(args[1], dict) else {}
            
            entity_logger.info(f"[save] Entity memory - raw args: {args}")
            entity_logger.info(f"[save] Entity memory - kwargs: {kwargs}")
            entity_logger.info(f"[save] Entity memory - entity_full_text: '{entity_full_text}'")
            entity_logger.info(f"[save] Entity memory - metadata: {metadata}")
            
            # Parse entity information from the full text
            # Format is typically: "EntityName(Type): Description"
            import re
            match = re.match(r'^(.+?)\((.+?)\):\s*(.+)$', entity_full_text)
            if match:
                entity_name = match.group(1).strip()
                entity_type = match.group(2).strip()
                entity_description = match.group(3).strip()
                
                # Add parsed info to metadata
                metadata['entity_name'] = entity_name
                metadata['entity_type'] = entity_type
                metadata['description'] = entity_description
                
                # Use the full text as the value to embed
                value = entity_full_text
                entity_logger.info(f"[save] Parsed entity - name: {entity_name}, type: {entity_type}, desc: {entity_description[:100]}...")
            else:
                # Enhanced fallback - try to extract meaningful entity information
                entity_logger.warning(f"[save] Could not parse entity format '{entity_full_text}', trying enhanced extraction...")
                
                # Try different patterns
                value = entity_full_text
                
                # Check if it's already structured data (like a dict or JSON)
                if entity_full_text.strip().startswith('{'):
                    try:
                        import json
                        entity_data = json.loads(entity_full_text)
                        metadata['entity_name'] = entity_data.get('name', entity_data.get('entity_name', f"Entity_{str(uuid.uuid4())[:8]}"))
                        metadata['entity_type'] = entity_data.get('type', entity_data.get('entity_type', 'extracted'))
                        metadata['description'] = entity_data.get('description', entity_full_text)
                        entity_logger.info(f"[save] Parsed JSON entity - name: {metadata['entity_name']}, type: {metadata['entity_type']}")
                    except:
                        # Not JSON, continue with text extraction
                        pass
                
                # If no entity_name set yet, try to extract from text
                if 'entity_name' not in metadata or not metadata['entity_name']:
                    # Look for patterns like "NAME is a TYPE" or "The TYPE NAME"
                    patterns = [
                        r'(\w+(?:\s+\w+)*)\s+is\s+a\s+(\w+)',  # "John Doe is a person"
                        r'The\s+(\w+)\s+(\w+(?:\s+\w+)*)',     # "The person John Doe"
                        r'(\w+(?:\s+\w+)*)\s*\(([^)]+)\)',     # "John Doe (person)"
                        r'([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)',   # Capitalized names
                    ]
                    
                    entity_extracted = False
                    for pattern in patterns:
                        pattern_match = re.search(pattern, entity_full_text, re.IGNORECASE)
                        if pattern_match:
                            if len(pattern_match.groups()) == 2:
                                metadata['entity_name'] = pattern_match.group(1).strip()
                                metadata['entity_type'] = pattern_match.group(2).strip()
                            else:
                                metadata['entity_name'] = pattern_match.group(1).strip()
                                metadata['entity_type'] = 'extracted'
                            entity_extracted = True
                            entity_logger.info(f"[save] Extracted with pattern - name: {metadata['entity_name']}, type: {metadata['entity_type']}")
                            break
                    
                    # Final fallback
                    if not entity_extracted:
                        # Use first 30 characters as name, generate unique ID
                        import uuid
                        metadata['entity_name'] = entity_full_text[:30].strip() or f"Entity_{str(uuid.uuid4())[:8]}"
                        metadata['entity_type'] = 'unclassified'
                        entity_logger.warning(f"[save] Final fallback - name: {metadata['entity_name']}, type: {metadata['entity_type']}")
                
                metadata['description'] = entity_full_text
            
        else:
            # Check if this is long-term memory calling with kwargs
            logger.info(f"[save] Checking long-term memory path: memory_type={self.memory_type}, has_task_description={'task_description' in kwargs}, kwargs_keys={list(kwargs.keys())}")
            if self.memory_type == "long_term" and 'task_description' in kwargs:
                # Long-term memory passes data through kwargs
                task_description = kwargs.get('task_description', '')
                metadata = kwargs.get('metadata', {})
                
                # CrewAI's long-term memory stores task descriptions, not outputs
                # This is by design - the system uses task descriptions to find similar past tasks
                value = task_description
                logger.info(f"[save] Long-term memory (kwargs path) storing task description as designed")
                
                # Add task_description to metadata for task_hash generation
                metadata['task_description'] = task_description
                # Add additional fields to metadata
                if 'score' in kwargs:
                    metadata['score'] = kwargs['score']
                if 'datetime' in kwargs:
                    metadata['datetime'] = kwargs['datetime']
                    
                logger.info(f"[save] Long-term memory with kwargs - value: {value[:100] if value else 'None'}...")
            else:
                # Standard short-term memory or fallback
                value = args[0] if len(args) > 0 else kwargs.get('value')
                metadata = args[1] if len(args) > 1 else kwargs.get('metadata', {})
        
        # More debug logging for numpy arrays
        if hasattr(value, 'shape'):
            logger.debug(f"[save] Value shape: {value.shape}")
        if hasattr(value, 'dtype'):
            logger.debug(f"[save] Value dtype: {value.dtype}")
        
        # Handle case where value is None but content might be in kwargs or metadata
        if value is None:
            # Check if content is in kwargs
            if 'content' in kwargs:
                value = kwargs.pop('content')
                logger.info(f"[save] Found content in kwargs: {value}")
            elif 'data' in kwargs:
                value = kwargs.pop('data')
                logger.info(f"[save] Found data in kwargs: {value}")
            elif metadata and 'content' in metadata:
                value = metadata['content']
                logger.info(f"[save] Found content in metadata: {value}")
            else:
                logger.warning(f"[save] No value provided and no content found in kwargs or metadata for {self.memory_type} memory")
                logger.warning(f"[save] Available data - args: {args}, kwargs: {kwargs}, metadata: {metadata}")
                return
        
        try:
            # Extract agent from kwargs if provided, otherwise use stored agent_context
            agent = kwargs.get('agent') or self.agent_context
            
            # Log agent information for debugging
            if self.memory_type == "entity":
                entity_logger.info(f"[save] Agent parameter received: {kwargs.get('agent')}")
                entity_logger.info(f"[save] Agent context fallback: {self.agent_context}")
                entity_logger.info(f"[save] Final agent used: {agent}")
                entity_logger.info(f"[save] Agent type: {type(agent)}")
                if hasattr(agent, 'role'):
                    entity_logger.info(f"[save] Agent role: {agent.role}")
                if hasattr(agent, 'id'):
                    entity_logger.info(f"[save] Agent id: {agent.id}")
            
            # Handle different input formats
            if isinstance(value, str):
                # Text content - need to generate embedding
                if self.embedder:
                    # Use the synchronous wrapper which handles event loop creation
                    try:
                        embedding = self._generate_embedding_sync(value)
                    except Exception as e:
                        logger.error(f"Error running embedding generation: {e}")
                        embedding = None
                    
                    if embedding is not None:
                        # Map metadata fields to schema fields for all memory types
                        memory_schema = DatabricksIndexSchemas.get_schema(self.memory_type)
                        # Get embedding model from the embedder configuration
                        embedding_model = 'databricks-gte-large-en'  # Default
                        if isinstance(self.embedder, dict):
                            # Extract model from embedder config
                            if self.embedder.get('provider') == 'databricks':
                                embedding_model = self.embedder.get('config', {}).get('model', 'databricks-gte-large-en')
                            elif self.embedder.get('provider') == 'custom' and 'config' in self.embedder:
                                # For custom embedder, check if model is specified
                                custom_config = self.embedder.get('config', {})
                                if hasattr(custom_config.get('embedder'), 'model'):
                                    embedding_model = custom_config['embedder'].model
                        
                        save_data = {
                            'data': value, 
                            'embedding': embedding, 
                            'metadata': metadata, 
                            'agent': agent,
                            'embedding_model': embedding_model
                        }
                        
                        if self.memory_type == "entity":
                            # Map entity-specific fields
                            for schema_field in memory_schema.keys():
                                if schema_field == 'entity_name':
                                    save_data['entity_name'] = metadata.get('entity_name', '')
                                elif schema_field == 'entity_type':
                                    save_data['entity_type'] = metadata.get('entity_type', 'unknown')
                                elif schema_field == 'description':
                                    save_data['description'] = metadata.get('description', value)
                                elif schema_field == 'relationships':
                                    save_data['relationships'] = metadata.get('relationships', [])
                                elif schema_field == 'attributes':
                                    save_data['attributes'] = metadata.get('attributes', {})
                            
                            entity_logger.info(f"[save] Mapped entity data using schema: entity_name='{save_data.get('entity_name')}', entity_type='{save_data.get('entity_type')}'")
                            
                        elif self.memory_type == "long_term":
                            # Map long-term memory specific fields
                            for schema_field in memory_schema.keys():
                                if schema_field == 'task_description':
                                    save_data['task_description'] = metadata.get('task_description', value)
                                elif schema_field == 'quality':
                                    save_data['quality'] = metadata.get('quality', 0.8)
                                elif schema_field == 'importance':
                                    save_data['importance'] = metadata.get('importance', 0.5)
                            
                            logger.info(f"[save] Mapped long-term memory data: task_description='{save_data.get('task_description', '')[:50]}...', quality={save_data.get('quality')}")
                            
                        elif self.memory_type == "short_term":
                            # Map short-term memory specific fields  
                            for schema_field in memory_schema.keys():
                                if schema_field == 'content':
                                    save_data['content'] = metadata.get('content', value)
                                elif schema_field == 'query_text':
                                    save_data['query_text'] = metadata.get('query_text', '')
                                elif schema_field == 'session_id':
                                    save_data['session_id'] = metadata.get('session_id', '')
                            
                            logger.info(f"[save] Mapped short-term memory data: content='{save_data.get('content', '')[:50]}...'")
                        
                        # Map agent information to agent_id field according to schema
                        # Priority: agent.role > metadata.agent > agent string > agent.id
                        agent_id_set = False
                        
                        # Enhanced debug logging for entity memory
                        if self.memory_type == "entity":
                            entity_logger.info(f"[save] Agent parameter during agent_id mapping: {agent}")
                            entity_logger.info(f"[save] Agent type during mapping: {type(agent)}")
                            entity_logger.info(f"[save] metadata.agent during mapping: {metadata.get('agent')}")
                            if hasattr(agent, 'role'):
                                entity_logger.info(f"[save] Agent.role available: {agent.role}")
                            if hasattr(agent, 'id'):
                                entity_logger.info(f"[save] Agent.id available: {agent.id}")
                        
                        if agent and hasattr(agent, 'role'):
                            # Use agent role as agent_id (most preferred - human readable)
                            save_data['agent_id'] = agent.role
                            logger.info(f"[save] Using agent.role as agent_id: '{agent.role}'")
                            agent_id_set = True
                        elif metadata.get('agent'):
                            # Extract agent from metadata (often contains the role)
                            save_data['agent_id'] = metadata.get('agent')
                            logger.info(f"[save] Using metadata.agent as agent_id: '{metadata.get('agent')}'")
                            agent_id_set = True
                        elif agent and isinstance(agent, str):
                            # Agent passed as string
                            save_data['agent_id'] = agent
                            logger.info(f"[save] Using agent string as agent_id: '{agent}'")
                            agent_id_set = True
                        elif agent and hasattr(agent, 'id'):
                            # Fallback to agent UUID (less preferred for memory)
                            save_data['agent_id'] = agent.id  
                            logger.info(f"[save] Using agent.id as agent_id: '{agent.id}'")
                            agent_id_set = True
                        
                        # Final logging for agent_id
                        if not agent_id_set:
                            logger.warning(f"[save] No agent_id could be determined - will use storage default 'default_agent'")
                            logger.warning(f"[save] Available data - agent: {agent}, metadata.agent: {metadata.get('agent')}")
                            # For entity memory, log additional debug info
                            if self.memory_type == "entity":
                                entity_logger.warning(f"[save] Entity memory agent_id problem - args: {len(args)} items, kwargs keys: {list(kwargs.keys())}")
                        else:
                            final_agent_id = save_data.get('agent_id')
                            logger.info(f"[save] Memory will be attributed to agent_id: '{final_agent_id}'")
                            # Special logging for entity memory
                            if self.memory_type == "entity":
                                entity_logger.info(f"[save] Entity '{save_data.get('entity_name', 'Unknown')}' attributed to agent: '{final_agent_id}'")
                        
                        # Extract LLM model information
                        llm_model = "unknown"
                        
                        # For long-term memory, try to get LLM model from the agent that completed the task
                        if self.memory_type == "long_term" and metadata.get('agent'):
                            # The agent name is stored in metadata for long-term memory
                            agent_name = metadata.get('agent')
                            logger.info(f"[save] Long-term memory - looking for LLM model for agent: {agent_name}")
                            
                            # Try to find this agent in the crew
                            if self.crew and hasattr(self.crew, 'agents'):
                                for crew_agent in self.crew.agents:
                                    if hasattr(crew_agent, 'role') and crew_agent.role == agent_name:
                                        # Found the agent, extract LLM model
                                        if hasattr(crew_agent, 'llm'):
                                            if hasattr(crew_agent.llm, 'model'):
                                                llm_model = crew_agent.llm.model
                                            elif hasattr(crew_agent.llm, 'model_name'):
                                                llm_model = crew_agent.llm.model_name
                                            elif isinstance(crew_agent.llm, str):
                                                llm_model = crew_agent.llm
                                            else:
                                                # Try to get the string representation
                                                llm_model = str(crew_agent.llm)
                                        logger.info(f"[save] Found LLM model for agent {agent_name}: '{llm_model}'")
                                        break
                        
                        # Try other sources if still unknown
                        if llm_model == "unknown":
                            if agent and hasattr(agent, 'llm'):
                                # Agent has LLM configuration
                                if hasattr(agent.llm, 'model'):
                                    llm_model = agent.llm.model
                                elif hasattr(agent.llm, 'model_name'):
                                    llm_model = agent.llm.model_name
                                elif isinstance(agent.llm, str):
                                    llm_model = agent.llm
                                logger.info(f"[save] Extracted LLM model from agent.llm: '{llm_model}'")
                            elif metadata.get('llm_model'):
                                # LLM model in metadata
                                llm_model = metadata.get('llm_model')
                                logger.info(f"[save] Using LLM model from metadata: '{llm_model}'")
                            elif metadata.get('model'):
                                # Alternative field name
                                llm_model = metadata.get('model')
                                logger.info(f"[save] Using model from metadata: '{llm_model}'")
                        save_data['llm_model'] = llm_model
                        
                        # Extract tools used information
                        tools_used = []
                        
                        # For long-term memory, try to get tools from the agent that completed the task
                        if self.memory_type == "long_term" and metadata.get('agent'):
                            agent_name = metadata.get('agent')
                            
                            # Try to find this agent in the crew and get its tools
                            if self.crew and hasattr(self.crew, 'agents'):
                                for crew_agent in self.crew.agents:
                                    if hasattr(crew_agent, 'role') and crew_agent.role == agent_name:
                                        # Found the agent, extract tools
                                        if hasattr(crew_agent, 'tools') and isinstance(crew_agent.tools, list):
                                            for tool in crew_agent.tools:
                                                if hasattr(tool, 'name'):
                                                    tools_used.append(tool.name)
                                                elif hasattr(tool, '__name__'):
                                                    tools_used.append(tool.__name__)
                                                elif isinstance(tool, str):
                                                    tools_used.append(tool)
                                                else:
                                                    # Try to get string representation
                                                    tools_used.append(str(tool))
                                        logger.info(f"[save] Found tools for agent {agent_name}: {tools_used}")
                                        break
                        
                        # Try other sources if still empty
                        if not tools_used:
                            if agent and hasattr(agent, 'tools'):
                                # Agent has tools list
                                if isinstance(agent.tools, list):
                                    # Extract tool names from tool objects
                                    for tool in agent.tools:
                                        if hasattr(tool, 'name'):
                                            tools_used.append(tool.name)
                                        elif hasattr(tool, '__name__'):
                                            tools_used.append(tool.__name__)
                                        elif isinstance(tool, str):
                                            tools_used.append(tool)
                                logger.info(f"[save] Extracted tools from agent.tools: {tools_used}")
                        elif metadata.get('tools_used'):
                            # Tools in metadata
                            tools_value = metadata.get('tools_used')
                            if isinstance(tools_value, list):
                                tools_used = tools_value
                            elif isinstance(tools_value, str):
                                # Try to parse JSON string
                                try:
                                    import json
                                    tools_used = json.loads(tools_value)
                                except:
                                    tools_used = [tools_value]
                            logger.info(f"[save] Using tools from metadata: {tools_used}")
                        elif metadata.get('tools'):
                            # Alternative field name
                            tools_value = metadata.get('tools')
                            if isinstance(tools_value, list):
                                tools_used = tools_value
                            logger.info(f"[save] Using tools from metadata.tools: {tools_used}")
                        save_data['tools_used'] = tools_used
                        
                        self._async_save(save_data)
                    else:
                        logger.warning("Failed to generate embedding for text content")
                else:
                    logger.warning("No embedder available for text content")
                    
            elif isinstance(value, dict):
                # Dict with potential embedding
                if 'embedding' in value:
                    # Already has embedding
                    self._async_save({'data': value, 'metadata': metadata, 'agent': agent})
                elif 'data' in value and self.embedder:
                    # Has data but no embedding - generate it
                    try:
                        embedding = self._generate_embedding_sync(value['data'])
                    except Exception as e:
                        logger.error(f"Error running embedding generation: {e}")
                        embedding = None
                    
                    if embedding is not None:
                        value['embedding'] = embedding
                        self._async_save({'data': value, 'metadata': metadata, 'agent': agent})
                else:
                    logger.warning("Dict value missing embedding and cannot generate one")
                    
            elif isinstance(value, list) or (hasattr(value, '__len__') and hasattr(value, '__getitem__')):
                # Direct embedding vector (list or array-like)
                try:
                    value_len = len(value)
                    if value_len == self.storage.embedding_dimension:
                        # Convert to list if it's a numpy array or similar
                        if hasattr(value, 'tolist'):
                            value = value.tolist()
                        self._async_save({
                            'data': {'embedding': value, 'data': metadata.get('content', '')},
                            'metadata': metadata,
                            'agent': agent
                        })
                    else:
                        logger.warning(f"Embedding vector length {value_len} doesn't match expected dimension {self.storage.embedding_dimension}")
                except Exception as e:
                    logger.error(f"Error processing embedding vector: {e}")
            else:
                logger.warning(f"Unsupported value type for save: {type(value)}")
                
        except Exception as e:
            logger.error(f"Error in CrewAI wrapper save: {e}")
            # Log the full traceback for debugging
            import traceback
            logger.error(f"Full traceback: {traceback.format_exc()}")
            
    def search(self, query: Union[str, Dict, List], top_k: int = 3, **kwargs) -> List[Dict]:
        """
        Search with CrewAI-compatible interface.
        
        Args:
            query: Query text, dict, or embedding
            top_k: Number of results to return
            **kwargs: Additional search parameters
            
        Returns:
            List of search results
        """
        try:
            # Check if memory is disabled for the current agent
            if not self._is_memory_enabled_for_current_agent():
                logger.info(f"[search] Memory is disabled for current agent, skipping {self.memory_type} similarity search")
                return []
            
            # Debug logging
            logger.debug(f"[search] Query type: {type(query)}")
            if hasattr(query, 'shape'):
                logger.debug(f"[search] Query shape: {query.shape}")
            if hasattr(query, 'dtype'):
                logger.debug(f"[search] Query dtype: {query.dtype}")
            # Remove 'limit' from kwargs if present to avoid duplicate
            search_kwargs = kwargs.copy()
            search_kwargs.pop('limit', None)
            
            # Handle different query formats
            if isinstance(query, str):
                # Handle empty query for entity memory "get all" operations
                if not query and self.memory_type == "entity":
                    entity_logger.info("[search] Empty query for entity memory - retrieving all entities")
                    # For empty queries in entity memory, just get recent entries
                    # Use dummy embedding for "get all" operation
                    dummy_embedding = [0.0] * getattr(self.storage, 'embedding_dimension', 1024)
                    results = self._service_search(dummy_embedding, k=top_k, filters=search_kwargs.get('filters'))
                    return self._format_results_for_crewai(results)
                    
                # Text query - generate embedding
                if self.embedder:
                    try:
                        if self.memory_type == "entity":
                            entity_logger.info(f"[search] Generating embedding for query: '{query[:100]}...'")
                        embedding = self._generate_embedding_sync(query)
                        if self.memory_type == "entity" and embedding:
                            entity_logger.info(f"[search] Generated embedding with length: {len(embedding)}")
                    except Exception as e:
                        logger.error(f"Error running embedding generation: {e}")
                        embedding = None
                    
                    if embedding is not None:
                        # Use service layer for search
                        if self.memory_type == "entity":
                            entity_logger.info(f"[search] Calling _service_search with filters: {search_kwargs.get('filters')}")
                        initial_results = self._service_search(embedding, k=top_k, filters=search_kwargs.get('filters'))
                        if self.memory_type == "entity":
                            entity_logger.info(f"[search] Initial semantic search found {len(initial_results)} results")
                        
                        # Use relationship-based retrieval for entity memory if enabled
                        if (self.memory_type == "entity" and 
                            self.enable_relationship_retrieval and 
                            self.relationship_retriever and
                            query.strip()):  # Only for non-empty queries
                            
                            try:
                                entity_logger.info(f"[search] Using relationship-based entity retrieval for query: '{query}'")
                                
                                # Extract agent_id and group_id from filters or use context/defaults
                                filters = search_kwargs.get('filters', {})
                                agent_id = filters.get('agent_id')
                                group_id = filters.get('group_id')
                                
                                # If not in filters, try to get from agent context
                                if not agent_id and self.agent_context and hasattr(self.agent_context, 'role'):
                                    agent_id = self.agent_context.role
                                    entity_logger.info(f"[search] Using agent_id from context: '{agent_id}'")
                                elif not agent_id:
                                    agent_id = 'default_agent'
                                    entity_logger.info(f"[search] Using fallback agent_id: '{agent_id}'")
                                
                                # For group_id, we need to get it from the crew configuration
                                # This should come from the search filters, but let's add a fallback
                                if not group_id:
                                    group_id = 'user_admin_admin_com'  # Based on your crew_id pattern
                                    entity_logger.info(f"[search] Using fallback group_id: '{group_id}'")
                                
                                # Use relationship-enhanced search
                                entity_logger.info(f"[search] Initial semantic search found {len(initial_results)} results")
                                
                                # Perform relationship search even with empty initial results
                                # The relationship retriever can still try to find related entities
                                enhanced_results = self._async_relationship_search(
                                    query=query,
                                    initial_results=initial_results,  # Can be empty list or results from broader search
                                    agent_id=agent_id,
                                    group_id=group_id,
                                    max_hops=2,
                                    max_total=top_k,
                                    relationship_weight=0.3
                                )
                                
                                entity_logger.info(f"[search] Relationship retrieval returned {len(enhanced_results)} results")
                                return enhanced_results
                                
                            except Exception as e:
                                entity_logger.error(f"[search] Relationship retrieval failed: {e}")
                                entity_logger.info("[search] Falling back to standard semantic search")
                                return self._format_results_for_crewai(initial_results)
                        else:
                            # Standard semantic search
                            return self._format_results_for_crewai(initial_results)
                    else:
                        logger.warning("Failed to generate embedding for query")
                        return []
                else:
                    logger.warning("No embedder available for text query")
                    return []
                    
            elif isinstance(query, dict) and 'embedding' in query:
                # Dict with embedding
                embedding = query['embedding']
                results = self._service_search(embedding, k=top_k, filters=search_kwargs.get('filters'))
                return self._format_results_for_crewai(results)
                
            elif isinstance(query, list) or (hasattr(query, '__len__') and hasattr(query, '__getitem__')):
                # Direct embedding vector (list or array-like)
                try:
                    query_len = len(query)
                    if query_len == self.storage.embedding_dimension:
                        # Convert to list if it's a numpy array or similar
                        if hasattr(query, 'tolist'):
                            query = query.tolist()
                        results = self._service_search(query, k=top_k, filters=search_kwargs.get('filters'))
                        return self._format_results_for_crewai(results)
                    else:
                        logger.warning(f"Query vector length {query_len} doesn't match embedding dimension {self.storage.embedding_dimension}")
                        return []
                except Exception as e:
                    logger.error(f"Error processing vector query: {e}")
                    return []
                
            else:
                logger.warning(f"Unsupported query type for search: {type(query)}")
                return []
                
        except Exception as e:
            logger.error(f"Error in CrewAI wrapper search: {e}")
            # Log the full traceback for debugging
            import traceback
            logger.error(f"Full traceback: {traceback.format_exc()}")
            return []
            
    def reset(self) -> None:
        """Reset the storage."""
        self.storage.reset()
        
    def get_stats(self) -> Dict[str, Any]:
        """Get storage statistics."""
        return self.storage.get_stats()
        
    def _generate_embedding_sync(self, text: str) -> Optional[List[float]]:
        """
        Synchronous wrapper for embedding generation.
        Handles embeddings in a thread-safe manner without event loop conflicts.
        IMPORTANT: Must use the same embedding model that was used when saving entities.
        """
        try:
            # Handle embedder configuration dictionary (from CrewAI)
            if isinstance(self.embedder, dict):
                # Check if this is a custom embedder with a function
                if self.embedder.get('provider') == 'custom' and 'embedder' in self.embedder.get('config', {}):
                    # Use the custom embedder function directly (this is the DatabricksEmbeddingFunction)
                    custom_embedder = self.embedder['config']['embedder']
                    if hasattr(custom_embedder, '__call__'):
                        # Log what we're embedding for entity memory
                        if self.memory_type == "entity":
                            entity_logger.info(f"[_generate_embedding_sync] Embedding text for search: '{text[:100]}...'")
                        
                        embeddings = custom_embedder([text])
                        if embeddings and len(embeddings) > 0:
                            # Convert numpy array to list if needed
                            embedding = embeddings[0]
                            if hasattr(embedding, 'tolist'):
                                return embedding.tolist()
                            return embedding
                        return None
                else:
                    # For non-custom embedders, we should still try to use them
                    logger.warning(f"Non-custom embedder config found: {self.embedder.get('provider')}")
                    return None
                        
            # Handle different embedder object interfaces (DatabricksEmbeddingFunction object)
            elif hasattr(self.embedder, '__call__'):
                # Embedder is a function - call it directly
                embeddings = self.embedder([text])
                if embeddings and len(embeddings) > 0:
                    # Convert numpy array to list if needed
                    embedding = embeddings[0]
                    if hasattr(embedding, 'tolist'):
                        return embedding.tolist()
                    return embedding
                return None
                
            elif hasattr(self.embedder, 'embed'):
                # Embedder has embed method
                return self.embedder.embed(text)
                
            elif hasattr(self.embedder, 'embed_documents'):
                # Embedder has embed_documents method (LangChain style)
                embeddings = self.embedder.embed_documents([text])
                return embeddings[0] if embeddings else None
                
            else:
                logger.error(f"Embedder does not have a recognized interface. Type: {type(self.embedder)}")
                return None
                
        except Exception as e:
            logger.error(f"Error in sync embedding generation: {e}")
            return None
    
    async def _generate_embedding(self, text: str) -> Optional[List[float]]:
        """
        Generate embedding for text using the configured embedder.
        
        This method should directly use the custom embedder if available
        to avoid re-authentication issues.
        
        Args:
            text: Text to embed
            
        Returns:
            Embedding vector or None
        """
        try:
            if not self.embedder:
                logger.warning("No embedder configured for memory backend")
                return None
                
            # Handle embedder configuration dictionary (from CrewAI)
            if isinstance(self.embedder, dict):
                # Check if this is a custom embedder with a function
                if self.embedder.get('provider') == 'custom' and 'embedder' in self.embedder.get('config', {}):
                    # Use the custom embedder directly - it already has auth configured
                    custom_embedder = self.embedder['config']['embedder']
                    if hasattr(custom_embedder, '__call__'):
                        embeddings = custom_embedder([text])
                        if embeddings and len(embeddings) > 0:
                            # Convert numpy array to list if needed
                            embedding = embeddings[0]
                            if hasattr(embedding, 'tolist'):
                                return embedding.tolist()
                            return embedding
                        return None
                else:
                    # For non-custom embedders, we can't easily generate embeddings
                    # without risking authentication issues
                    logger.warning(f"Non-custom embedder found, cannot generate async embeddings: {self.embedder.get('provider')}")
                    return None
                
            # Handle different embedder object interfaces (legacy support)
            elif hasattr(self.embedder, '__call__'):
                # Embedder is a function
                embeddings = self.embedder([text])
                if embeddings and len(embeddings) > 0:
                    # Convert numpy array to list if needed
                    embedding = embeddings[0]
                    if hasattr(embedding, 'tolist'):
                        return embedding.tolist()
                    return embedding
                return None
                
            elif hasattr(self.embedder, 'embed'):
                # Embedder has embed method
                return self.embedder.embed(text)
                
            elif hasattr(self.embedder, 'embed_documents'):
                # Embedder has embed_documents method (LangChain style)
                embeddings = self.embedder.embed_documents([text])
                return embeddings[0] if embeddings else None
                
            else:
                logger.error(f"Embedder does not have a recognized interface. Type: {type(self.embedder)}")
                return None
                
        except Exception as e:
            logger.error(f"Error generating embedding: {e}")
            return None
            
    # CrewAI-specific methods that might be called
    
    def add(self, texts: List[str], metadatas: Optional[List[Dict]] = None, **kwargs) -> None:
        """
        Add multiple texts to storage (CrewAI compatibility).
        
        Args:
            texts: List of texts to add
            metadatas: Optional list of metadata dicts
            **kwargs: Additional arguments
        """
        if not metadatas:
            metadatas = [{}] * len(texts)
            
        for text, metadata in zip(texts, metadatas):
            self.save(value=text, metadata=metadata, **kwargs)
            
    def similarity_search(self, query: str, k: int = 3, **kwargs) -> List[Dict]:
        """
        Similarity search (CrewAI compatibility).
        
        Args:
            query: Query text
            k: Number of results
            **kwargs: Additional arguments
            
        Returns:
            List of results
        """
        return self.search(query=query, top_k=k, **kwargs)
        
    def similarity_search_with_score(self, query: str, k: int = 3, **kwargs) -> List[tuple]:
        """
        Similarity search with scores (CrewAI compatibility).
        
        Args:
            query: Query text
            k: Number of results
            **kwargs: Additional arguments
            
        Returns:
            List of (document, score) tuples
        """
        results = self.search(query=query, top_k=k, **kwargs)
        # Convert to expected format
        return [(result, result.get('score', 0.0)) for result in results]
        
    def load(self, task: str, latest_n: int = 3) -> List[Dict[str, Any]]:
        """
        Load memories for a task (CrewAI LongTermMemory compatibility).
        
        This method is called by CrewAI's LongTermMemory.search() method.
        
        Args:
            task: Task description to search for relevant memories
            latest_n: Number of recent memories to retrieve
            
        Returns:
            List of memory entries
        """
        logger.info(f"[CrewAIDatabricksWrapper.load] Called for {self.memory_type} memory with task: {task}")
        
        # Check if memory is disabled for the current agent
        if not self._is_memory_enabled_for_current_agent():
            logger.info(f"[CrewAIDatabricksWrapper.load] Memory is disabled for current agent, returning empty results for {self.memory_type}")
            return []
        
        # Use the search method to find relevant memories
        results = self.search(query=task, top_k=latest_n)
        
        # Format results for CrewAI's expectations
        formatted_results = []
        for result in results:
            # Extract the actual content from the result
            content = result.get('data', result.get('content', ''))
            metadata = result.get('metadata', {})
            
            # Create a formatted result that includes both content and metadata
            formatted_result = {
                'content': content,
                'metadata': metadata,
                'score': result.get('score', 0.0)
            }
            
            # Include any additional fields from the original result
            for key, value in result.items():
                if key not in ['data', 'content', 'metadata', 'score', 'embedding']:
                    formatted_result[key] = value
                    
            formatted_results.append(formatted_result)
            
        logger.debug(f"[CrewAIDatabricksWrapper.load] Returning {len(formatted_results)} results")
        return formatted_results
        
    def get_entities(self, limit: int = 10) -> List[str]:
        """
        Get list of entities (EntityMemory compatibility).
        
        This method is called by CrewAI's EntityMemory.
        
        Args:
            limit: Maximum number of entities to retrieve
            
        Returns:
            List of entity names
        """
        entity_logger.info(f"[CrewAIDatabricksWrapper.get_entities] Called for {self.memory_type} memory")
        
        # Search for all entities
        results = self.search(query="", top_k=limit)
        
        # Extract entity names from results
        entities = []
        for result in results:
            metadata = result.get('metadata', {})
            entity_name = metadata.get('entity_name')
            if entity_name and entity_name not in entities:
                entities.append(entity_name)
                
        entity_logger.debug(f"[CrewAIDatabricksWrapper.get_entities] Returning {len(entities)} entities")
        return entities