"""
Knowledge Source Factory for creating knowledge sources from configuration.

This factory creates the appropriate knowledge source objects based on configuration,
avoiding JSON serialization issues by creating objects only in the subprocess.
"""

import logging
from typing import List, Dict, Any, Optional
from crewai.knowledge.source.string_knowledge_source import StringKnowledgeSource

logger = logging.getLogger(__name__)


class KnowledgeSourceFactory:
    """Factory for creating knowledge sources from configuration."""
    
    @staticmethod
    def create_knowledge_sources(
        knowledge_configs: List[Dict[str, Any]],
        execution_id: Optional[str] = None,
        group_id: Optional[str] = None,
        embedder_config: Optional[Any] = None,
        agent_id: Optional[str] = None
    ) -> List[Any]:
        """
        Create knowledge source objects from configuration.

        Args:
            knowledge_configs: List of knowledge source configurations
            execution_id: Execution ID for isolation
            group_id: Group ID for tenant isolation
            embedder_config: Configuration for the embedder
            agent_id: Agent ID that will access this knowledge source

        Returns:
            List of knowledge source objects
        """
        knowledge_sources = []
        
        if not knowledge_configs:
            return knowledge_sources
        
        logger.info(f"[KnowledgeFactory] Creating {len(knowledge_configs)} knowledge sources")
        logger.info(f"[KnowledgeFactory] Execution ID: {execution_id}, Group ID: {group_id}")
        
        for config in knowledge_configs:
            try:
                source_type = config.get('type', 'unknown')
                logger.info(f"[KnowledgeFactory] Processing {source_type} knowledge source")
                
                if source_type == 'databricks_volume':
                    # Import here to avoid circular dependencies
                    from src.engines.crewai.knowledge.databricks_vector_knowledge_source import DatabricksVectorKnowledgeSource
                    
                    # Extract file path from config
                    file_path = config.get('source', '')
                    metadata = config.get('metadata', {})
                    
                    # Create knowledge source with execution isolation and agent access control
                    knowledge_source = DatabricksVectorKnowledgeSource(
                        file_paths=[file_path] if file_path else [],
                        volume_path=file_path.split('/user_')[0] if '/user_' in file_path else None,
                        execution_id=execution_id or metadata.get('execution_id', 'default'),
                        group_id=group_id or metadata.get('group_id', 'default'),
                        embedder_config=embedder_config,  # Pass the embedder config
                        agent_ids=[agent_id] if agent_id else []  # Pass agent ID for access control
                    )
                    
                    knowledge_sources.append(knowledge_source)
                    logger.info(f"[KnowledgeFactory] Created DatabricksVectorKnowledgeSource with collection: {knowledge_source.collection_name}")
                    
                elif source_type == 'string':
                    # Handle string knowledge sources
                    content = config.get('content', '')
                    knowledge_source = StringKnowledgeSource(content=content)
                    knowledge_sources.append(knowledge_source)
                    logger.info(f"[KnowledgeFactory] Created StringKnowledgeSource")
                    
                else:
                    logger.warning(f"[KnowledgeFactory] Unknown knowledge source type: {source_type}")
                    
            except Exception as e:
                logger.error(f"[KnowledgeFactory] Error creating knowledge source: {e}", exc_info=True)
        
        logger.info(f"[KnowledgeFactory] Successfully created {len(knowledge_sources)} knowledge sources")
        return knowledge_sources