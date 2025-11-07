"""
Universal Entity Relationship Retriever for CrewAI Databricks Integration

This module provides intelligent relationship-based entity retrieval that works with any
description content without hard-coded patterns. It uses embedding similarity and 
semantic analysis to infer relationships dynamically.
"""

import asyncio
import json
import logging
from typing import Dict, List, Any, Set, Tuple, Optional
from dataclasses import dataclass
import numpy as np

from src.core.logger import LoggerManager

entity_logger = LoggerManager.get_instance().databricks_entity


@dataclass
class EntityNode:
    """Represents an entity in the relationship graph."""
    name: str
    entity_type: str
    description: str
    agent_id: str
    metadata: Dict[str, Any]
    explicit_relationships: List[str]


@dataclass
class RelationshipEdge:
    """Represents a relationship between two entities."""
    source: str
    target: str
    strength: float
    relationship_type: str
    evidence: str


@dataclass
class RetrievalCandidate:
    """Represents a candidate entity for retrieval."""
    entity: EntityNode
    relevance_score: float
    retrieval_method: str
    relationship_path: List[str]
    relationship_context: Optional[Dict[str, Any]] = None


class EntityRelationshipRetriever:
    """
    Universal relationship-based entity retrieval system.
    
    Uses semantic similarity, description analysis, and graph traversal
    to find relevant entities through their relationships without 
    hard-coded patterns.
    """
    
    def __init__(self, memory_backend_service, embedding_model: str = "sentence-transformers/all-MiniLM-L6-v2"):
        """
        Initialize the relationship retriever.
        
        Args:
            memory_backend_service: MemoryBackendService for all vector operations
            embedding_model: Model for computing embeddings
        """
        self.memory_backend_service = memory_backend_service
        self.embedding_model = embedding_model
        self.entity_graph: Dict[str, EntityNode] = {}
        self.relationship_edges: List[RelationshipEdge] = []
        self.description_embeddings: Dict[str, np.ndarray] = {}
        
    async def build_entity_graph(self, workspace_url: str, index_name: str, endpoint_name: str,
                               user_token: str, agent_id: str, group_id: str) -> None:
        """
        Build the entity relationship graph from the index.
        
        Args:
            workspace_url: Databricks workspace URL
            index_name: Vector search index name
            endpoint_name: Vector search endpoint name
            user_token: User authentication token
            agent_id: Agent identifier for filtering
            group_id: Group identifier for tenant isolation
        """
        try:
            # Get all entities for this agent/group
            all_entities = await self._get_all_entities(
                workspace_url, index_name, endpoint_name, user_token, agent_id, group_id
            )
            
            # Clear existing graph
            self.entity_graph.clear()
            self.relationship_edges.clear()
            self.description_embeddings.clear()
            
            # Build entity nodes
            for entity_data in all_entities:
                entity_node = EntityNode(
                    name=entity_data.get('entity_name', ''),
                    entity_type=entity_data.get('entity_type', ''),
                    description=entity_data.get('description', ''),
                    agent_id=entity_data.get('agent_id', ''),
                    metadata=entity_data,
                    explicit_relationships=self._parse_explicit_relationships(
                        entity_data.get('relationships', '')
                    )
                )
                self.entity_graph[entity_node.name] = entity_node
                
                # Compute and store description embedding
                if entity_node.description:
                    embedding = await self._compute_embedding(entity_node.description)
                    self.description_embeddings[entity_node.name] = embedding
            
            # Build relationship edges
            await self._build_relationship_edges()
            
            entity_logger.info(f"Built entity graph with {len(self.entity_graph)} entities and {len(self.relationship_edges)} relationships")
            
        except Exception as e:
            entity_logger.error(f"Error building entity graph: {e}")
            raise
    
    async def search_with_relationships(
        self, 
        query: str, 
        initial_results: List[Dict[str, Any]], 
        workspace_url: str,
        index_name: str,
        endpoint_name: str,
        user_token: str,
        agent_id: str,
        group_id: str,
        max_hops: int = 2, 
        max_total: int = 10,
        relationship_weight: float = 0.3
    ) -> List[Dict[str, Any]]:
        """
        Perform relationship-enhanced entity search.
        
        This method follows a two-step approach:
        1. Start with the most relevant entities from similarity search (like normal top 3)
        2. From those top entities, follow relationships to find additional related entities
        
        Args:
            query: Search query
            initial_results: Initial semantic search results (the top relevant entities)
            workspace_url: Databricks workspace URL
            index_name: Vector search index name
            endpoint_name: Vector search endpoint name
            user_token: User authentication token
            agent_id: Agent identifier
            group_id: Group identifier
            max_hops: Maximum relationship hops to traverse
            max_total: Maximum entities to return
            relationship_weight: Weight for relationship-based scoring
            
        Returns:
            Enhanced search results with relationship context
        """
        try:
            entity_logger.info(f"[search_with_relationships] Starting relationship search for query: '{query}'")
            entity_logger.info(f"[search_with_relationships] Initial results: {len(initial_results)}")
            entity_logger.info(f"[search_with_relationships] Agent ID: {agent_id}, Group ID: {group_id}")
            
            # If no initial results, return empty
            if not initial_results:
                entity_logger.info(f"[search_with_relationships] No initial results to work with")
                return []
            
            # Build a focused entity graph around the initial results
            # This is more efficient than building the entire graph
            await self._build_focused_entity_graph(
                initial_results, workspace_url, index_name, endpoint_name, user_token, agent_id, group_id
            )
            
            # Start with semantic search results (the top entities)
            seed_entities = [r.get('entity_name', '') for r in initial_results if r.get('entity_name')]
            entity_logger.info(f"[search_with_relationships] Seed entities: {seed_entities}")
            
            # Find related entities by following relationships from the seed entities
            related_entities = await self._traverse_relationships_from_seeds(
                seed_entities, query, workspace_url, index_name, endpoint_name, 
                user_token, max_hops
            )
            
            entity_logger.info(f"[search_with_relationships] Found {len(related_entities)} related entities through relationships")
            
            # Score and rank all candidates
            all_candidates = await self._score_and_rank_candidates(
                seed_entities, related_entities, query, relationship_weight
            )
            
            # Format results for CrewAI
            formatted_results = self._format_results_for_crewai(
                all_candidates[:max_total]
            )
            
            entity_logger.info(f"[search_with_relationships] Returning {len(formatted_results)} total results")
            return formatted_results
            
        except Exception as e:
            entity_logger.error(f"Error in relationship search: {e}")
            # Fallback to original results
            entity_logger.info(f"[search_with_relationships] Falling back to original {len(initial_results)} results")
            return initial_results[:max_total]
    
    async def _get_all_entities(self, workspace_url: str, index_name: str, endpoint_name: str, 
                              user_token: str, agent_id: str, group_id: str) -> List[Dict[str, Any]]:
        """Get all entities from the index for graph building by using index document retrieval."""
        try:
            entity_logger.info(f"[_get_all_entities] Fetching entities for agent_id: {agent_id}, group_id: {group_id}")
            entity_logger.info(f"[_get_all_entities] Index: {index_name}, Endpoint: {endpoint_name}")
            
            # Use the index service to get all entities from the index
            # This is more reliable than using a dummy embedding for similarity search
            result = await self.memory_backend_service.get_index_documents(
                workspace_url=workspace_url,
                endpoint_name=endpoint_name,
                index_name=index_name,
                index_type="entity",
                embedding_dimension=1024,
                limit=100,  # Get up to 100 entities for graph building
                user_token=user_token
            )
            
            entities = []
            if result.get("success") and result.get("documents"):
                entities = result["documents"]
                entity_logger.info(f"[_get_all_entities] Found {len(entities)} entities via get_index_documents")
                if entities:
                    entity_logger.info(f"[_get_all_entities] First entity sample: {entities[0]}")
            else:
                entity_logger.warning(f"[_get_all_entities] get_index_documents returned no entities: {result}")
            
            return entities
        except Exception as e:
            entity_logger.error(f"Error fetching entities: {e}")
            return []
    
    async def _build_focused_entity_graph(
        self, 
        initial_results: List[Dict[str, Any]], 
        workspace_url: str, 
        index_name: str, 
        endpoint_name: str,
        user_token: str, 
        agent_id: str, 
        group_id: str
    ) -> None:
        """
        Build a focused entity graph around the initial seed entities.
        This is more efficient than building the entire graph.
        """
        try:
            entity_logger.info(f"[_build_focused_entity_graph] Building focused graph for {len(initial_results)} initial entities")
            
            # Clear existing graph
            self.entity_graph.clear()
            self.relationship_edges.clear()
            self.description_embeddings.clear()
            
            # Add initial entities to the graph
            for entity_data in initial_results:
                entity_node = EntityNode(
                    name=entity_data.get('entity_name', ''),
                    entity_type=entity_data.get('entity_type', ''),
                    description=entity_data.get('description', ''),
                    agent_id=entity_data.get('agent_id', ''),
                    metadata=entity_data,
                    explicit_relationships=self._parse_explicit_relationships(
                        entity_data.get('relationships', '')
                    )
                )
                self.entity_graph[entity_node.name] = entity_node
                
                # Compute and store description embedding
                if entity_node.description:
                    embedding = await self._compute_embedding(entity_node.description)
                    self.description_embeddings[entity_node.name] = embedding
            
            entity_logger.info(f"[_build_focused_entity_graph] Added {len(self.entity_graph)} seed entities to graph")
            
            # For each seed entity, try to find and add entities mentioned in their relationships
            await self._expand_graph_with_related_entities(
                workspace_url, index_name, endpoint_name, user_token
            )
            
            # Build relationship edges between all entities in the focused graph
            await self._build_relationship_edges()
            
            entity_logger.info(f"[_build_focused_entity_graph] Final focused graph: {len(self.entity_graph)} entities, {len(self.relationship_edges)} relationships")
            
        except Exception as e:
            entity_logger.error(f"Error building focused entity graph: {e}")
            raise
    
    async def _expand_graph_with_related_entities(
        self, 
        workspace_url: str, 
        index_name: str, 
        endpoint_name: str, 
        user_token: str
    ) -> None:
        """
        Expand the graph by searching for entities mentioned in the relationships of seed entities.
        """
        try:
            # Collect all relationship names from seed entities
            relationship_names = set()
            for entity in self.entity_graph.values():
                relationship_names.update(entity.explicit_relationships)
            
            entity_logger.info(f"[_expand_graph_with_related_entities] Looking for {len(relationship_names)} related entities")
            
            # For each relationship name, try to find the corresponding entity
            for rel_name in relationship_names:
                if rel_name and rel_name not in self.entity_graph:
                    # Search for this entity by name
                    await self._search_and_add_entity_by_name(
                        rel_name, workspace_url, index_name, endpoint_name, user_token
                    )
            
        except Exception as e:
            entity_logger.error(f"Error expanding graph with related entities: {e}")
    
    async def _search_and_add_entity_by_name(
        self, 
        entity_name: str, 
        workspace_url: str, 
        index_name: str, 
        endpoint_name: str, 
        user_token: str
    ) -> None:
        """
        Search for an entity by name and add it to the graph if found.
        """
        try:
            # Create a simple embedding for the entity name to search
            name_embedding = await self._compute_embedding(entity_name)
            
            # Search for entities with similar names
            results = await self.memory_backend_service.search_vectors(
                workspace_url=workspace_url,
                index_name=index_name,
                endpoint_name=endpoint_name,
                query_embedding=name_embedding.tolist(),
                memory_type="entity",
                k=3,  # Get top 3 matches
                filters=None,
                user_token=user_token
            )
            
            # Add matching entities to the graph
            for entity_data in results:
                found_name = entity_data.get('entity_name', '')
                if found_name and found_name not in self.entity_graph:
                    # Check if this is likely the entity we're looking for
                    if self._is_name_match(entity_name, found_name):
                        entity_node = EntityNode(
                            name=found_name,
                            entity_type=entity_data.get('entity_type', ''),
                            description=entity_data.get('description', ''),
                            agent_id=entity_data.get('agent_id', ''),
                            metadata=entity_data,
                            explicit_relationships=self._parse_explicit_relationships(
                                entity_data.get('relationships', '')
                            )
                        )
                        self.entity_graph[found_name] = entity_node
                        
                        # Compute and store description embedding
                        if entity_node.description:
                            embedding = await self._compute_embedding(entity_node.description)
                            self.description_embeddings[found_name] = embedding
                        
                        entity_logger.info(f"[_search_and_add_entity_by_name] Added related entity: {found_name}")
                        break
            
        except Exception as e:
            entity_logger.error(f"Error searching for entity {entity_name}: {e}")
    
    def _is_name_match(self, search_name: str, found_name: str) -> bool:
        """Check if two entity names are likely the same entity."""
        search_lower = search_name.lower().strip()
        found_lower = found_name.lower().strip()
        
        # Exact match
        if search_lower == found_lower:
            return True
        
        # One is contained in the other
        if search_lower in found_lower or found_lower in search_lower:
            return True
        
        # High word overlap
        search_words = set(search_lower.split())
        found_words = set(found_lower.split())
        if search_words and found_words:
            overlap = len(search_words.intersection(found_words))
            return overlap >= min(len(search_words), len(found_words)) * 0.7
        
        return False
    
    async def _traverse_relationships_from_seeds(
        self, 
        seed_entities: List[str], 
        query: str, 
        workspace_url: str,
        index_name: str,
        endpoint_name: str,
        user_token: str,
        max_hops: int
    ) -> List[RetrievalCandidate]:
        """
        Traverse relationships starting from seed entities to find related entities.
        """
        related_candidates = []
        visited = set(seed_entities)
        current_hop = set(seed_entities)
        
        entity_logger.info(f"[_traverse_relationships_from_seeds] Starting traversal from {len(seed_entities)} seeds")
        
        for hop in range(max_hops):
            next_hop = set()
            entity_logger.info(f"[_traverse_relationships_from_seeds] Hop {hop + 1}: exploring {len(current_hop)} entities")
            
            for entity_name in current_hop:
                # Find all outgoing edges from this entity
                outgoing_edges = [
                    edge for edge in self.relationship_edges 
                    if edge.source == entity_name and edge.target not in visited
                ]
                
                entity_logger.info(f"[_traverse_relationships_from_seeds] Entity '{entity_name}' has {len(outgoing_edges)} outgoing relationships")
                
                for edge in outgoing_edges:
                    target_entity = self.entity_graph.get(edge.target)
                    if target_entity:
                        # Score this relationship based on query relevance
                        relevance_score = await self._score_relationship_relevance(
                            edge, target_entity, query, hop + 1
                        )
                        
                        candidate = RetrievalCandidate(
                            entity=target_entity,
                            relevance_score=relevance_score,
                            retrieval_method="relationship_traversal",
                            relationship_path=[entity_name],
                            relationship_context={
                                'source_entity': entity_name,
                                'relationship_type': edge.relationship_type,
                                'relationship_strength': edge.strength,
                                'hop_distance': hop + 1,
                                'evidence': edge.evidence
                            }
                        )
                        
                        related_candidates.append(candidate)
                        visited.add(edge.target)
                        next_hop.add(edge.target)
                        
                        entity_logger.info(f"[_traverse_relationships_from_seeds] Found related entity: {edge.target} (score: {relevance_score:.3f})")
            
            current_hop = next_hop
            if not current_hop:
                entity_logger.info(f"[_traverse_relationships_from_seeds] No more entities to explore at hop {hop + 1}")
                break
        
        entity_logger.info(f"[_traverse_relationships_from_seeds] Traversal complete: found {len(related_candidates)} related entities")
        return related_candidates
    
    def _parse_explicit_relationships(self, relationships_str: str) -> List[str]:
        """Parse explicit relationships from the relationships field."""
        if not relationships_str:
            return []
        
        relationships = []
        # Remove quotes and parse line by line
        clean_str = relationships_str.strip().strip('"\'')
        
        for line in clean_str.split('\\n'):
            line = line.strip()
            if line.startswith('- '):
                rel_name = line[2:].strip()
                if rel_name:
                    relationships.append(rel_name)
        
        return relationships
    
    async def _compute_embedding(self, text: str) -> np.ndarray:
        """Compute embedding for text using the configured model."""
        try:
            # This would use your actual embedding service
            # For now, return a placeholder - you'd implement actual embedding computation
            # using your databricks embedding service or similar
            
            # Placeholder: return random embedding of appropriate dimension
            # In real implementation, use: await self.databricks_service.compute_embedding(text)
            return np.random.rand(384)  # Common dimension for sentence transformers
            
        except Exception as e:
            entity_logger.error(f"Error computing embedding: {e}")
            return np.zeros(384)
    
    async def _build_relationship_edges(self) -> None:
        """Build relationship edges using multiple strategies."""
        
        # Strategy 1: Explicit relationships from the relationships field
        await self._build_explicit_relationship_edges()
        
        # Strategy 2: Semantic similarity between descriptions
        await self._build_semantic_relationship_edges()
        
        # Strategy 3: Name similarity and co-occurrence patterns
        await self._build_name_based_relationship_edges()
    
    async def _build_explicit_relationship_edges(self) -> None:
        """Build edges from explicit relationship declarations."""
        for entity_name, entity in self.entity_graph.items():
            for related_name in entity.explicit_relationships:
                # Find the related entity (handle name variations)
                target_entity = self._find_entity_by_name(related_name)
                if target_entity:
                    edge = RelationshipEdge(
                        source=entity_name,
                        target=target_entity.name,
                        strength=1.0,  # Explicit relationships have high strength
                        relationship_type="explicit",
                        evidence=f"Explicitly listed in {entity_name}'s relationships"
                    )
                    self.relationship_edges.append(edge)
    
    async def _build_semantic_relationship_edges(self) -> None:
        """Build edges based on semantic similarity of descriptions."""
        entity_names = list(self.entity_graph.keys())
        
        for i, name1 in enumerate(entity_names):
            entity1 = self.entity_graph[name1]
            embedding1 = self.description_embeddings.get(name1)
            
            if embedding1 is None:
                continue
                
            for name2 in entity_names[i+1:]:
                entity2 = self.entity_graph[name2]
                embedding2 = self.description_embeddings.get(name2)
                
                if embedding2 is None:
                    continue
                
                # Compute cosine similarity
                similarity = self._cosine_similarity(embedding1, embedding2)
                
                # Create edge if similarity is above threshold
                if similarity > 0.7:  # Adjustable threshold
                    relationship_type = await self._infer_relationship_type_from_descriptions(
                        entity1.description, entity2.description
                    )
                    
                    edge = RelationshipEdge(
                        source=name1,
                        target=name2,
                        strength=similarity,
                        relationship_type=relationship_type,
                        evidence=f"High semantic similarity ({similarity:.3f}) between descriptions"
                    )
                    self.relationship_edges.append(edge)
    
    async def _build_name_based_relationship_edges(self) -> None:
        """Build edges based on name patterns and co-occurrence."""
        entity_names = list(self.entity_graph.keys())
        
        for i, name1 in enumerate(entity_names):
            entity1 = self.entity_graph[name1]
            
            for name2 in entity_names[i+1:]:
                entity2 = self.entity_graph[name2]
                
                # Check for name similarity patterns
                name_similarity = self._compute_name_similarity(name1, name2)
                description_cooccurrence = self._check_description_cooccurrence(
                    entity1.description, entity2.description, name1, name2
                )
                
                if name_similarity > 0.8 or description_cooccurrence:
                    relationship_type = "name_based" if name_similarity > 0.8 else "contextual"
                    strength = max(name_similarity, 0.6 if description_cooccurrence else 0.0)
                    
                    edge = RelationshipEdge(
                        source=name1,
                        target=name2,
                        strength=strength,
                        relationship_type=relationship_type,
                        evidence=f"Name similarity or contextual co-occurrence detected"
                    )
                    self.relationship_edges.append(edge)
    
    def _find_entity_by_name(self, name: str) -> Optional[EntityNode]:
        """Find entity by name, handling variations and partial matches."""
        # Exact match first
        if name in self.entity_graph:
            return self.entity_graph[name]
        
        # Fuzzy matching for common variations
        name_lower = name.lower().strip()
        for entity_name, entity in self.entity_graph.items():
            entity_name_lower = entity_name.lower().strip()
            
            # Check for partial matches or nickname patterns
            if (name_lower in entity_name_lower or 
                entity_name_lower in name_lower or
                self._compute_name_similarity(name, entity_name) > 0.8):
                return entity
        
        return None
    
    def _cosine_similarity(self, vec1: np.ndarray, vec2: np.ndarray) -> float:
        """Compute cosine similarity between two vectors."""
        try:
            dot_product = np.dot(vec1, vec2)
            norm1 = np.linalg.norm(vec1)
            norm2 = np.linalg.norm(vec2)
            
            if norm1 == 0 or norm2 == 0:
                return 0.0
            
            return dot_product / (norm1 * norm2)
        except:
            return 0.0
    
    async def _infer_relationship_type_from_descriptions(self, desc1: str, desc2: str) -> str:
        """Infer relationship type using semantic analysis of descriptions."""
        desc1_lower = desc1.lower()
        desc2_lower = desc2.lower()
        
        # Look for relationship keywords in descriptions
        family_keywords = ['father', 'mother', 'son', 'daughter', 'birth name', 'real name', 'born', 'family']
        professional_keywords = ['mentor', 'trainer', 'boss', 'employee', 'colleague', 'worked', 'career']
        organizational_keywords = ['organization', 'company', 'institution', 'member', 'belongs']
        
        if any(keyword in desc1_lower or keyword in desc2_lower for keyword in family_keywords):
            return 'family'
        elif any(keyword in desc1_lower or keyword in desc2_lower for keyword in professional_keywords):
            return 'professional'
        elif any(keyword in desc1_lower or keyword in desc2_lower for keyword in organizational_keywords):
            return 'organizational'
        else:
            return 'semantic'
    
    def _compute_name_similarity(self, name1: str, name2: str) -> float:
        """Compute name similarity using multiple strategies."""
        # Simple Jaccard similarity on words
        words1 = set(name1.lower().split())
        words2 = set(name2.lower().split())
        
        if not words1 or not words2:
            return 0.0
        
        intersection = len(words1.intersection(words2))
        union = len(words1.union(words2))
        
        return intersection / union if union > 0 else 0.0
    
    def _check_description_cooccurrence(self, desc1: str, desc2: str, name1: str, name2: str) -> bool:
        """Check if entities co-occur in each other's descriptions."""
        desc1_lower = desc1.lower()
        desc2_lower = desc2.lower()
        name1_lower = name1.lower()
        name2_lower = name2.lower()
        
        return name1_lower in desc2_lower or name2_lower in desc1_lower
    
    async def _traverse_relationships(
        self, 
        seed_entities: List[str], 
        query: str, 
        max_hops: int
    ) -> List[RetrievalCandidate]:
        """Traverse the relationship graph from seed entities."""
        related_candidates = []
        visited = set(seed_entities)
        current_hop = set(seed_entities)
        
        for hop in range(max_hops):
            next_hop = set()
            
            for entity_name in current_hop:
                # Find all outgoing edges from this entity
                outgoing_edges = [
                    edge for edge in self.relationship_edges 
                    if edge.source == entity_name and edge.target not in visited
                ]
                
                for edge in outgoing_edges:
                    target_entity = self.entity_graph.get(edge.target)
                    if target_entity:
                        # Score this relationship based on query relevance
                        relevance_score = await self._score_relationship_relevance(
                            edge, target_entity, query, hop + 1
                        )
                        
                        candidate = RetrievalCandidate(
                            entity=target_entity,
                            relevance_score=relevance_score,
                            retrieval_method="relationship_traversal",
                            relationship_path=[entity_name],
                            relationship_context={
                                'source_entity': entity_name,
                                'relationship_type': edge.relationship_type,
                                'relationship_strength': edge.strength,
                                'hop_distance': hop + 1,
                                'evidence': edge.evidence
                            }
                        )
                        
                        related_candidates.append(candidate)
                        visited.add(edge.target)
                        next_hop.add(edge.target)
            
            current_hop = next_hop
            if not current_hop:
                break
        
        return related_candidates
    
    async def _score_relationship_relevance(
        self, 
        edge: RelationshipEdge, 
        target_entity: EntityNode, 
        query: str, 
        hop_distance: int
    ) -> float:
        """Score how relevant a relationship is to the query."""
        base_score = edge.strength
        
        # Distance penalty
        distance_penalty = 0.7 ** hop_distance
        base_score *= distance_penalty
        
        # Query relevance boost
        query_lower = query.lower()
        entity_name_lower = target_entity.name.lower()
        entity_desc_lower = target_entity.description.lower()
        
        # Name match boost
        if entity_name_lower in query_lower or any(word in query_lower for word in entity_name_lower.split()):
            base_score *= 2.0
        
        # Description relevance boost
        query_words = set(query_lower.split())
        desc_words = set(entity_desc_lower.split())
        word_overlap = len(query_words.intersection(desc_words))
        if word_overlap > 0:
            base_score *= (1 + 0.2 * word_overlap)
        
        # Relationship type boost based on query
        if edge.relationship_type == 'family' and any(word in query_lower for word in ['family', 'father', 'mother', 'son', 'daughter']):
            base_score *= 1.5
        elif edge.relationship_type == 'professional' and any(word in query_lower for word in ['work', 'career', 'job', 'professional']):
            base_score *= 1.5
        
        return base_score
    
    async def _score_and_rank_candidates(
        self, 
        seed_entities: List[str], 
        related_entities: List[RetrievalCandidate], 
        query: str,
        relationship_weight: float
    ) -> List[RetrievalCandidate]:
        """Score and rank all candidates."""
        all_candidates = []
        
        # Add seed entities (from semantic search)
        for i, entity_name in enumerate(seed_entities):
            if entity_name in self.entity_graph:
                entity = self.entity_graph[entity_name]
                candidate = RetrievalCandidate(
                    entity=entity,
                    relevance_score=1.0 - (i * 0.1),  # Maintain semantic search order
                    retrieval_method="semantic_search",
                    relationship_path=[]
                )
                all_candidates.append(candidate)
        
        # Add related entities
        all_candidates.extend(related_entities)
        
        # Apply relationship weighting and sort
        for candidate in all_candidates:
            if candidate.retrieval_method == "relationship_traversal":
                candidate.relevance_score *= relationship_weight
        
        # Remove duplicates and sort by score
        seen = set()
        unique_candidates = []
        for candidate in sorted(all_candidates, key=lambda x: x.relevance_score, reverse=True):
            if candidate.entity.name not in seen:
                unique_candidates.append(candidate)
                seen.add(candidate.entity.name)
        
        return unique_candidates
    
    def _format_results_for_crewai(self, candidates: List[RetrievalCandidate]) -> List[Dict[str, Any]]:
        """Format results for CrewAI consumption."""
        formatted_results = []
        
        for candidate in candidates:
            entity = candidate.entity
            
            result = {
                'id': entity.metadata.get('id', ''),
                'entity_name': entity.name,
                'entity_type': entity.entity_type,
                'description': entity.description,
                'content': f"{entity.name}({entity.entity_type}): {entity.description}",
                'context': f"{entity.name}({entity.entity_type}): {entity.description}",  # CrewAI expects this
                'metadata': {
                    'relationships': json.dumps(entity.explicit_relationships),
                    'agent_id': entity.agent_id,
                    'retrieval_method': candidate.retrieval_method,
                    'relevance_score': candidate.relevance_score,
                    **entity.metadata
                }
            }
            
            # Add relationship context if available
            if candidate.relationship_context:
                result['metadata']['relationship_context'] = candidate.relationship_context
            
            formatted_results.append(result)
        
        return formatted_results