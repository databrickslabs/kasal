# Advanced Databricks Memory Implementation TODO List

## Overview
This document tracks the implementation of advanced memory types differentiation for Databricks Vector Search in the Kasal project. The goal is to properly differentiate between short-term, long-term, and entity memory types using hybrid search, reranking, and temporal awareness.

## 1. Core Memory Infrastructure Changes

### 1.1 Memory Type Differentiation
- [ ] Add `task_hash` field to long-term memory records for exact task matching
- [ ] Implement temporal decay functions for short-term memory
- [ ] Add `interaction_sequence` and `session_id` fields to short-term memory
- [ ] Add graph-like relationship tracking for entity memory
- [ ] Implement memory-type specific metadata schemas

### 1.2 Hybrid Search Implementation
- [ ] Update `databricks_vector_storage.py` to support hybrid search for long-term memory
- [ ] Add `query_type='hybrid'` parameter support in search methods
- [ ] Implement task hash generation for exact matching
- [ ] Add keyword search capability alongside vector search
- [ ] Implement Reciprocal Rank Fusion (RRF) scoring

### 1.3 Reranking System
- [ ] Create new `memory_reranker.py` module
- [ ] Implement cross-encoder reranking for short-term memory
- [ ] Implement quality-based reranking for long-term memory
- [ ] Add temporal relevance scoring
- [ ] Create reranking configuration options

## 2. Memory-Specific Enhancements

### 2.1 Short-Term Memory (Working Memory)
- [ ] Implement sliding window mechanism (limit to recent N interactions)
- [ ] Add contextual chunking with surrounding context
- [ ] Implement temporal decay weighting
- [ ] Add session tracking for conversation continuity
- [ ] Create memory pruning mechanism for old entries

### 2.2 Long-Term Memory (Semantic + Procedural)
- [ ] Implement memory consolidation from short-term to long-term
- [ ] Add quality score evaluation using LLM
- [ ] Create similar memory merging functionality
- [ ] Implement importance-based filtering
- [ ] Add task performance tracking

### 2.3 Entity Memory (Knowledge Graph)
- [ ] Implement entity relationship extraction
- [ ] Create bidirectional linking system
- [ ] Add entity type hierarchies
- [ ] Implement relationship strength scoring
- [ ] Add entity evolution tracking over time

## 3. Advanced Retrieval Features

### 3.1 Memory Consolidation Service
- [ ] Create `memory_consolidator.py` service
- [ ] Implement importance evaluation algorithm
- [ ] Add similarity detection for memory merging
- [ ] Create consolidation scheduling mechanism
- [ ] Add configuration for consolidation thresholds

### 3.2 Temporal Awareness
- [ ] Implement time-based decay functions
- [ ] Add memory freshness scoring
- [ ] Create temporal knowledge graph for entities
- [ ] Implement memory aging mechanisms
- [ ] Add time-window based retrieval options

### 3.3 Advanced Search Features
- [ ] Implement multi-stage retrieval pipeline
- [ ] Add metadata filtering capabilities
- [ ] Create memory-type specific post-processing
- [ ] Implement result aggregation strategies
- [ ] Add search result caching layer

## 4. File Changes

### 4.1 New Files to Create
- [ ] `src/backend/src/engines/crewai/memory/memory_reranker.py`
- [ ] `src/backend/src/engines/crewai/memory/memory_consolidator.py`
- [ ] `src/backend/src/engines/crewai/memory/temporal_scorer.py`
- [ ] `src/backend/src/engines/crewai/memory/advanced_databricks_memory.py`

### 4.2 Files to Modify
- [ ] `src/backend/src/engines/crewai/memory/databricks_vector_storage.py`
  - Add hybrid search support
  - Implement memory-type specific save logic
  - Add temporal metadata tracking
- [ ] `src/backend/src/engines/crewai/memory/crewai_databricks_wrapper.py`
  - Update search method for hybrid queries
  - Add reranking integration
  - Implement memory-type specific behaviors
- [ ] `src/backend/src/engines/crewai/memory/memory_backend_factory.py`
  - Add advanced memory configuration options
  - Integrate new components
- [ ] `src/backend/src/schemas/memory_backend.py`
  - Add new configuration fields for advanced features

## 5. Unit Tests

### 5.1 New Test Files
- [ ] `src/backend/tests/unit/engines/crewai/memory/test_memory_reranker.py`
  - Test reranking algorithms for each memory type
  - Test temporal scoring functions
  - Test quality-based ordering
- [ ] `src/backend/tests/unit/engines/crewai/memory/test_memory_consolidator.py`
  - Test importance evaluation
  - Test memory merging logic
  - Test consolidation thresholds
- [ ] `src/backend/tests/unit/engines/crewai/memory/test_temporal_scorer.py`
  - Test decay functions
  - Test time-window filtering
  - Test freshness calculations
- [ ] `src/backend/tests/unit/engines/crewai/memory/test_advanced_databricks_memory.py`
  - Test integrated memory system
  - Test memory-type specific behaviors

### 5.2 Test Updates
- [ ] Update `test_databricks_vector_storage.py`
  - Add hybrid search tests
  - Test task hash generation
  - Test new metadata fields
  - Test memory-type specific storage
- [ ] Update `test_crewai_databricks_wrapper.py`
  - Test updated search methods
  - Test reranking integration
  - Test memory consolidation triggers
- [ ] Update `test_memory_backend_factory.py`
  - Test new configuration options
  - Test component integration

### 5.3 Integration Tests
- [ ] Create end-to-end memory lifecycle tests
- [ ] Test short-term to long-term consolidation
- [ ] Test entity relationship tracking
- [ ] Test temporal query scenarios
- [ ] Test memory retrieval accuracy

## 6. Configuration and Documentation

### 6.1 Configuration Updates
- [ ] Add memory consolidation settings to environment variables
- [ ] Create reranking configuration options
- [ ] Add temporal decay parameters
- [ ] Document hybrid search settings

### 6.2 Documentation
- [ ] Update CLAUDE.md with new memory features
- [ ] Create memory implementation guide
- [ ] Document configuration options
- [ ] Add troubleshooting section

## 7. Performance Optimizations

### 7.1 Caching
- [ ] Implement embedding cache for frequent queries
- [ ] Add result caching with TTL
- [ ] Create memory access patterns analysis

### 7.2 Batch Operations
- [ ] Implement batch embedding generation
- [ ] Add bulk memory consolidation
- [ ] Create batch search capabilities

### 7.3 Async Improvements
- [ ] Optimize async embedding generation
- [ ] Implement proper connection pooling
- [ ] Add concurrent search support

## 8. Implementation Order

### Phase 1: Core Infrastructure (Week 1)
1. Memory type differentiation
2. Hybrid search basic implementation
3. Basic reranking system

### Phase 2: Memory-Specific Features (Week 2)
1. Short-term memory enhancements
2. Long-term memory consolidation
3. Entity memory relationships

### Phase 3: Advanced Features (Week 3)
1. Temporal awareness
2. Advanced retrieval pipeline
3. Performance optimizations

### Phase 4: Testing and Documentation (Week 4)
1. Complete unit tests
2. Integration testing
3. Documentation updates

## 9. Success Criteria

- [ ] Each memory type has distinct search and storage behavior
- [ ] Long-term memory supports exact task matching with semantic fallback
- [ ] Short-term memory implements temporal decay
- [ ] Entity memory tracks relationships
- [ ] All unit tests pass with >80% coverage
- [ ] Performance benchmarks show <100ms retrieval time
- [ ] Memory consolidation works automatically
- [ ] Documentation is complete and clear

## 10. Notes and Considerations

- Maintain backward compatibility with existing CrewAI integration
- Ensure thread safety for concurrent operations
- Monitor Databricks API rate limits
- Consider memory storage costs for large-scale deployments
- Plan for memory migration from current implementation

---

Last Updated: [Current Date]
Status: Planning Phase