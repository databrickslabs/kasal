# Memory Backend Feature Documentation

## Table of Contents
- [Overview](#overview)
- [Architecture](#architecture)
- [Key Components](#key-components)
- [Supported Backends](#supported-backends)
- [API Reference](#api-reference)
- [Configuration Guide](#configuration-guide)
- [Usage Examples](#usage-examples)
- [Frontend Components](#frontend-components)
- [Security Considerations](#security-considerations)
- [Migration Guide](#migration-guide)
- [Troubleshooting](#troubleshooting)

## Overview

The Memory Backend feature provides a flexible, extensible system for managing AI agent memory storage in Kasal. It enables agents to persist and retrieve different types of memories (short-term, long-term, and entity-based) using various vector database backends, including [Databricks Mosaic AI Vector Search](https://docs.databricks.com/en/generative-ai/vector-search.html).

### Key Features

- **Multiple Backend Support**: Currently supports CrewAI's default (ChromaDB + SQLite) and [Databricks Mosaic AI Vector Search](https://docs.databricks.com/en/generative-ai/vector-search.html)
- **Memory Type Management**: Separate storage for short-term, long-term, and entity memories
- **Group Isolation**: Complete memory isolation between different user groups for security
- **One-Click Setup**: Automated setup for Databricks Mosaic AI Vector Search with endpoint and index creation
- **Flexible Configuration**: Per-group memory backend configurations with default fallback
- **Authentication Integration**: Comprehensive authentication hierarchy supporting OBO, OAuth, and API keys

### Use Cases

1. **Local Development**: Use the default ChromaDB + SQLite backend for rapid prototyping
2. **Enterprise Deployment**: Use [Databricks Mosaic AI Vector Search](https://www.databricks.com/product/machine-learning/vector-search) for scalable, governed memory storage
3. **Multi-Tenant Applications**: Leverage group isolation for secure, separated memory storage
4. **Document Processing**: Store and retrieve document embeddings for RAG applications

## Architecture

### System Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                          Frontend (React)                         │
│  ┌─────────────────┐  ┌──────────────────┐  ┌───────────────┐  │
│  │ MemoryBackend   │  │ Databricks       │  │    Zustand    │  │
│  │ Components      │  │ Setup UI         │  │    Store      │  │
│  └────────┬────────┘  └────────┬─────────┘  └───────┬───────┘  │
└───────────┼────────────────────┼─────────────────────┼──────────┘
            │                    │                     │
            ▼                    ▼                     ▼
┌─────────────────────────────────────────────────────────────────┐
│                      API Layer (FastAPI)                         │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │             Memory Backend Router                        │   │
│  │  /memory-backend/configs, /databricks/*, /stats/*      │   │
│  └────────────────────────┬────────────────────────────────┘   │
└───────────────────────────┼─────────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────────┐
│                    Service Layer (Facade)                        │
│  ┌───────────────────────────────────────────────────────────┐ │
│  │              MemoryBackendService (Facade)                 │ │
│  │  ┌─────────────┐ ┌─────────────┐ ┌───────────────────┐   │ │
│  │  │ Base CRUD   │ │ Connection  │ │ Index Management  │   │ │
│  │  │ Service     │ │ Service     │ │ Service           │   │ │
│  │  └─────────────┘ └─────────────┘ └───────────────────┘   │ │
│  │  ┌─────────────┐ ┌─────────────┐ ┌───────────────────┐   │ │
│  │  │ Config      │ │ Setup       │ │ Verification      │   │ │
│  │  │ Service     │ │ Service     │ │ Service           │   │ │
│  │  └─────────────┘ └─────────────┘ └───────────────────┘   │ │
│  └───────────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────────┐
│                    Repository Layer                              │
│  ┌───────────────────────────────────────────────────────────┐ │
│  │            MemoryBackendRepository                         │ │
│  │  - get_by_group_id()                                      │ │
│  │  - get_default_by_group_id()                             │ │
│  │  - set_default()                                          │ │
│  │  - delete_all_by_group_id()                              │ │
│  └───────────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────────┐
│                    Database (SQLAlchemy)                         │
│  ┌───────────────────────────────────────────────────────────┐ │
│  │                  memory_backends table                      │ │
│  │  - id (UUID)                                              │ │
│  │  - group_id (foreign key)                                 │ │
│  │  - backend_type (enum)                                    │ │
│  │  - databricks_config (JSON)                               │ │
│  │  - is_default (boolean)                                   │ │
│  └───────────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────────┘
```

### Memory Flow Diagram

```
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│   CrewAI Agent  │────▶│ Memory Backend  │────▶│ Vector Database │
│                 │     │   Abstraction   │     │                 │
└─────────────────┘     └────────┬────────┘     └─────────────────┘
                                │
                   ┌────────────┴────────────┐
                   │                         │
           ┌───────▼────────┐       ┌────────▼────────┐
           │ Default Backend │       │ Databricks      │
           │ (ChromaDB)      │       │ Vector Search   │
           └────────────────┘       └─────────────────┘
```

## Key Components

### Backend Services

#### 1. MemoryBackendService (Facade)
The main service that orchestrates all memory backend operations:

```python
class MemoryBackendService:
    """
    Facade service for managing memory backend configurations.
    Delegates to specialized services for different operations.
    """
    def __init__(self, uow: UnitOfWork):
        self._base_service = MemoryBackendBaseService(uow)
        self._config_service = MemoryConfigService(uow)
        self._connection_service = DatabricksConnectionService(uow)
        self._index_service = DatabricksIndexService()
        self._setup_service = DatabricksVectorSearchSetupService(uow)
        self._verification_service = DatabricksVectorSearchVerificationService()
```

#### 2. MemoryBackendBaseService
Handles CRUD operations for memory backend configurations:
- Create, read, update, delete configurations
- Manage default backend selection
- Handle group-specific configurations

#### 3. DatabricksConnectionService
Manages Databricks authentication and connections:
- Implements authentication fallback hierarchy
- Tests connections to Databricks endpoints
- Manages OAuth, OBO, and API key authentication

#### 4. DatabricksIndexService
Handles Databricks Mosaic AI Vector Search index operations:
- Create indexes with proper schemas
- Delete indexes and endpoints
- Query index information and statistics

#### 5. DatabricksVectorSearchSetupService
Provides one-click setup functionality:
- Creates endpoints and indexes automatically
- Configures proper schemas for each memory type
- Saves configuration to database

### Database Models

#### MemoryBackend Model
```python
class MemoryBackend(Base):
    """Memory backend configuration model."""
    __tablename__ = "memory_backends"
    
    id = Column(String, primary_key=True, default=generate_uuid)
    group_id = Column(String(100), index=True, nullable=False)
    name = Column(String(255), nullable=False)
    description = Column(String(1000), nullable=True)
    backend_type = Column(Enum(MemoryBackendTypeEnum), nullable=False)
    databricks_config = Column(JSON, nullable=True)
    enable_short_term = Column(Boolean, default=True)
    enable_long_term = Column(Boolean, default=True)
    enable_entity = Column(Boolean, default=True)
    is_default = Column(Boolean, default=False)
```

### Schemas

#### DatabricksMemoryConfig
```python
class DatabricksMemoryConfig(BaseModel):
    endpoint_name: str  # Direct Access endpoint for memory
    document_endpoint_name: Optional[str]  # Storage Optimized for documents
    short_term_index: str
    long_term_index: Optional[str]
    entity_index: Optional[str]
    document_index: Optional[str]
    workspace_url: Optional[str]
    embedding_dimension: int = 1024
```

## Supported Backends

### 1. Default Backend (ChromaDB + SQLite)
- **Use Case**: Local development and testing
- **Features**: 
  - Zero configuration required
  - Fast local performance
  - Automatic persistence in `/Library/Application Support/kasal_default_[crew_id]/`
- **Limitations**: Single-machine deployment

### 2. Databricks Mosaic AI Vector Search
- **Use Case**: Enterprise-scale deployments
- **Features**:
  - [Unity Catalog](https://docs.databricks.com/en/data-governance/unity-catalog/index.html) integration for governance
  - Scalable vector search with [HNSW algorithm](https://api-docs.databricks.com/python/vector-search/index.html)
  - [Direct Access endpoints](https://docs.databricks.com/en/generative-ai/create-query-vector-search.html#create-a-direct-vector-access-index) for CRUD operations
  - [Storage Optimized endpoints](https://www.databricks.com/blog/announcing-storage-optimized-endpoints-vector-search) for static data
- **Requirements**: 
  - Databricks workspace
  - [Vector Search endpoints](https://docs.databricks.com/en/generative-ai/vector-search.html#create-a-vector-search-endpoint)
  - Proper authentication (OAuth/PAT)

## API Reference

### Endpoints

#### Configuration Management
- `POST /memory-backend/configs` - Create new configuration
- `GET /memory-backend/configs` - List all configurations
- `GET /memory-backend/configs/{backend_id}` - Get specific configuration
- `PUT /memory-backend/configs/{backend_id}` - Update configuration
- `DELETE /memory-backend/configs/{backend_id}` - Delete configuration
- `POST /memory-backend/configs/{backend_id}/set-default` - Set as default

#### Databricks Operations
- `POST /memory-backend/databricks/test-connection` - Test connection
- `POST /memory-backend/databricks/indexes` - Get available indexes
- `POST /memory-backend/databricks/create-index` - Create new index
- `POST /memory-backend/databricks/one-click-setup` - Automated setup
- `GET /memory-backend/databricks/verify-resources` - Verify resources exist
- `POST /memory-backend/databricks/empty-index` - Clear index data

#### Memory Management
- `GET /memory-backend/stats/{crew_id}` - Get memory statistics
- `POST /memory-backend/clear/{crew_id}` - Clear crew memory

### Request/Response Examples

#### Create Configuration
```json
POST /memory-backend/configs
{
  "name": "Production Memory Backend",
  "description": "Databricks backend for production",
  "backend_type": "databricks",
  "databricks_config": {
    "endpoint_name": "kasal_memory_endpoint",
    "short_term_index": "ml.agents.short_term_memory",
    "long_term_index": "ml.agents.long_term_memory",
    "entity_index": "ml.agents.entity_memory",
    "workspace_url": "https://example.databricks.com",
    "embedding_dimension": 1024
  },
  "enable_short_term": true,
  "enable_long_term": true,
  "enable_entity": true
}
```

#### One-Click Setup Response
```json
{
  "success": true,
  "message": "Databricks Mosaic AI Vector Search setup completed successfully",
  "endpoints": {
    "memory": {
      "name": "kasal_memory_20250103_120000_abc1",
      "type": "Direct Access",
      "status": "created"
    },
    "document": {
      "name": "kasal_docs_20250103_120000_abc1",
      "type": "Direct Access",
      "status": "created"
    }
  },
  "indexes": {
    "short_term": {
      "name": "ml.agents.short_term_memory_20250103_120000_abc1",
      "status": "created"
    },
    "long_term": {
      "name": "ml.agents.long_term_memory_20250103_120000_abc1",
      "status": "created"
    },
    "entity": {
      "name": "ml.agents.entity_memory_20250103_120000_abc1",
      "status": "created"
    }
  },
  "backend_id": "550e8400-e29b-41d4-a716-446655440000"
}
```

## Configuration Guide

### Environment Variables

```bash
# Databricks Authentication
DATABRICKS_TOKEN=your-pat-token
DATABRICKS_CLIENT_ID=your-oauth-client-id
DATABRICKS_CLIENT_SECRET=your-oauth-client-secret

# Database Configuration
USE_NULLPOOL=true  # For handling event loop conflicts
```

### Memory Backend Selection

The system uses the following priority for selecting memory backends:

1. **Group-specific configuration**: Latest active configuration for the group
2. **Default configuration**: Backend marked as `is_default=true`
3. **Fallback**: CrewAI's default backend if no configuration exists

### Disabled Configuration

A special configuration with all memory types disabled:
```python
{
    "backend_type": "default",
    "enable_short_term": False,
    "enable_long_term": False,
    "enable_entity": False
}
```
This configuration disables Databricks memory and falls back to default storage.

## Usage Examples

### Python Backend Usage

#### Creating a Memory Backend Configuration
```python
from src.services.memory_backend_service import MemoryBackendService
from src.schemas.memory_backend import MemoryBackendCreate, DatabricksMemoryConfig

async def create_databricks_backend(group_id: str):
    async with UnitOfWork() as uow:
        service = MemoryBackendService(uow)
        
        config = MemoryBackendCreate(
            name="Production Memory",
            backend_type="databricks",
            databricks_config=DatabricksMemoryConfig(
                endpoint_name="vector_search_endpoint",
                short_term_index="ml.agents.short_term",
                embedding_dimension=1024
            )
        )
        
        backend = await service.create_memory_backend(group_id, config)
        return backend
```

#### Testing Databricks Connection
```python
async def test_connection(config: DatabricksMemoryConfig, user_token: str):
    async with UnitOfWork() as uow:
        service = MemoryBackendService(uow)
        result = await service.test_databricks_connection(config, user_token)
        
        if result["success"]:
            print(f"Connected! Endpoint status: {result['details']['endpoint_status']}")
        else:
            print(f"Failed: {result['message']}")
```

### Frontend Usage

#### Using the Memory Backend Store
```typescript
import { useMemoryBackendStore } from '@/store/memoryBackend';

function MemoryConfigComponent() {
  const {
    config,
    updateDatabricksConfig,
    testDatabricksConnection,
    saveConfig
  } = useMemoryBackendStore();

  const handleTest = async () => {
    const result = await testDatabricksConnection();
    if (result.success) {
      console.log('Connection successful!');
    }
  };

  const handleSave = async () => {
    const success = await saveConfig();
    if (success) {
      console.log('Configuration saved!');
    }
  };
}
```

## Frontend Components

### Component Hierarchy

```
MemoryBackend/
├── index.ts                      # Export barrel
├── MemoryBackendSelector.tsx     # Backend type selection
├── MemoryBackendConfig.tsx       # Configuration form
└── DatabricksOneClickSetup.tsx   # Automated setup wizard
```

### Key Components

#### MemoryBackendSelector
- Allows users to select between available backends
- Shows current configuration status
- Provides quick access to setup wizards

#### MemoryBackendConfig
- Detailed configuration form for selected backend
- Connection testing functionality
- Index selection and validation

#### DatabricksOneClickSetup
- Step-by-step wizard for Databricks setup
- Automatic endpoint and index creation
- Progress tracking and error handling

## Security Considerations

### Authentication Hierarchy

1. **On-Behalf-Of (OBO)**: Uses user's token for operations
2. **OAuth Client Credentials**: Service-to-service authentication
3. **API Keys**: Stored encrypted in database
4. **Environment Variables**: Fallback for local development

### Group Isolation

- Each group's memory is completely isolated
- Crew IDs include group_id in hash calculation
- No cross-group memory access possible

### Data Protection

- All API keys stored encrypted using AES-256
- Sensitive configuration masked in logs
- HTTPS required for all Databricks communication

## Migration Guide

### Migrating from Default to Databricks

1. **Backup existing memory** (if needed)
2. **Run one-click setup**:
   ```bash
   POST /memory-backend/databricks/one-click-setup
   {
     "workspace_url": "https://your-workspace.databricks.com",
     "catalog": "ml",
     "schema": "agents"
   }
   ```
3. **Verify resources**:
   ```bash
   GET /memory-backend/databricks/verify-resources?workspace_url=...
   ```
4. **Set as default backend**:
   ```bash
   POST /memory-backend/configs/{backend_id}/set-default
   ```

### Switching Between Backends

The system supports switching backends without data loss:
- Old backend remains accessible
- New backend starts fresh
- Can switch back if needed

## Troubleshooting

### Common Issues

#### 1. Authentication Failures
**Problem**: "Unable to authenticate with Databricks"
**Solution**: 
- Check authentication hierarchy
- Verify API keys in database
- Ensure OAuth credentials are valid

#### 2. Event Loop Conflicts
**Problem**: "attached to a different loop"
**Solution**:
- Set `USE_NULLPOOL=true` environment variable
- Restart the application

#### 3. Index Creation Failures
**Problem**: "Index already exists"
**Solution**:
- Use unique names for indexes
- Check existing indexes before creation
- Use the verify-resources endpoint

#### 4. Memory Not Persisting
**Problem**: Crew memory resets between runs
**Solution**:
- Ensure crew_id generation is deterministic
- Check that memory backend is enabled
- Verify index connectivity

### Debug Tools

#### Check Resource Status
```bash
curl -X GET "https://api.example.com/memory-backend/databricks/verify-resources?workspace_url=..."
```

#### View Memory Statistics
```bash
curl -X GET "https://api.example.com/memory-backend/stats/{crew_id}"
```

#### Test Connection
```bash
curl -X POST "https://api.example.com/memory-backend/databricks/test-connection" \
  -H "Content-Type: application/json" \
  -d '{"endpoint_name": "...", "workspace_url": "..."}'
```

### Logging

The system provides comprehensive logging:
- Authentication attempts and methods used
- Index operations and results  
- Configuration changes
- Error details with stack traces

Enable debug logging:
```python
logger = LoggerManager.get_instance().system
logger.setLevel(logging.DEBUG)
```

## Future Enhancements

### Planned Features

1. **Additional Backends**
   - Pinecone integration
   - Qdrant support
   - Custom backend plugin system

2. **Advanced Features**
   - Memory migration tools
   - Backup and restore functionality
   - Memory analytics dashboard
   - Cross-backend synchronization

3. **Performance Optimizations**
   - Connection pooling improvements
   - Batch operations for memory updates
   - Caching layer for frequent queries

4. **Developer Experience**
   - CLI tools for memory management
   - Memory debugging interface
   - Performance profiling tools

### Extension Points

The architecture is designed for extensibility:
- New backends implement base interfaces
- Service layer uses dependency injection
- Frontend components are modular
- API follows RESTful patterns

## Conclusion

The Memory Backend feature provides a robust, scalable solution for AI agent memory management in Kasal. With support for multiple backends, comprehensive security, and an intuitive setup process, it enables both rapid prototyping and enterprise-scale deployments. The modular architecture ensures easy extensibility for future enhancements while maintaining backward compatibility.