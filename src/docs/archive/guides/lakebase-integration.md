# Lakebase Integration Guide

## Overview

Kasal now supports **Databricks Lakebase** as a database backend when deployed in Databricks Apps. Lakebase is a fully-managed PostgreSQL OLTP engine that provides low-latency transactional operations with integrated authentication and high availability.

## Key Features

- **PostgreSQL Compatibility**: Works seamlessly with SQLAlchemy and existing PostgreSQL code
- **Integrated Authentication**: Token-based authentication with Databricks Apps
- **Automatic Migration**: Migrates existing SQLite/PostgreSQL data when creating instance
- **High Availability**: Support for multi-node configurations
- **Automatic Backups**: Configurable retention period (2-35 days)

## Configuration Process

### 1. Access Database Management

Navigate to **Configuration** → **Database Management** → **Lakebase Configuration** tab

### 2. Create Lakebase Instance

1. **Configure Instance Settings**:
   - **Instance Name**: Unique identifier (1-63 characters, letters and hyphens only)
   - **Capacity**: Choose compute units (CU_1, CU_2, or CU_4)
   - **Retention Days**: Backup retention period (2-35 days)
   - **Node Count**: Number of nodes for high availability (1-3)

2. **Click "Create Lakebase Instance"**:
   - Creates the Lakebase instance in Databricks
   - Waits for instance to be ready (typically 2-3 minutes)
   - **Automatically migrates existing data** from SQLite/PostgreSQL

3. **Enable Lakebase**:
   - Once created, toggle "Enable Lakebase" switch
   - Save configuration

## Architecture

### Clean Architecture Pattern Maintained

```
Frontend → API Router → Service Layer → Repository Layer → Lakebase
```

- **API Router** (`database_management_router.py`): Handles HTTP endpoints
- **Service Layer** (`lakebase_service.py`): Business logic and orchestration
- **Repository Layer** (`database_config_repository.py`): Data persistence
- **Session Factory** (`lakebase_session.py`): Connection management

### Data Migration Flow

1. **Instance Creation**: Creates Lakebase instance via Databricks SDK
2. **Schema Creation**: Uses SQLAlchemy metadata to create all tables
3. **Data Transfer**: Reads from existing database and writes to Lakebase
4. **Configuration Update**: Saves Lakebase settings and marks migration complete

## API Endpoints

### Configuration Management

```http
GET /api/database-management/lakebase/config
```
Returns current Lakebase configuration

```http
POST /api/database-management/lakebase/config
```
Save Lakebase configuration

### Instance Management

```http
POST /api/database-management/lakebase/create
```
Create new Lakebase instance and migrate data

```json
{
  "instance_name": "kasal-lakebase",
  "capacity": "CU_1",
  "retention_days": 14,
  "node_count": 1
}
```

```http
GET /api/database-management/lakebase/instance/{instance_name}
```
Get instance status and details

```http
POST /api/database-management/lakebase/test-connection
```
Test connection to Lakebase instance

## Connection Management

### Token-Based Authentication

Lakebase uses temporary tokens that are automatically refreshed:

```python
# Automatic token generation per request
cred = w.database.generate_database_credential(
    request_id=str(uuid.uuid4()),
    instance_names=[instance_name]
)

# Connection string with token
connection_url = (
    f"postgresql+asyncpg://{user}:{cred.token}@"
    f"{endpoint}:5432/databricks_postgres?sslmode=require"
)
```

### Session Lifecycle

1. **Token Generation**: Fresh token for each session
2. **Connection Pooling**: 5 connections, 30-minute recycle
3. **Automatic Retry**: Refreshes token on authentication errors
4. **Resource Cleanup**: Proper disposal of engines and sessions

## Environment Variables

When Lakebase is enabled, Kasal automatically uses these settings:

```bash
# Detected automatically when Lakebase is enabled
DATABASE_TYPE=lakebase
LAKEBASE_INSTANCE_NAME=kasal-lakebase

# Databricks authentication (required)
DATABRICKS_HOST=https://your-workspace.databricks.com
DATABRICKS_TOKEN=your-token  # Or use OAuth
```

## Status Indicators

The UI shows clear status for your Lakebase instance:

- 🔵 **NOT_CREATED**: Instance doesn't exist
- 🔄 **CREATING**: Instance being provisioned
- ✅ **READY**: Instance operational
- ⚠️ **STOPPED**: Instance stopped (needs restart)
- ❌ **ERROR**: Instance has issues

## Limitations and Considerations

### Region Availability
Lakebase is available in: us-east-1, us-west-2, eu-west-1, ap-southeast-1, ap-southeast-2, eu-central-1, us-east-2, ap-south-1

### Resource Limits
- Maximum 10 instances per workspace
- 1000 concurrent connections per instance
- 2TB logical size limit per instance

### Migration Notes
- Migration is one-way (existing → Lakebase)
- All tables and data are copied
- Foreign keys and constraints are preserved
- Migration happens automatically during instance creation

## Troubleshooting

### Instance Creation Fails

1. **Check permissions**: Ensure you have `databricks_superuser` role
2. **Verify region**: Lakebase must be available in your region
3. **Check quotas**: Maximum 10 instances per workspace

### Connection Issues

1. **Token expiration**: Sessions automatically refresh tokens
2. **Instance state**: Verify instance is in "READY" state
3. **Network**: Ensure proper network connectivity to Databricks

### Migration Problems

1. **Check logs**: Review backend logs for detailed errors
2. **Table compatibility**: Some data types may need adjustment
3. **Size limits**: Large databases may take time to migrate

## Best Practices

1. **Create instance during low-traffic periods** for smooth migration
2. **Test connection** after creation to verify setup
3. **Monitor first operations** after enabling Lakebase
4. **Keep backup** of original database before migration
5. **Use high availability** (node_count > 1) for production

## Security

- **Token rotation**: Automatic per-request token generation
- **SSL required**: All connections use SSL/TLS
- **Unity Catalog integration**: Inherits Databricks permissions
- **No hardcoded credentials**: Tokens generated dynamically

## Performance Benefits

- **Low latency**: Optimized for OLTP workloads
- **Connection pooling**: Efficient resource usage
- **Automatic scaling**: Based on selected capacity
- **Query optimization**: PostgreSQL query planner

## Future Enhancements

- [ ] Scheduled backups to Unity Catalog volumes
- [ ] Monitoring dashboard for instance metrics
- [ ] Automatic capacity scaling based on load
- [ ] Multi-region replication support
- [ ] Point-in-time recovery UI