# Databricks Volume Configuration

## Overview

The Databricks Volume Configuration feature allows automatic upload of task outputs to Databricks Volumes. This feature is configured **per-group**, ensuring complete isolation between different user groups/tenants.

## Per-Group Configuration

Each group has its own independent Databricks volume configuration stored in the `databricks_configs` table with the following fields:

- `volume_enabled`: Enable/disable volume uploads for all tasks in the group
- `volume_path`: Default volume path (e.g., `catalog.schema.volume`)
- `volume_file_format`: Default file format (`json`, `csv`, or `txt`)
- `volume_create_date_dirs`: Whether to create date-based directory structure

## How It Works

### Backend (Per-Group Isolation)

1. **Database Model**: The `DatabricksConfig` model includes a `group_id` field for multi-tenant isolation
2. **Repository Layer**: `DatabricksConfigRepository` filters all queries by `group_id`
3. **Service Layer**: `DatabricksService` is initialized with the user's `group_id` from the request context
4. **API Endpoints**: All endpoints receive `GroupContextDep` which provides the authenticated user's group context

### Frontend

1. **Configuration Page**: Users see and edit only their group's volume configuration
2. **API Calls**: The `DatabricksService` uses `apiClient` which includes authentication headers
3. **Automatic Context**: The backend determines the group from the authentication token

### Task Execution

When a task is executed:

1. **Global Check**: The system checks if volume uploads are enabled for the user's group
2. **Default Application**: If enabled globally and no task-specific callback is set, `DatabricksVolumeCallback` is applied
3. **Per-Task Override**: Tasks can override the global settings with their own `callback_config`
4. **Path Structure**: Files are organized as:
   ```
   /Volumes/{catalog}/{schema}/{volume}/{execution_name}/{YYYY/MM/DD}/{task_name}.{format}
   ```

## Configuration Hierarchy

1. **Group-Level Settings** (Global for the group)
   - Stored in `databricks_configs` table with `group_id`
   - Configured via Configuration → Volume Uploads page
   - Applies to all tasks in the group by default

2. **Task-Level Override** (Optional)
   - Set in individual task configuration
   - Overrides the group's default volume path
   - Uses same authentication context as the group

## Example Scenarios

### Scenario 1: Multiple Groups
- **Group A** configures: `users.group_a.outputs` as their volume path
- **Group B** configures: `users.group_b.outputs` as their volume path
- Each group's tasks automatically upload to their respective volumes
- Complete isolation between groups

### Scenario 2: Task Override
- **Group Default**: `main.default.task_outputs`
- **Specific Task**: Overrides with `main.analytics.ml_outputs`
- Other tasks continue using the group default

## Security & Isolation

- **Group Isolation**: Each group can only see and modify their own configuration
- **Authentication**: Group context is derived from the user's authentication token
- **No Cross-Group Access**: Repository layer ensures queries are filtered by `group_id`
- **Audit Trail**: `created_by_email` field tracks who created/modified the configuration

## API Endpoints

- `GET /databricks/config` - Returns the group's configuration
- `POST /databricks/config` - Updates the group's configuration

Both endpoints automatically use the authenticated user's group context for proper isolation.