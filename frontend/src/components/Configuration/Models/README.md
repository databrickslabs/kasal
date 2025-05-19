# Model Configuration System

This directory contains components for managing AI model configurations.

## Overview

The Model Configuration system allows users to enable or disable different AI models and view their properties. It replaces the static configuration in `models.ts` with a database-backed solution.

## Architecture

The system consists of:

1. **Frontend**:
   - `ModelConfiguration.tsx`: UI component for enabling/disabling models and viewing their properties
   - `ModelService.ts`: Service that communicates with the backend API

2. **Backend**:
   - `/api/models` endpoints: REST API for CRUD operations on model configurations
   - `ModelConfig` database table: Stores model configurations in SQLite
   - `populate_tools.py`: Script that populates default model configurations when initializing the database

## Usage

### In the UI

Users can access model configurations in the Configuration page under the "Models" tab. They can:

- View all available models and their properties
- Enable or disable models with toggles
- Search for specific models
- Save changes to the database

### In the Code

To use models in the application, replace direct imports of `models.ts` with `ModelService`:

```typescript
// Before:
import { models } from '../config/models';

// After:
import { ModelService } from '../../../api/ModelService';

// In an async function:
const modelService = ModelService.getInstance();
const models = await modelService.getActiveModels();
```

For cases where async/await can't be used, there's a synchronous fallback:

```typescript
const modelService = ModelService.getInstance();
const models = modelService.getActiveModelsSync(); // Note: This may not reflect the latest changes
```

## Adding New Models

New models can be added in two ways:

1. **Via the backend**: Add model configurations to the `default_models` list in `populate_tools.py`
2. **Via the API**: Use POST `/api/models` to add new model configurations

## Benefits

- **Persistence**: Model configurations are stored in the database
- **Dynamic Updates**: Changes take effect immediately without code deployments
- **User Control**: Administrators can enable/disable models without modifying code
- **Consistency**: All model configurations follow the same structure and validation 