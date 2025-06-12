# Documentation Embeddings

Kasal uses embeddings to enhance AI crew generation by providing relevant CrewAI documentation context to the LLM.

## How It Works

1. **Documentation Seeding**: CrewAI documentation is automatically downloaded, chunked, and converted to embeddings
2. **Semantic Search**: When creating crews, user prompts are embedded and matched against documentation 
3. **Context Injection**: Relevant documentation is added to the LLM prompt for better crew generation

## Database Support

- **PostgreSQL**: Uses pgvector extension for optimized vector similarity search
- **SQLite**: Uses pure SQL with JSON functions for similarity calculations (development fallback)

## What Gets Embedded

Documentation from key CrewAI concepts:
- Agents - Autonomous entities that perform tasks
- Tasks - Individual units of work 
- Crews - Collections of agents working together
- Tools - External capabilities agents can use
- Processes - Execution patterns and workflows

## Configuration

Embeddings use the `databricks-gte-large-en` model and are automatically seeded on application startup.

The system retrieves the top 3 most relevant documentation chunks for each crew generation request.