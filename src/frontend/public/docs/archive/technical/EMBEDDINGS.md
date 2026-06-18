# Documentation Embeddings

Kasal uses embeddings to help AI agents understand CrewAI documentation better when creating workflows.

## Simple Explanation

Think of embeddings as a way to help the AI understand documentation by converting text into numbers that represent meaning. This helps the AI find relevant information quickly when you ask it to create agents or tasks.

## When Embeddings Are Used

Documentation embeddings are **ONLY** created in these scenarios:

1. **Local Development** - When using SQLite or localhost PostgreSQL
2. **With Databricks Vector Search** - When you have configured Databricks for memory storage

Embeddings are **SKIPPED** when:
- Running in production without Databricks configured
- Using remote PostgreSQL (non-localhost)
- No vector search backend is available

## How It Works

### 1. Setup Phase (Automatic)
- On first startup, Kasal downloads CrewAI documentation
- Documentation is split into small chunks for easier processing
- Each chunk is converted to embeddings (number arrays)
- Embeddings are stored based on your configuration:
  - **Local dev**: Stored in your database
  - **Databricks**: Stored in Vector Search index

### 2. Usage Phase (When You Create Crews)
- Your request is converted to embeddings
- System finds the 3 most similar documentation chunks
- Relevant docs are included when the AI creates your crew
- Result: Better, more accurate crew configurations

## Storage Backends

### Local Development (SQLite/Local PostgreSQL)
- Embeddings stored directly in database
- Simple setup, no external services needed
- Good for testing and development

### Production with Databricks
- Uses Databricks Vector Search for scalability
- Embeddings stored in vector indexes
- Better performance for large-scale deployments

### Production without Databricks
- Documentation embeddings are **disabled**
- System works without embeddings
- Crews still function but with less context

## What Gets Embedded

- **CrewAI Concepts**: How agents, tasks, crews work
- **Tool Documentation**: Available tools and their usage
- **Best Practices**: Patterns for effective crew design
- **Error Handling**: Common issues and solutions

## Configuration Notes

- **Model Used**: `databricks-gte-large-en` (1024 dimensions)
- **Chunk Size**: ~1000 characters per chunk
- **Results Retrieved**: Top 3 most relevant chunks
- **Automatic**: No manual setup required

## Troubleshooting

**"Could not check backend configuration" Warning**
- This means embeddings are disabled
- Check if you're in local dev or have Databricks configured
- The system will continue working without embeddings

**"Embedding service not available" Message**
- Mock embeddings will be used for testing
- Configure Databricks in settings for real embeddings
- Only affects quality of crew generation suggestions

**Embeddings Not Working in Production**
- This is expected without Databricks
- Configure Databricks Vector Search to enable
- Or use local development for testing with embeddings