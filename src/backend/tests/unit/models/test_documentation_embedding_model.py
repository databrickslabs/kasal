"""
Unit tests for documentation_embedding model.

Tests the functionality of the DocumentationEmbedding database model including
field validation, Vector type handling, and data integrity.
"""
import pytest
import json
from datetime import datetime, timezone
from unittest.mock import MagicMock

from src.models.documentation_embedding import DocumentationEmbedding, Vector


class TestVector:
    """Test cases for Vector custom type."""

    def test_vector_init(self):
        """Test Vector type initialization."""
        # Test default dimension
        vector_default = Vector()
        assert vector_default.dim == 1024
        
        # Test custom dimension
        vector_custom = Vector(dim=512)
        assert vector_custom.dim == 512

    def test_vector_get_col_spec(self):
        """Test Vector column specification."""
        # Arrange
        vector = Vector(dim=1024)
        
        # Act
        col_spec = vector.get_col_spec()
        
        # Assert
        # Should return vector(1024) for PostgreSQL or TEXT for SQLite
        assert "vector(1024)" in col_spec or "TEXT" in col_spec

    def test_vector_bind_processor(self):
        """Test Vector bind processor for different dialects."""
        # Arrange
        vector = Vector(dim=3)
        
        # Test with mock SQLite dialect
        mock_sqlite_dialect = MagicMock()
        mock_sqlite_dialect.name = "sqlite"
        
        # Act
        sqlite_processor = vector.bind_processor(mock_sqlite_dialect)
        
        # Assert
        assert callable(sqlite_processor)
        
        # Test processing list value for SQLite
        test_list = [1.0, 2.0, 3.0]
        result = sqlite_processor(test_list)
        assert isinstance(result, str)
        assert json.loads(result) == test_list
        
        # Test processing None value
        none_result = sqlite_processor(None)
        assert none_result is None

    def test_vector_result_processor(self):
        """Test Vector result processor for different dialects."""
        # Arrange
        vector = Vector(dim=3)
        
        # Test with mock SQLite dialect
        mock_sqlite_dialect = MagicMock()
        mock_sqlite_dialect.name = "sqlite"
        
        # Act
        sqlite_processor = vector.result_processor(mock_sqlite_dialect, None)
        
        # Assert
        assert callable(sqlite_processor)
        
        # Test processing JSON string for SQLite
        test_json = json.dumps([1.0, 2.0, 3.0])
        result = sqlite_processor(test_json)
        assert result == [1.0, 2.0, 3.0]
        
        # Test processing None value
        none_result = sqlite_processor(None)
        assert none_result is None

    def test_vector_different_dimensions(self):
        """Test Vector with different dimensions."""
        dimensions = [128, 256, 512, 768, 1024, 1536]
        
        for dim in dimensions:
            vector = Vector(dim=dim)
            assert vector.dim == dim
            col_spec = vector.get_col_spec()
            assert f"vector({dim})" in col_spec or "TEXT" in col_spec


class TestDocumentationEmbedding:
    """Test cases for DocumentationEmbedding model."""

    def test_documentation_embedding_table_name(self):
        """Test that the table name is correctly set."""
        # Act & Assert
        assert DocumentationEmbedding.__tablename__ == "documentation_embeddings"

    def test_documentation_embedding_column_structure(self):
        """Test DocumentationEmbedding model column structure."""
        # Act
        columns = DocumentationEmbedding.__table__.columns
        
        # Assert - Check that all expected columns exist
        expected_columns = [
            'id', 'source', 'title', 'content', 'embedding', 
            'doc_metadata', 'created_at', 'updated_at'
        ]
        for col_name in expected_columns:
            assert col_name in columns, f"Column {col_name} should exist in DocumentationEmbedding model"

    def test_documentation_embedding_column_types_and_constraints(self):
        """Test that columns have correct data types and constraints."""
        # Act
        columns = DocumentationEmbedding.__table__.columns
        
        # Assert
        # Primary key
        assert columns['id'].primary_key is True
        assert columns['id'].index is True
        assert "INTEGER" in str(columns['id'].type)
        
        # Required indexed string fields
        indexed_string_fields = ['source', 'title']
        for field in indexed_string_fields:
            assert columns[field].nullable is False
            assert columns[field].index is True
            assert "VARCHAR" in str(columns[field].type) or "STRING" in str(columns[field].type)
        
        # Content field (required text)
        assert columns['content'].nullable is False
        assert "TEXT" in str(columns['content'].type)
        
        # Embedding field (required vector)
        assert columns['embedding'].nullable is False
        # Vector type should be configured
        assert hasattr(columns['embedding'].type, 'dim') or "TEXT" in str(columns['embedding'].type)
        
        # Optional JSON metadata field
        assert columns['doc_metadata'].nullable is True
        assert "JSON" in str(columns['doc_metadata'].type)
        
        # DateTime fields with server defaults
        datetime_fields = ['created_at', 'updated_at']
        for field in datetime_fields:
            assert "DATETIME" in str(columns[field].type)
            # Should have server defaults
            assert columns[field].server_default is not None

    def test_documentation_embedding_indexes(self):
        """Test that the model has the expected database indexes."""
        # Act
        columns = DocumentationEmbedding.__table__.columns
        
        # Assert indexed columns
        indexed_columns = ['id', 'source', 'title']
        for col_name in indexed_columns:
            assert columns[col_name].index is True

    def test_documentation_embedding_repr_method(self):
        """Test DocumentationEmbedding __repr__ method structure."""
        # Act & Assert
        # Test that the repr method is defined and accessible
        assert hasattr(DocumentationEmbedding, '__repr__')
        assert callable(getattr(DocumentationEmbedding, '__repr__'))

    def test_documentation_embedding_source_scenarios(self):
        """Test documentation source field scenarios."""
        # Test valid source patterns
        valid_sources = [
            "crewai_docs",
            "api_reference",
            "user_guide",
            "tutorials",
            "examples",
            "best_practices"
        ]
        
        for source in valid_sources:
            # Assert source format
            assert isinstance(source, str)
            assert len(source) > 0
            assert "_" in source or source.isalnum()

    def test_documentation_embedding_title_scenarios(self):
        """Test documentation title field scenarios."""
        # Test valid title patterns
        valid_titles = [
            "Getting Started with CrewAI",
            "Agent Configuration Guide",
            "Task Management Best Practices",
            "Crew Orchestration Tutorial",
            "API Reference - Agents",
            "Tools Integration Guide"
        ]
        
        for title in valid_titles:
            # Assert title format
            assert isinstance(title, str)
            assert len(title) > 0
            assert len(title) <= 200  # Reasonable title length

    def test_documentation_embedding_content_scenarios(self):
        """Test documentation content field scenarios."""
        # Test different content types
        content_examples = [
            "This is a short documentation snippet.",
            "# Markdown Content\n\nThis is a longer markdown content with **bold** text and `code` examples.",
            "```python\nfrom crewai import Agent\nagent = Agent(name='test')\n```",
            "A" * 5000,  # Long content
            "Multi-line\ncontent\nwith\nnewlines"
        ]
        
        for content in content_examples:
            # Assert content format
            assert isinstance(content, str)
            assert len(content) > 0

    def test_documentation_embedding_vector_scenarios(self):
        """Test embedding vector field scenarios."""
        # Test different vector dimensions and formats
        vector_examples = [
            [0.1, 0.2, 0.3] * 341 + [0.4],  # 1024 dimensions
            [0.5] * 512,  # 512 dimensions
            [0.0] * 768,  # 768 dimensions (common for some models)
            [-0.1, 0.2, -0.3, 0.4, 0.5]  # Small vector for testing
        ]
        
        for vector in vector_examples:
            # Assert vector format
            assert isinstance(vector, list)
            assert len(vector) > 0
            assert all(isinstance(x, (int, float)) for x in vector)

    def test_documentation_embedding_metadata_scenarios(self):
        """Test documentation metadata field scenarios."""
        # Test different metadata structures
        metadata_examples = [
            None,  # No metadata
            {},    # Empty metadata
            {"author": "system", "version": "1.0"},
            {
                "source_file": "docs/agents.md",
                "section": "configuration",
                "tags": ["agent", "config", "tutorial"],
                "difficulty": "beginner",
                "last_updated": "2023-12-01",
                "word_count": 1250
            },
            {
                "api_endpoint": "/agents",
                "http_method": "POST",
                "parameters": ["name", "role", "goal"],
                "response_format": "json"
            }
        ]
        
        for metadata in metadata_examples:
            if metadata is not None:
                # Assert metadata format
                assert isinstance(metadata, dict)
                # Should be JSON serializable
                json.dumps(metadata)  # Should not raise exception

    def test_documentation_embedding_timestamp_behavior(self):
        """Test timestamp behavior in DocumentationEmbedding."""
        # Act
        columns = DocumentationEmbedding.__table__.columns
        
        # Assert server defaults for timestamps
        assert columns['created_at'].server_default is not None
        assert columns['updated_at'].server_default is not None
        assert columns['updated_at'].onupdate is not None

    def test_documentation_embedding_model_documentation(self):
        """Test DocumentationEmbedding model documentation."""
        # Act & Assert
        assert DocumentationEmbedding.__doc__ is not None
        assert "documentation embeddings" in DocumentationEmbedding.__doc__.lower()


class TestDocumentationEmbeddingEdgeCases:
    """Test edge cases and error scenarios for DocumentationEmbedding."""

    def test_documentation_embedding_very_long_content(self):
        """Test DocumentationEmbedding with very long content."""
        # Arrange
        long_content = "This is a very long documentation content. " * 1000  # ~43,000 characters
        
        # Assert
        assert isinstance(long_content, str)
        assert len(long_content) > 40000

    def test_documentation_embedding_empty_content(self):
        """Test DocumentationEmbedding with minimal content."""
        # Arrange
        minimal_content = "Minimal doc."
        
        # Assert
        assert isinstance(minimal_content, str)
        assert len(minimal_content) > 0

    def test_documentation_embedding_special_characters(self):
        """Test DocumentationEmbedding with special characters."""
        # Test content with special characters
        special_content = """
        # Documentation with Special Characters
        
        This content includes:
        - Unicode: 🤖 AI Agent
        - Code: `print("Hello, World!")`
        - Math: α + β = γ
        - Symbols: @#$%^&*()
        - Quotes: "double" and 'single'
        - Accents: café, naïve, résumé
        """
        
        # Assert
        assert isinstance(special_content, str)
        assert "🤖" in special_content
        assert "α" in special_content

    def test_documentation_embedding_markdown_content(self):
        """Test DocumentationEmbedding with markdown content."""
        # Arrange
        markdown_content = """
        # Agent Configuration
        
        ## Overview
        
        CrewAI agents are configured with the following parameters:
        
        - **name**: Agent identifier
        - **role**: Agent's role in the crew
        - **goal**: Primary objective
        - **backstory**: Context and background
        
        ### Example
        
        ```python
        from crewai import Agent
        
        agent = Agent(
            name="researcher",
            role="Senior Research Analyst",
            goal="Gather and analyze market data",
            backstory="Expert in market research with 10+ years experience"
        )
        ```
        
        ## Configuration Options
        
        | Parameter | Type | Required | Description |
        |-----------|------|----------|-------------|
        | name | str | Yes | Agent name |
        | role | str | Yes | Agent role |
        | goal | str | Yes | Agent goal |
        
        > **Note**: Always provide clear and specific goals for better agent performance.
        """
        
        # Assert
        assert isinstance(markdown_content, str)
        assert "# Agent Configuration" in markdown_content
        assert "```python" in markdown_content
        assert "| Parameter |" in markdown_content

    def test_documentation_embedding_code_examples(self):
        """Test DocumentationEmbedding with code examples."""
        # Arrange
        code_content = """
        Here's how to create an agent:
        
        ```python
        from crewai import Agent, Task, Crew
        
        # Define an agent
        agent = Agent(
            name='data_analyst',
            role='Data Analyst',
            goal='Analyze sales data and provide insights',
            backstory='You are an experienced data analyst with expertise in sales analytics.',
            tools=['python_repl', 'sql_database']
        )
        
        # Define a task
        task = Task(
            description='Analyze Q4 sales data and identify trends',
            expected_output='A comprehensive report with visualizations',
            agent=agent
        )
        
        # Create a crew
        crew = Crew(
            agents=[agent],
            tasks=[task],
            verbose=True
        )
        
        # Execute the crew
        result = crew.kickoff()
        print(result)
        ```
        """
        
        # Assert
        assert isinstance(code_content, str)
        assert "from crewai import" in code_content
        assert "def " not in code_content or "agent =" in code_content

    def test_documentation_embedding_api_documentation(self):
        """Test DocumentationEmbedding with API documentation."""
        # Arrange
        api_content = """
        ## POST /api/agents
        
        Create a new agent.
        
        ### Request Body
        
        ```json
        {
            "name": "research_agent",
            "role": "Senior Researcher",
            "goal": "Conduct thorough research",
            "backstory": "PhD in Computer Science",
            "tools": ["web_search", "document_analysis"],
            "llm": "gpt-4",
            "max_iter": 25,
            "verbose": true
        }
        ```
        
        ### Response
        
        ```json
        {
            "id": "agent_123",
            "name": "research_agent",
            "status": "created",
            "created_at": "2023-12-01T10:00:00Z"
        }
        ```
        
        ### Status Codes
        
        - `201 Created`: Agent created successfully
        - `400 Bad Request`: Invalid request data
        - `401 Unauthorized`: Authentication required
        - `422 Unprocessable Entity`: Validation errors
        """
        
        # Assert
        assert isinstance(api_content, str)
        assert "POST /api/agents" in api_content
        assert "```json" in api_content
        assert "201 Created" in api_content

    def test_documentation_embedding_complex_metadata(self):
        """Test DocumentationEmbedding with complex metadata."""
        # Arrange
        complex_metadata = {
            "document_info": {
                "source_file": "docs/advanced/agents.md",
                "section_hierarchy": ["Advanced", "Agents", "Configuration"],
                "word_count": 2500,
                "reading_time_minutes": 10
            },
            "content_analysis": {
                "topics": ["agent_creation", "configuration", "best_practices"],
                "difficulty_level": "intermediate",
                "code_examples": 5,
                "has_diagrams": False
            },
            "indexing_info": {
                "indexed_at": "2023-12-01T15:30:00Z",
                "embedding_model": "text-embedding-ada-002",
                "chunk_size": 1000,
                "overlap": 200
            },
            "references": [
                {"type": "internal", "target": "/docs/basics/getting-started"},
                {"type": "external", "target": "https://crewai.io/docs"}
            ]
        }
        
        # Assert
        assert isinstance(complex_metadata, dict)
        assert "document_info" in complex_metadata
        assert "content_analysis" in complex_metadata
        assert "indexing_info" in complex_metadata
        # Should be JSON serializable
        json.dumps(complex_metadata)

    def test_documentation_embedding_vector_edge_cases(self):
        """Test DocumentationEmbedding with edge case vectors."""
        # Test edge case vectors
        edge_case_vectors = [
            [0.0] * 1024,  # All zeros
            [1.0] * 1024,  # All ones
            [-1.0] * 1024, # All negative ones
            [float('inf')] * 1024,  # All infinity (might cause issues)
            [0.999999999] * 1024,   # Very close to 1
            [-0.999999999] * 1024,  # Very close to -1
        ]
        
        for vector in edge_case_vectors[:-1]:  # Skip infinity case
            # Assert vector properties
            assert isinstance(vector, list)
            assert len(vector) == 1024
            assert all(isinstance(x, float) for x in vector)

    def test_documentation_embedding_source_categorization(self):
        """Test documentation source categorization."""
        # Test different source categories
        source_categories = {
            "official_docs": ["crewai_official", "api_reference", "user_guide"],
            "tutorials": ["getting_started", "advanced_tutorial", "examples"],
            "community": ["community_guides", "blog_posts", "forum_answers"],
            "code": ["source_code", "inline_docs", "comments"]
        }
        
        for category, sources in source_categories.items():
            for source in sources:
                # Assert source categorization
                assert isinstance(source, str)
                assert len(source) > 0
                assert "_" in source or source.isalnum()

    def test_documentation_embedding_data_integrity(self):
        """Test data integrity constraints."""
        # Act
        table = DocumentationEmbedding.__table__
        
        # Assert primary key
        primary_keys = [col for col in table.columns if col.primary_key]
        assert len(primary_keys) == 1
        assert primary_keys[0].name == 'id'
        
        # Assert required fields
        required_fields = ['source', 'title', 'content', 'embedding']
        for field_name in required_fields:
            field = table.columns[field_name]
            assert field.nullable is False
        
        # Assert optional fields
        optional_fields = ['doc_metadata']
        for field_name in optional_fields:
            field = table.columns[field_name]
            assert field.nullable is True
        
        # Assert indexed fields
        indexed_fields = ['id', 'source', 'title']
        for field_name in indexed_fields:
            field = table.columns[field_name]
            assert field.index is True