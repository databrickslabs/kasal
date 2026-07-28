"""
Unit tests for documentation_embedding schemas.

Tests the functionality of Pydantic schemas for documentation embedding operations
including validation, serialization, and field constraints.
"""
import pytest
from datetime import datetime
from pydantic import ValidationError
from typing import Dict, List

from src.schemas.documentation_embedding import (
    DocumentationEmbeddingBase, DocumentationEmbeddingCreate, DocumentationEmbedding
)


class TestDocumentationEmbeddingBase:
    """Test cases for DocumentationEmbeddingBase schema."""
    
    def test_valid_documentation_embedding_base_minimal(self):
        """Test DocumentationEmbeddingBase with minimal required fields."""
        embedding_data = {
            "source": "user_manual.md",
            "title": "Getting Started Guide",
            "content": "This is a comprehensive guide to getting started with the application.",
            "embedding": [0.1, 0.2, 0.3, 0.4, 0.5]
        }
        embedding = DocumentationEmbeddingBase(**embedding_data)
        assert embedding.source == "user_manual.md"
        assert embedding.title == "Getting Started Guide"
        assert embedding.content == "This is a comprehensive guide to getting started with the application."
        assert embedding.embedding == [0.1, 0.2, 0.3, 0.4, 0.5]
        assert embedding.doc_metadata is None
    
    def test_valid_documentation_embedding_base_full(self):
        """Test DocumentationEmbeddingBase with all fields."""
        embedding_data = {
            "source": "api_reference.md",
            "title": "API Reference - Authentication",
            "content": "Authentication endpoints allow users to login and manage their sessions.",
            "embedding": [0.8, -0.2, 0.5, 0.1, -0.3, 0.7],
            "doc_metadata": {
                "section": "authentication",
                "tags": ["api", "auth", "security"],
                "word_count": 42,
                "last_updated": "2023-12-01"
            }
        }
        embedding = DocumentationEmbeddingBase(**embedding_data)
        assert embedding.source == "api_reference.md"
        assert embedding.title == "API Reference - Authentication"
        assert embedding.content.startswith("Authentication endpoints")
        assert len(embedding.embedding) == 6
        assert embedding.doc_metadata["section"] == "authentication"
        assert "api" in embedding.doc_metadata["tags"]
    
    def test_documentation_embedding_base_missing_required_fields(self):
        """Test DocumentationEmbeddingBase validation with missing required fields."""
        with pytest.raises(ValidationError) as exc_info:
            DocumentationEmbeddingBase(
                source="test.md",
                title="Test Title"
            )
        
        errors = exc_info.value.errors()
        missing_fields = [error["loc"][0] for error in errors if error["type"] == "missing"]
        assert "content" in missing_fields
        assert "embedding" in missing_fields
    
    def test_documentation_embedding_base_empty_values(self):
        """Test DocumentationEmbeddingBase with empty values."""
        embedding_data = {
            "source": "",
            "title": "",
            "content": "",
            "embedding": []
        }
        embedding = DocumentationEmbeddingBase(**embedding_data)
        assert embedding.source == ""
        assert embedding.title == ""
        assert embedding.content == ""
        assert embedding.embedding == []
    
    def test_documentation_embedding_base_various_embedding_dimensions(self):
        """Test DocumentationEmbeddingBase with various embedding dimensions."""
        embedding_scenarios = [
            {"name": "small", "embedding": [0.1, 0.2]},
            {"name": "medium", "embedding": [0.1] * 100},
            {"name": "large", "embedding": [0.5] * 1536},  # Common OpenAI embedding size
            {"name": "very_large", "embedding": [0.0] * 4096}  # Larger model embedding size
        ]
        
        for scenario in embedding_scenarios:
            embedding_data = {
                "source": f"test_{scenario['name']}.md",
                "title": f"Test {scenario['name'].title()} Embedding",
                "content": f"Content for {scenario['name']} embedding test",
                "embedding": scenario["embedding"]
            }
            embedding = DocumentationEmbeddingBase(**embedding_data)
            assert len(embedding.embedding) == len(scenario["embedding"])
            assert embedding.source == f"test_{scenario['name']}.md"
    
    def test_documentation_embedding_base_various_content_types(self):
        """Test DocumentationEmbeddingBase with various content types."""
        content_scenarios = [
            {
                "title": "Markdown Content",
                "content": "# Header\n\nThis is **bold** text with [links](http://example.com).",
                "source": "markdown.md"
            },
            {
                "title": "Code Documentation",
                "content": "```python\ndef hello_world():\n    print('Hello, World!')\n```",
                "source": "code_docs.md"
            },
            {
                "title": "API Documentation", 
                "content": "POST /api/v1/users\nCreate a new user account with email and password.",
                "source": "api_docs.md"
            },
            {
                "title": "Long Content",
                "content": "Lorem ipsum dolor sit amet. " * 100,
                "source": "long_content.md"
            }
        ]
        
        for scenario in content_scenarios:
            embedding_data = {
                "source": scenario["source"],
                "title": scenario["title"],
                "content": scenario["content"],
                "embedding": [0.1, 0.2, 0.3]
            }
            embedding = DocumentationEmbeddingBase(**embedding_data)
            assert embedding.title == scenario["title"]
            assert embedding.content == scenario["content"]
            assert embedding.source == scenario["source"]
    
    def test_documentation_embedding_base_metadata_scenarios(self):
        """Test DocumentationEmbeddingBase with various metadata scenarios."""
        metadata_scenarios = [
            {
                "name": "simple",
                "metadata": {"category": "tutorial"}
            },
            {
                "name": "complex",
                "metadata": {
                    "category": "reference",
                    "subcategory": "api",
                    "tags": ["authentication", "security", "oauth"],
                    "difficulty": "intermediate",
                    "estimated_reading_time": 5,
                    "prerequisites": ["basic_auth", "api_concepts"],
                    "related_docs": ["oauth_guide.md", "security_best_practices.md"]
                }
            },
            {
                "name": "nested",
                "metadata": {
                    "document_info": {
                        "version": "2.1",
                        "authors": ["john.doe", "jane.smith"],
                        "review_status": "approved"
                    },
                    "technical_details": {
                        "language": "en",
                        "format": "markdown",
                        "encoding": "utf-8"
                    }
                }
            }
        ]
        
        for scenario in metadata_scenarios:
            embedding_data = {
                "source": f"{scenario['name']}_doc.md",
                "title": f"{scenario['name'].title()} Documentation",
                "content": f"Content for {scenario['name']} metadata test",
                "embedding": [0.1, 0.2, 0.3],
                "doc_metadata": scenario["metadata"]
            }
            embedding = DocumentationEmbeddingBase(**embedding_data)
            assert embedding.doc_metadata == scenario["metadata"]
    
    def test_documentation_embedding_base_special_characters(self):
        """Test DocumentationEmbeddingBase with special characters."""
        embedding_data = {
            "source": "special_chars_doc.md",
            "title": "Documentation with Special Characters: áéíóú, 中文, русский, 🚀",
            "content": "Content with emojis 🎉, Unicode ∀x∈ℝ, and symbols @#$%^&*()",
            "embedding": [0.1, -0.5, 0.8, -0.2],
            "doc_metadata": {
                "languages": ["en", "es", "zh", "ru"],
                "symbols": ["emoji", "unicode", "math"]
            }
        }
        embedding = DocumentationEmbeddingBase(**embedding_data)
        assert "🚀" in embedding.title
        assert "🎉" in embedding.content
        assert "∀x∈ℝ" in embedding.content
        assert "unicode" in embedding.doc_metadata["symbols"]


class TestDocumentationEmbeddingCreate:
    """Test cases for DocumentationEmbeddingCreate schema."""
    
    def test_documentation_embedding_create_inheritance(self):
        """Test that DocumentationEmbeddingCreate inherits from DocumentationEmbeddingBase."""
        create_data = {
            "source": "new_doc.md",
            "title": "New Documentation",
            "content": "Content for new documentation",
            "embedding": [0.1, 0.2, 0.3, 0.4],
            "doc_metadata": {"type": "tutorial"}
        }
        create_embedding = DocumentationEmbeddingCreate(**create_data)
        
        # Should have all base class attributes
        assert hasattr(create_embedding, 'source')
        assert hasattr(create_embedding, 'title')
        assert hasattr(create_embedding, 'content')
        assert hasattr(create_embedding, 'embedding')
        assert hasattr(create_embedding, 'doc_metadata')
        
        # Should behave like base class
        assert create_embedding.source == "new_doc.md"
        assert create_embedding.title == "New Documentation"
        assert create_embedding.content == "Content for new documentation"
        assert create_embedding.embedding == [0.1, 0.2, 0.3, 0.4]
        assert create_embedding.doc_metadata == {"type": "tutorial"}
    
    def test_documentation_embedding_create_minimal(self):
        """Test DocumentationEmbeddingCreate with minimal data."""
        create_data = {
            "source": "minimal.md",
            "title": "Minimal Doc",
            "content": "Minimal content",
            "embedding": [0.0]
        }
        create_embedding = DocumentationEmbeddingCreate(**create_data)
        assert create_embedding.source == "minimal.md"
        assert create_embedding.doc_metadata is None
    
    def test_documentation_embedding_create_various_sources(self):
        """Test DocumentationEmbeddingCreate with various source types."""
        source_types = [
            "readme.md",
            "docs/api/authentication.md",
            "tutorials/getting-started.rst", 
            "reference/python-sdk.txt",
            "https://docs.example.com/guide",
            "internal://company-wiki/onboarding"
        ]
        
        for source in source_types:
            create_data = {
                "source": source,
                "title": f"Documentation from {source}",
                "content": f"Content extracted from {source}",
                "embedding": [0.1, 0.2]
            }
            create_embedding = DocumentationEmbeddingCreate(**create_data)
            assert create_embedding.source == source
    
    def test_documentation_embedding_create_large_embedding(self):
        """Test DocumentationEmbeddingCreate with large embedding vectors."""
        large_embedding = [0.1 * i for i in range(1000)]  # 1000-dimensional embedding
        
        create_data = {
            "source": "large_doc.md",
            "title": "Document with Large Embedding",
            "content": "This document has a large embedding vector",
            "embedding": large_embedding
        }
        create_embedding = DocumentationEmbeddingCreate(**create_data)
        assert len(create_embedding.embedding) == 1000
        assert create_embedding.embedding[999] == 0.1 * 999


class TestDocumentationEmbedding:
    """Test cases for DocumentationEmbedding schema."""
    
    def test_valid_documentation_embedding_minimal(self):
        """Test DocumentationEmbedding with all required fields."""
        now = datetime.now()
        embedding_data = {
            "id": 1,
            "source": "test_doc.md",
            "title": "Test Documentation",
            "content": "Test content for documentation",
            "embedding": [0.1, 0.2, 0.3],
            "created_at": now,
            "updated_at": now
        }
        embedding = DocumentationEmbedding(**embedding_data)
        assert embedding.id == 1
        assert embedding.source == "test_doc.md"
        assert embedding.title == "Test Documentation"
        assert embedding.content == "Test content for documentation"
        assert embedding.embedding == [0.1, 0.2, 0.3]
        assert embedding.created_at == now
        assert embedding.updated_at == now
        assert embedding.doc_metadata is None
    
    def test_valid_documentation_embedding_full(self):
        """Test DocumentationEmbedding with all fields including metadata."""
        now = datetime.now()
        embedding_data = {
            "id": 42,
            "source": "comprehensive_guide.md",
            "title": "Comprehensive User Guide",
            "content": "A detailed guide covering all aspects of the application usage.",
            "embedding": [0.5, -0.2, 0.8, 0.1, -0.3],
            "doc_metadata": {
                "section": "user_guides",
                "difficulty": "beginner",
                "tags": ["guide", "tutorial", "basics"],
                "word_count": 1500,
                "reading_time_minutes": 7
            },
            "created_at": now,
            "updated_at": now
        }
        embedding = DocumentationEmbedding(**embedding_data)
        assert embedding.id == 42
        assert embedding.source == "comprehensive_guide.md"
        assert embedding.title == "Comprehensive User Guide"
        assert len(embedding.embedding) == 5
        assert embedding.doc_metadata["difficulty"] == "beginner"
        assert embedding.doc_metadata["reading_time_minutes"] == 7
    
    def test_documentation_embedding_inheritance(self):
        """Test that DocumentationEmbedding inherits from DocumentationEmbeddingBase."""
        now = datetime.now()
        embedding_data = {
            "id": 3,
            "source": "inherit_test.md",
            "title": "Inheritance Test",
            "content": "Testing inheritance behavior",
            "embedding": [0.1, 0.2],
            "created_at": now,
            "updated_at": now
        }
        embedding = DocumentationEmbedding(**embedding_data)
        
        # Should have all base class attributes
        assert hasattr(embedding, 'source')
        assert hasattr(embedding, 'title')
        assert hasattr(embedding, 'content')
        assert hasattr(embedding, 'embedding')
        assert hasattr(embedding, 'doc_metadata')
        
        # Should have response-specific attributes
        assert hasattr(embedding, 'id')
        assert hasattr(embedding, 'created_at')
        assert hasattr(embedding, 'updated_at')
        
        # Should behave like base class
        assert embedding.source == "inherit_test.md"
        assert embedding.title == "Inheritance Test"
        assert embedding.doc_metadata is None  # Default from base
    
    def test_documentation_embedding_missing_required_fields(self):
        """Test DocumentationEmbedding validation with missing required fields."""
        with pytest.raises(ValidationError) as exc_info:
            DocumentationEmbedding(
                source="test.md",
                title="Test",
                content="Content",
                embedding=[0.1]
            )
        
        errors = exc_info.value.errors()
        missing_fields = [error["loc"][0] for error in errors if error["type"] == "missing"]
        required_fields = {"id", "created_at", "updated_at"}
        assert required_fields.intersection(set(missing_fields)) == required_fields
    
    def test_documentation_embedding_config(self):
        """Test DocumentationEmbedding Config class."""
        assert hasattr(DocumentationEmbedding, 'model_config')
        assert DocumentationEmbedding.model_config.get('from_attributes') is True
    
    def test_documentation_embedding_id_conversion(self):
        """Test DocumentationEmbedding with different ID types."""
        now = datetime.now()
        embedding_data = {
            "id": "123",  # String that can be converted to int
            "source": "id_test.md",
            "title": "ID Conversion Test",
            "content": "Testing ID conversion",
            "embedding": [0.1],
            "created_at": now,
            "updated_at": now
        }
        embedding = DocumentationEmbedding(**embedding_data)
        assert embedding.id == 123
        assert isinstance(embedding.id, int)
    
    def test_documentation_embedding_datetime_conversion(self):
        """Test DocumentationEmbedding with datetime string conversion."""
        embedding_data = {
            "id": 5,
            "source": "datetime_test.md",
            "title": "DateTime Conversion Test",
            "content": "Testing datetime conversion",
            "embedding": [0.1, 0.2],
            "created_at": "2023-01-01T12:00:00",
            "updated_at": "2023-01-01T12:30:00"
        }
        embedding = DocumentationEmbedding(**embedding_data)
        assert isinstance(embedding.created_at, datetime)
        assert isinstance(embedding.updated_at, datetime)
    
    def test_documentation_embedding_negative_embeddings(self):
        """Test DocumentationEmbedding with negative embedding values."""
        now = datetime.now()
        negative_embedding = [-0.5, -0.8, 0.2, -0.1, 0.9, -0.3]
        
        embedding_data = {
            "id": 6,
            "source": "negative_embedding.md",
            "title": "Document with Negative Embeddings",
            "content": "This document has negative values in its embedding",
            "embedding": negative_embedding,
            "created_at": now,
            "updated_at": now
        }
        embedding = DocumentationEmbedding(**embedding_data)
        assert embedding.embedding == negative_embedding
        assert min(embedding.embedding) < 0
        assert max(embedding.embedding) > 0


class TestSchemaIntegration:
    """Integration tests for documentation_embedding schema interactions."""
    
    def test_documentation_embedding_lifecycle(self):
        """Test complete documentation embedding lifecycle."""
        # Create embedding
        create_data = {
            "source": "lifecycle_test.md",
            "title": "Lifecycle Test Documentation",
            "content": "This document tests the complete lifecycle of documentation embeddings.",
            "embedding": [0.2, 0.4, 0.6, 0.8, 1.0],
            "doc_metadata": {
                "category": "test",
                "tags": ["lifecycle", "testing"],
                "word_count": 15
            }
        }
        create_embedding = DocumentationEmbeddingCreate(**create_data)
        
        # Embedding response (simulating what would come from database)
        now = datetime.now()
        response_data = {
            "id": 100,
            "source": create_embedding.source,
            "title": create_embedding.title,
            "content": create_embedding.content,
            "embedding": create_embedding.embedding,
            "doc_metadata": create_embedding.doc_metadata,
            "created_at": now,
            "updated_at": now
        }
        embedding_response = DocumentationEmbedding(**response_data)
        
        # Verify lifecycle
        assert create_embedding.source == "lifecycle_test.md"
        assert create_embedding.doc_metadata["category"] == "test"
        assert embedding_response.id == 100
        assert embedding_response.source == create_embedding.source
        assert embedding_response.title == create_embedding.title
        assert embedding_response.content == create_embedding.content
        assert embedding_response.embedding == create_embedding.embedding
        assert embedding_response.doc_metadata == create_embedding.doc_metadata
        assert embedding_response.created_at == now
        assert embedding_response.updated_at == now
    
    def test_documentation_search_scenarios(self):
        """Test documentation embedding scenarios for search functionality."""
        now = datetime.now()
        
        # Different types of documentation with embeddings
        docs = [
            {
                "id": 1,
                "source": "getting_started.md",
                "title": "Getting Started Guide",
                "content": "Learn how to get started with our platform",
                "embedding": [0.8, 0.2, 0.1, 0.9],  # High similarity to "getting started"
                "doc_metadata": {
                    "category": "tutorial",
                    "difficulty": "beginner",
                    "tags": ["intro", "basics"]
                }
            },
            {
                "id": 2,
                "source": "api_auth.md",
                "title": "API Authentication",
                "content": "How to authenticate with our REST API",
                "embedding": [0.1, 0.9, 0.8, 0.2],  # High similarity to "api authentication"
                "doc_metadata": {
                    "category": "reference",
                    "difficulty": "intermediate",
                    "tags": ["api", "auth", "security"]
                }
            },
            {
                "id": 3,
                "source": "troubleshooting.md",
                "title": "Troubleshooting Guide",
                "content": "Common issues and their solutions",
                "embedding": [0.3, 0.1, 0.9, 0.7],  # High similarity to "troubleshooting"
                "doc_metadata": {
                    "category": "support",
                    "difficulty": "intermediate",
                    "tags": ["problems", "solutions", "debug"]
                }
            }
        ]
        
        # Create embedding objects
        embeddings = []
        for doc_data in docs:
            doc_data["created_at"] = now
            doc_data["updated_at"] = now
            embedding = DocumentationEmbedding(**doc_data)
            embeddings.append(embedding)
        
        # Verify search-related properties
        categories = [emb.doc_metadata["category"] for emb in embeddings]
        assert "tutorial" in categories
        assert "reference" in categories
        assert "support" in categories
        
        # Group by difficulty
        by_difficulty = {}
        for emb in embeddings:
            difficulty = emb.doc_metadata["difficulty"]
            if difficulty not in by_difficulty:
                by_difficulty[difficulty] = []
            by_difficulty[difficulty].append(emb)
        
        assert len(by_difficulty["beginner"]) == 1
        assert len(by_difficulty["intermediate"]) == 2
        
        # Verify embedding dimensions are consistent
        embedding_dims = [len(emb.embedding) for emb in embeddings]
        assert all(dim == 4 for dim in embedding_dims)
    
    def test_documentation_versioning_scenarios(self):
        """Test documentation embedding versioning scenarios."""
        base_time = datetime(2023, 1, 1, 12, 0, 0)
        
        # Original document
        original = DocumentationEmbedding(
            id=1,
            source="user_guide.md",
            title="User Guide v1.0",
            content="Original user guide content",
            embedding=[0.5, 0.5, 0.5],
            doc_metadata={
                "version": "1.0",
                "status": "published"
            },
            created_at=base_time,
            updated_at=base_time
        )
        
        # Updated document (new embedding due to content changes)
        updated = DocumentationEmbedding(
            id=2,
            source="user_guide.md",
            title="User Guide v1.1",
            content="Updated user guide content with new features",
            embedding=[0.6, 0.4, 0.7],  # Different embedding due to content change
            doc_metadata={
                "version": "1.1",
                "status": "published",
                "previous_version": "1.0",
                "changes": ["added_features", "updated_examples"]
            },
            created_at=base_time,
            updated_at=datetime(2023, 2, 1, 12, 0, 0)
        )
        
        # Verify versioning
        assert original.doc_metadata["version"] == "1.0"
        assert updated.doc_metadata["version"] == "1.1"
        assert updated.doc_metadata["previous_version"] == "1.0"
        assert original.embedding != updated.embedding
        assert original.updated_at < updated.updated_at
        assert "added_features" in updated.doc_metadata["changes"]
    
    def test_documentation_multilingual_scenarios(self):
        """Test documentation embedding multilingual scenarios."""
        now = datetime.now()
        
        # Same content in different languages
        languages = [
            {
                "lang": "en",
                "title": "Getting Started",
                "content": "Welcome to our platform! This guide will help you get started.",
                "embedding": [0.8, 0.1, 0.2, 0.9]
            },
            {
                "lang": "es", 
                "title": "Comenzando",
                "content": "¡Bienvenido a nuestra plataforma! Esta guía te ayudará a empezar.",
                "embedding": [0.7, 0.2, 0.3, 0.8]  # Similar but different due to language
            },
            {
                "lang": "fr",
                "title": "Commencer",
                "content": "Bienvenue sur notre plateforme ! Ce guide vous aidera à commencer.",
                "embedding": [0.75, 0.15, 0.25, 0.85]  # Similar but different due to language
            }
        ]
        
        embeddings = []
        for i, lang_data in enumerate(languages):
            embedding_data = {
                "id": i + 1,
                "source": f"getting_started_{lang_data['lang']}.md",
                "title": lang_data["title"],
                "content": lang_data["content"],
                "embedding": lang_data["embedding"],
                "doc_metadata": {
                    "language": lang_data["lang"],
                    "content_type": "getting_started",
                    "is_translation": lang_data["lang"] != "en"
                },
                "created_at": now,
                "updated_at": now
            }
            embedding = DocumentationEmbedding(**embedding_data)
            embeddings.append(embedding)
        
        # Verify multilingual support
        languages_available = [emb.doc_metadata["language"] for emb in embeddings]
        assert "en" in languages_available
        assert "es" in languages_available
        assert "fr" in languages_available
        
        # Original language is not a translation
        en_doc = next(emb for emb in embeddings if emb.doc_metadata["language"] == "en")
        assert en_doc.doc_metadata["is_translation"] is False
        
        # Translations are marked as such
        translations = [emb for emb in embeddings if emb.doc_metadata["is_translation"]]
        assert len(translations) == 2
        
        # All docs have same content type
        content_types = [emb.doc_metadata["content_type"] for emb in embeddings]
        assert all(ct == "getting_started" for ct in content_types)
    
    def test_documentation_bulk_operations(self):
        """Test bulk operations with documentation embeddings."""
        now = datetime.now()
        
        # Create multiple embeddings for bulk processing
        bulk_create_data = []
        for i in range(10):
            create_data = {
                "source": f"bulk_doc_{i}.md",
                "title": f"Bulk Document {i}",
                "content": f"Content for bulk document number {i}",
                "embedding": [0.1 * j for j in range(5)],  # 5-dimensional embeddings
                "doc_metadata": {
                    "batch": "bulk_import_2023",
                    "sequence": i,
                    "category": "bulk_test"
                }
            }
            bulk_create_data.append(create_data)
        
        # Convert to create schemas
        create_embeddings = [DocumentationEmbeddingCreate(**data) for data in bulk_create_data]
        
        # Simulate bulk response
        response_embeddings = []
        for i, create_emb in enumerate(create_embeddings):
            response_data = {
                "id": i + 1,
                "source": create_emb.source,
                "title": create_emb.title, 
                "content": create_emb.content,
                "embedding": create_emb.embedding,
                "doc_metadata": create_emb.doc_metadata,
                "created_at": now,
                "updated_at": now
            }
            response_embedding = DocumentationEmbedding(**response_data)
            response_embeddings.append(response_embedding)
        
        # Verify bulk operations
        assert len(create_embeddings) == 10
        assert len(response_embeddings) == 10
        
        # Verify batch metadata
        batch_names = [emb.doc_metadata["batch"] for emb in response_embeddings]
        assert all(batch == "bulk_import_2023" for batch in batch_names)
        
        # Verify sequence order
        sequences = [emb.doc_metadata["sequence"] for emb in response_embeddings]
        assert sequences == list(range(10))
        
        # Verify all have same embedding dimension
        embedding_dims = [len(emb.embedding) for emb in response_embeddings]
        assert all(dim == 5 for dim in embedding_dims)