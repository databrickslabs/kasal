"""
Unit tests for documentation seeder.

Tests the functionality of documentation embedding seeder including
content fetching, processing, and database operations.
"""
import pytest
from unittest.mock import patch, AsyncMock, MagicMock
import asyncio
from datetime import datetime

from src.seeds.documentation import (
    fetch_url, extract_content, mock_create_embedding, create_documentation_chunks,
    setup_pgvector_extension, clear_existing_documentation, check_existing_documentation,
    seed_documentation_embeddings, seed_async, seed_sync, seed, DOCS_URLS, EMBEDDING_MODEL
)


class TestFetchUrl:
    """Test cases for fetch_url function."""
    
    @patch('src.seeds.documentation.requests')
    def test_fetch_url_success(self, mock_requests):
        """Test successful URL fetching."""
        mock_response = MagicMock()
        mock_response.text = "<html><body>Test content</body></html>"
        mock_requests.get.return_value = mock_response
        
        result = asyncio.run(fetch_url("https://test.com"))
        
        assert result == "<html><body>Test content</body></html>"
        mock_requests.get.assert_called_once_with("https://test.com")
        mock_response.raise_for_status.assert_called_once()
    
    @patch('src.seeds.documentation.requests')
    def test_fetch_url_failure(self, mock_requests):
        """Test URL fetching failure."""
        mock_requests.get.side_effect = Exception("Network error")
        
        result = asyncio.run(fetch_url("https://invalid.com"))
        
        assert result == ""
        mock_requests.get.assert_called_once_with("https://invalid.com")
    
    @patch('src.seeds.documentation.requests')
    def test_fetch_url_http_error(self, mock_requests):
        """Test URL fetching with HTTP error."""
        mock_response = MagicMock()
        mock_response.raise_for_status.side_effect = Exception("404 Not Found")
        mock_requests.get.return_value = mock_response
        
        result = asyncio.run(fetch_url("https://notfound.com"))
        
        assert result == ""


class TestExtractContent:
    """Test cases for extract_content function."""
    
    def test_extract_content_with_main(self):
        """Test content extraction with main tag."""
        html_content = """
        <html>
            <head><title>Test</title></head>
            <body>
                <nav>Navigation</nav>
                <main>
                    <h1>Main Content</h1>
                    <p>This is the main content.</p>
                </main>
                <footer>Footer</footer>
            </body>
        </html>
        """
        
        result = extract_content(html_content)
        
        assert "Main Content" in result
        assert "This is the main content." in result
        assert "Navigation" not in result
        assert "Footer" not in result
    
    def test_extract_content_with_documentation_content(self):
        """Test content extraction with documentation-content class."""
        html_content = """
        <html>
            <body>
                <div class="sidebar">Sidebar</div>
                <div class="documentation-content">
                    <h2>Documentation</h2>
                    <p>Documentation content here.</p>
                </div>
            </body>
        </html>
        """
        
        result = extract_content(html_content)
        
        assert "Documentation" in result
        assert "Documentation content here." in result
        assert "Sidebar" not in result
    
    def test_extract_content_with_article(self):
        """Test content extraction with article tag."""
        html_content = """
        <html>
            <body>
                <header>Header</header>
                <article>
                    <h1>Article Title</h1>
                    <p>Article content.</p>
                </article>
            </body>
        </html>
        """
        
        result = extract_content(html_content)
        
        assert "Article Title" in result
        assert "Article content." in result
        assert "Header" not in result
    
    def test_extract_content_fallback(self):
        """Test content extraction fallback to all text."""
        html_content = """
        <html>
            <body>
                <div>Some content</div>
                <span>More content</span>
            </body>
        </html>
        """
        
        result = extract_content(html_content)
        
        assert "Some content" in result
        assert "More content" in result
    
    def test_extract_content_error_handling(self):
        """Test content extraction error handling."""
        # Invalid HTML that might cause parsing errors
        invalid_html = "Not valid HTML content"
        
        result = extract_content(invalid_html)
        
        # Should still return some content or empty string, not crash
        assert isinstance(result, str)


class TestMockCreateEmbedding:
    """Test cases for mock_create_embedding function."""
    
    @pytest.mark.asyncio
    async def test_mock_create_embedding_deterministic(self):
        """Test that mock embedding is deterministic for same input."""
        text = "Test content for embedding"
        
        embedding1 = await mock_create_embedding(text)
        embedding2 = await mock_create_embedding(text)
        
        assert embedding1 == embedding2
        assert len(embedding1) == 1024
        assert all(isinstance(val, float) for val in embedding1)
    
    @pytest.mark.asyncio
    async def test_mock_create_embedding_different_inputs(self):
        """Test that different inputs produce different embeddings."""
        text1 = "First test content"
        text2 = "Second test content"
        
        embedding1 = await mock_create_embedding(text1)
        embedding2 = await mock_create_embedding(text2)
        
        assert embedding1 != embedding2
        assert len(embedding1) == len(embedding2) == 1024
    
    @pytest.mark.asyncio
    async def test_mock_create_embedding_normalized(self):
        """Test that mock embedding is normalized to unit length."""
        text = "Normalization test content"
        
        embedding = await mock_create_embedding(text)
        
        # Calculate magnitude
        magnitude = sum(x**2 for x in embedding) ** 0.5
        
        # Should be approximately 1.0 (unit length)
        assert abs(magnitude - 1.0) < 1e-10


class TestCreateDocumentationChunks:
    """Test cases for create_documentation_chunks function."""
    
    @patch('src.seeds.documentation.fetch_url')
    @patch('langchain.text_splitter.RecursiveCharacterTextSplitter')
    def test_create_documentation_chunks_success(self, mock_splitter_class, mock_fetch):
        """Test successful documentation chunk creation."""
        # Mock fetch_url to return a coroutine that returns a string
        async def mock_fetch_url(url):
            return "<html><main>Test documentation content</main></html>"
        mock_fetch.side_effect = mock_fetch_url
        
        # Mock text splitter
        mock_splitter = MagicMock()
        mock_splitter.split_text.return_value = ["Chunk 1 content", "Chunk 2 content"]
        mock_splitter_class.return_value = mock_splitter
        
        url = "https://docs.crewai.com/concepts/tasks"
        
        result = asyncio.run(create_documentation_chunks(url))
        
        assert len(result) == 2
        assert result[0]["source"] == url
        assert result[0]["title"] == "CrewAI Tasks Documentation - Part 1"
        assert result[0]["content"] == "Chunk 1 content"
        assert result[0]["chunk_index"] == 0
        assert result[0]["total_chunks"] == 2
        
        assert result[1]["title"] == "CrewAI Tasks Documentation - Part 2"
        assert result[1]["chunk_index"] == 1
    
    @patch('src.seeds.documentation.fetch_url')
    def test_create_documentation_chunks_no_content(self, mock_fetch):
        """Test chunk creation with no content."""
        async def mock_fetch_url(url):
            return ""
        mock_fetch.side_effect = mock_fetch_url
        
        result = asyncio.run(create_documentation_chunks("https://empty.com"))
        
        assert result == []
    
    @patch('src.seeds.documentation.fetch_url')
    def test_create_documentation_chunks_fetch_failure(self, mock_fetch):
        """Test chunk creation when fetch fails."""
        async def mock_fetch_url(url):
            return ""
        mock_fetch.side_effect = mock_fetch_url
        
        result = asyncio.run(create_documentation_chunks("https://failed.com"))
        
        assert result == []


class TestSetupPgvectorExtension:
    """Test cases for setup_pgvector_extension function."""
    
    @pytest.mark.asyncio
    async def test_setup_pgvector_extension_exists(self):
        """Test pgvector extension setup when extension already exists."""
        mock_session = AsyncMock()
        mock_result = MagicMock()
        mock_result.scalar_one_or_none.return_value = 1  # Extension exists
        mock_session.execute.return_value = mock_result
        
        await setup_pgvector_extension(mock_session)
        
        # Should check for extension but not create it
        assert mock_session.execute.call_count == 1
        mock_session.commit.assert_not_called()
    
    @pytest.mark.asyncio
    async def test_setup_pgvector_extension_not_exists(self):
        """Test pgvector extension setup when extension doesn't exist."""
        mock_session = AsyncMock()
        mock_result = MagicMock()
        mock_result.scalar_one_or_none.return_value = None  # Extension doesn't exist
        mock_session.execute.return_value = mock_result
        
        await setup_pgvector_extension(mock_session)
        
        # Should check for extension and create it
        assert mock_session.execute.call_count == 2
        mock_session.commit.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_setup_pgvector_extension_error(self):
        """Test pgvector extension setup error handling."""
        mock_session = AsyncMock()
        mock_session.execute.side_effect = Exception("Database error")
        
        with pytest.raises(Exception, match="Database error"):
            await setup_pgvector_extension(mock_session)
        
        mock_session.rollback.assert_called_once()


class TestClearExistingDocumentation:
    """Test cases for clear_existing_documentation function."""
    
    @pytest.mark.asyncio
    async def test_clear_existing_documentation_success(self):
        """Test successful clearing of existing documentation."""
        mock_session = AsyncMock()
        
        await clear_existing_documentation(mock_session)
        
        mock_session.execute.assert_called_once()
        mock_session.commit.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_clear_existing_documentation_error(self):
        """Test error handling in clearing existing documentation."""
        mock_session = AsyncMock()
        mock_session.execute.side_effect = Exception("Delete error")
        
        with pytest.raises(Exception, match="Delete error"):
            await clear_existing_documentation(mock_session)
        
        mock_session.rollback.assert_called_once()


class TestCheckExistingDocumentation:
    """Test cases for check_existing_documentation function."""
    
    @pytest.mark.asyncio
    async def test_check_existing_documentation_exists(self):
        """Test checking existing documentation when records exist."""
        mock_session = AsyncMock()
        mock_result = MagicMock()
        mock_result.scalar_one_or_none.return_value = 5  # 5 existing records
        mock_session.execute.return_value = mock_result
        
        exists, count = await check_existing_documentation(mock_session)
        
        assert exists is True
        assert count == 5
    
    @pytest.mark.asyncio
    async def test_check_existing_documentation_not_exists(self):
        """Test checking existing documentation when no records exist."""
        mock_session = AsyncMock()
        mock_result = MagicMock()
        mock_result.scalar_one_or_none.return_value = 0  # No existing records
        mock_session.execute.return_value = mock_result
        
        exists, count = await check_existing_documentation(mock_session)
        
        assert exists is False
        assert count == 0
    
    @pytest.mark.asyncio
    async def test_check_existing_documentation_error(self):
        """Test error handling in checking existing documentation."""
        mock_session = AsyncMock()
        mock_session.execute.side_effect = Exception("Query error")
        
        exists, count = await check_existing_documentation(mock_session)
        
        assert exists is False
        assert count == 0


class TestSeedDocumentationEmbeddings:
    """Test cases for seed_documentation_embeddings function."""
    
    @pytest.mark.asyncio
    @patch('src.seeds.documentation.setup_pgvector_extension')
    @patch('src.seeds.documentation.clear_existing_documentation')
    @patch('src.seeds.documentation.DocumentationEmbeddingService')
    @patch('src.seeds.documentation.create_documentation_chunks')
    @patch('src.seeds.documentation.LLMManager')
    async def test_seed_documentation_embeddings_success(self, mock_llm_manager, mock_create_chunks, 
                                                       mock_service_class, mock_clear, mock_setup):
        """Test successful documentation embeddings seeding."""
        mock_session = AsyncMock()
        
        # Mock setup and clear as async functions
        mock_setup.return_value = None
        mock_clear.return_value = None
        
        # Mock service
        mock_service = AsyncMock()
        mock_service_class.return_value = mock_service
        
        # Mock chunks creation
        mock_chunks = [
            {
                "source": "https://test.com",
                "title": "Test Doc",
                "content": "Test content",
                "chunk_index": 0,
                "total_chunks": 1
            }
        ]
        # Make create_chunks async
        async def mock_create_chunks_func(url):
            return mock_chunks
        mock_create_chunks.side_effect = mock_create_chunks_func
        
        # Mock LLM embedding
        mock_embedding = [0.1] * 1024
        mock_llm_manager.get_embedding = AsyncMock(return_value=mock_embedding)
        
        await seed_documentation_embeddings(mock_session)
        
        # Verify calls
        mock_setup.assert_called_once_with(mock_session)
        mock_clear.assert_called_once_with(mock_session)
        mock_service.create_documentation_embedding.assert_called()
    
    @pytest.mark.asyncio
    @patch('src.seeds.documentation.setup_pgvector_extension')
    @patch('src.seeds.documentation.clear_existing_documentation')
    @patch('src.seeds.documentation.DocumentationEmbeddingService')
    @patch('src.seeds.documentation.create_documentation_chunks')
    @patch('src.seeds.documentation.LLMManager')
    @patch('src.seeds.documentation.mock_create_embedding')
    async def test_seed_documentation_embeddings_llm_fallback(self, mock_fallback, mock_llm_manager, 
                                                            mock_create_chunks, mock_service_class, 
                                                            mock_clear, mock_setup):
        """Test documentation embeddings seeding with LLM fallback to mock."""
        mock_session = AsyncMock()
        
        # Mock setup and clear
        mock_setup.return_value = None
        mock_clear.return_value = None
        
        # Mock service
        mock_service = AsyncMock()
        mock_service_class.return_value = mock_service
        
        # Mock chunks creation
        mock_chunks = [
            {
                "source": "https://test.com",
                "title": "Test Doc",
                "content": "Test content",
                "chunk_index": 0,
                "total_chunks": 1
            }
        ]
        mock_create_chunks.return_value = mock_chunks
        
        # Mock LLM embedding failure
        mock_llm_manager.get_embedding.side_effect = Exception("LLM error")
        
        # Mock fallback embedding
        mock_embedding = [0.1] * 1024
        mock_fallback.return_value = mock_embedding
        
        await seed_documentation_embeddings(mock_session)
        
        # Verify fallback was called
        mock_fallback.assert_called()
        mock_service.create_documentation_embedding.assert_called()


class TestSeedAsync:
    """Test cases for seed_async function."""
    
    @pytest.mark.asyncio
    @patch('src.seeds.documentation.async_session_factory')
    @patch('src.seeds.documentation.check_existing_documentation')
    @patch('src.seeds.documentation.seed_documentation_embeddings')
    async def test_seed_async_no_existing_data(self, mock_seed_embeddings, mock_check, mock_session_factory):
        """Test seed_async when no existing data."""
        # Mock session
        mock_session = AsyncMock()
        mock_session_factory.return_value.__aenter__.return_value = mock_session
        mock_session_factory.return_value.__aexit__.return_value = None
        
        # Mock check - no existing data
        mock_check.return_value = (False, 0)
        
        # Mock seeding
        mock_seed_embeddings.return_value = None
        
        result = await seed_async()
        
        assert result == ("success", 0)
        mock_seed_embeddings.assert_called_once()
    
    @pytest.mark.asyncio
    @patch('src.seeds.documentation.async_session_factory')
    @patch('src.seeds.documentation.check_existing_documentation')
    async def test_seed_async_existing_data(self, mock_check, mock_session_factory):
        """Test seed_async when existing data found."""
        # Mock session
        mock_session = AsyncMock()
        mock_session_factory.return_value.__aenter__.return_value = mock_session
        mock_session_factory.return_value.__aexit__.return_value = None
        
        # Mock check - existing data found
        mock_check.return_value = (True, 5)
        
        result = await seed_async()
        
        assert result == ("skipped", 5)
    
    @pytest.mark.asyncio
    @patch('src.seeds.documentation.async_session_factory')
    async def test_seed_async_error(self, mock_session_factory):
        """Test seed_async error handling."""
        # Mock session factory to raise error
        mock_session_factory.side_effect = Exception("Database connection error")
        
        result = await seed_async()
        
        assert result == ("error", 0)


class TestSeedSync:
    """Test cases for seed_sync function."""
    
    @patch('asyncio.get_event_loop')
    def test_seed_sync_success(self, mock_get_event_loop):
        """Test successful synchronous seeding."""
        # Mock asyncio event loop
        mock_loop = MagicMock()
        mock_loop.run_until_complete.return_value = ("success", 0)
        mock_get_event_loop.return_value = mock_loop
        
        result = seed_sync()
        
        assert result == ("success", 0)
        mock_loop.run_until_complete.assert_called_once()
    
    @patch('asyncio.set_event_loop')
    @patch('asyncio.new_event_loop')
    @patch('asyncio.get_event_loop')
    def test_seed_sync_new_loop(self, mock_get_event_loop, mock_new_event_loop, mock_set_event_loop):
        """Test synchronous seeding with new event loop."""
        # Mock RuntimeError for get_event_loop
        mock_get_event_loop.side_effect = RuntimeError("No event loop")
        
        # Mock new event loop
        mock_new_loop = MagicMock()
        mock_new_loop.run_until_complete.return_value = ("success", 0)
        mock_new_event_loop.return_value = mock_new_loop
        
        result = seed_sync()
        
        assert result == ("success", 0)
        mock_new_event_loop.assert_called_once()
        mock_set_event_loop.assert_called_once_with(mock_new_loop)


class TestSeedMain:
    """Test cases for main seed function."""
    
    @pytest.mark.asyncio
    @patch('src.seeds.documentation.seed_async')
    async def test_seed_success(self, mock_seed_async):
        """Test main seed function success."""
        mock_seed_async.return_value = ("success", 0)
        
        result = await seed()
        
        assert result is True
        mock_seed_async.assert_called_once()
    
    @pytest.mark.asyncio
    @patch('src.seeds.documentation.seed_async')
    async def test_seed_skipped(self, mock_seed_async):
        """Test main seed function when skipped."""
        mock_seed_async.return_value = ("skipped", 5)
        
        result = await seed()
        
        assert result is True
        mock_seed_async.assert_called_once()
    
    @pytest.mark.asyncio
    @patch('src.seeds.documentation.seed_async')
    async def test_seed_error(self, mock_seed_async):
        """Test main seed function error."""
        mock_seed_async.return_value = ("error", 0)
        
        result = await seed()
        
        assert result is False
        mock_seed_async.assert_called_once()


class TestConstants:
    """Test cases for module constants."""
    
    def test_docs_urls_defined(self):
        """Test that DOCS_URLS is properly defined."""
        assert isinstance(DOCS_URLS, list)
        assert len(DOCS_URLS) > 0
        
        # All URLs should be CrewAI documentation URLs
        for url in DOCS_URLS:
            assert isinstance(url, str)
            assert url.startswith("https://docs.crewai.com/")
    
    def test_embedding_model_defined(self):
        """Test that EMBEDDING_MODEL is properly defined."""
        assert isinstance(EMBEDDING_MODEL, str)
        assert EMBEDDING_MODEL == "databricks-gte-large-en"
    
    def test_docs_urls_coverage(self):
        """Test that DOCS_URLS covers expected documentation sections."""
        expected_sections = ["tasks", "agents", "crews", "tools", "processes"]
        
        for section in expected_sections:
            matching_urls = [url for url in DOCS_URLS if section in url]
            assert len(matching_urls) > 0, f"No URL found for section: {section}"


class TestIntegrationPatterns:
    """Test cases for integration patterns and usage."""
    
    @patch('src.seeds.documentation.requests')
    def test_complete_workflow_simulation(self, mock_requests):
        """Test simulation of complete documentation seeding workflow."""
        # Mock HTTP response
        mock_response = MagicMock()
        mock_response.text = """
        <html>
            <body>
                <main>
                    <h1>CrewAI Tasks</h1>
                    <p>Tasks are the individual units of work that agents perform.</p>
                    <p>They define what needs to be done and how success is measured.</p>
                </main>
            </body>
        </html>
        """
        mock_requests.get.return_value = mock_response
        
        # Test the extraction workflow
        html_content = asyncio.run(fetch_url("https://docs.crewai.com/concepts/tasks"))
        assert "CrewAI Tasks" in html_content
        
        content = extract_content(html_content)
        assert "Tasks are the individual units" in content
        assert "individual units of work" in content
    
    @pytest.mark.asyncio
    async def test_embedding_consistency(self):
        """Test embedding consistency for documentation content."""
        content1 = "CrewAI tasks are powerful units of work"
        content2 = "CrewAI tasks are powerful units of work"  # Same content
        content3 = "CrewAI agents are intelligent workers"    # Different content
        
        # Same content should produce same embedding
        embedding1 = await mock_create_embedding(content1)
        embedding2 = await mock_create_embedding(content2)
        embedding3 = await mock_create_embedding(content3)
        
        assert embedding1 == embedding2
        assert embedding1 != embedding3
        
        # All embeddings should be normalized
        for embedding in [embedding1, embedding2, embedding3]:
            magnitude = sum(x**2 for x in embedding) ** 0.5
            assert abs(magnitude - 1.0) < 1e-10
    
    def test_url_processing_patterns(self):
        """Test URL processing patterns used in documentation seeding."""
        test_urls = [
            "https://docs.crewai.com/concepts/tasks",
            "https://docs.crewai.com/concepts/agents",
            "https://docs.crewai.com/concepts/crews"
        ]
        
        for url in test_urls:
            # Test page name extraction
            page_name = url.split('/')[-1].capitalize()
            title = f"CrewAI {page_name} Documentation"
            
            assert page_name in ["Tasks", "Agents", "Crews"]
            assert "CrewAI" in title
            assert "Documentation" in title
    
    @pytest.mark.asyncio
    async def test_error_recovery_patterns(self):
        """Test error recovery patterns in documentation seeding."""
        # Test empty content handling
        empty_result = await mock_create_embedding("")
        assert len(empty_result) == 1024
        
        # Test special character content
        special_content = "Content with émojis 🚀 and spëciál çhars"
        special_result = await mock_create_embedding(special_content)
        assert len(special_result) == 1024
        
        # Test very long content
        long_content = "Very long content. " * 1000
        long_result = await mock_create_embedding(long_content)
        assert len(long_result) == 1024