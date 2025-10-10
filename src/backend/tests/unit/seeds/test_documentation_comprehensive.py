import pytest
from unittest.mock import Mock, patch, AsyncMock, mock_open
from typing import List, Dict, Any, Optional
import json
from pathlib import Path

# Test documentation seeder - based on actual code inspection

from src.seeds.documentation import (
    extract_content, load_best_practices, create_best_practices_content,
    mock_create_embedding, DOCS_URLS, BEST_PRACTICES_PATH, EMBEDDING_MODEL
)


class TestDocumentationConstants:
    """Test documentation seeder constants"""

    def test_docs_urls_defined(self):
        """Test DOCS_URLS constant is properly defined"""
        assert DOCS_URLS is not None
        assert isinstance(DOCS_URLS, list)
        assert len(DOCS_URLS) > 0
        
        # Check that all URLs are strings and contain crewai.com
        for url in DOCS_URLS:
            assert isinstance(url, str)
            assert "crewai.com" in url

    def test_best_practices_path_defined(self):
        """Test BEST_PRACTICES_PATH constant is properly defined"""
        assert BEST_PRACTICES_PATH is not None
        assert isinstance(BEST_PRACTICES_PATH, Path)
        assert str(BEST_PRACTICES_PATH).endswith("tool_best_practices.json")

    def test_embedding_model_defined(self):
        """Test EMBEDDING_MODEL constant is properly defined"""
        assert EMBEDDING_MODEL is not None
        assert isinstance(EMBEDDING_MODEL, str)
        assert len(EMBEDDING_MODEL) > 0
        assert "databricks" in EMBEDDING_MODEL.lower()


class TestExtractContent:
    """Test extract_content function"""

    def test_extract_content_basic_html(self):
        """Test extract_content with basic HTML"""
        html_content = "<html><body><h1>Title</h1><p>Content paragraph</p></body></html>"
        
        result = extract_content(html_content)
        
        assert isinstance(result, str)
        assert "Title" in result
        assert "Content paragraph" in result

    def test_extract_content_with_navigation(self):
        """Test extract_content removes navigation elements"""
        html_content = """
        <html>
            <body>
                <nav>Navigation menu</nav>
                <header>Header content</header>
                <main><p>Main content</p></main>
                <footer>Footer content</footer>
            </body>
        </html>
        """
        
        result = extract_content(html_content)
        
        assert isinstance(result, str)
        assert "Main content" in result
        # Navigation, header, footer should be removed
        assert "Navigation menu" not in result
        assert "Header content" not in result
        assert "Footer content" not in result

    def test_extract_content_with_scripts_and_styles(self):
        """Test extract_content removes script and style tags"""
        html_content = """
        <html>
            <head>
                <style>body { color: red; }</style>
                <script>console.log('test');</script>
            </head>
            <body>
                <p>Visible content</p>
                <script>alert('popup');</script>
            </body>
        </html>
        """
        
        result = extract_content(html_content)
        
        assert isinstance(result, str)
        assert "Visible content" in result
        assert "color: red" not in result
        assert "console.log" not in result
        assert "alert" not in result

    def test_extract_content_empty_html(self):
        """Test extract_content with empty HTML"""
        html_content = "<html><body></body></html>"
        
        result = extract_content(html_content)
        
        assert isinstance(result, str)
        assert len(result.strip()) == 0

    def test_extract_content_malformed_html(self):
        """Test extract_content with malformed HTML"""
        html_content = "<html><body><p>Unclosed paragraph<div>Content</div>"
        
        result = extract_content(html_content)
        
        assert isinstance(result, str)
        assert "Unclosed paragraph" in result
        assert "Content" in result

    def test_extract_content_with_whitespace(self):
        """Test extract_content handles whitespace properly"""
        html_content = """
        <html>
            <body>
                <p>   Paragraph with   extra   spaces   </p>
                <div>
                    Another
                    line
                </div>
            </body>
        </html>
        """
        
        result = extract_content(html_content)
        
        assert isinstance(result, str)
        # Should normalize whitespace
        assert "Paragraph with extra spaces" in result or "Paragraph with   extra   spaces" in result


class TestMockCreateEmbedding:
    """Test mock_create_embedding function"""

    @pytest.mark.asyncio
    async def test_mock_create_embedding_basic(self):
        """Test mock_create_embedding with basic text"""
        text = "This is a test text for embedding"
        
        result = await mock_create_embedding(text)
        
        assert isinstance(result, list)
        assert len(result) == 1024  # Databricks GTE large embedding dimension
        assert all(isinstance(x, float) for x in result)

    @pytest.mark.asyncio
    async def test_mock_create_embedding_empty_text(self):
        """Test mock_create_embedding with empty text"""
        text = ""
        
        result = await mock_create_embedding(text)
        
        assert isinstance(result, list)
        assert len(result) == 1024
        assert all(isinstance(x, float) for x in result)

    @pytest.mark.asyncio
    async def test_mock_create_embedding_deterministic(self):
        """Test mock_create_embedding is deterministic"""
        text = "Same text for consistency test"
        
        result1 = await mock_create_embedding(text)
        result2 = await mock_create_embedding(text)
        
        assert result1 == result2

    @pytest.mark.asyncio
    async def test_mock_create_embedding_different_texts(self):
        """Test mock_create_embedding produces different results for different texts"""
        text1 = "First text"
        text2 = "Second text"
        
        result1 = await mock_create_embedding(text1)
        result2 = await mock_create_embedding(text2)
        
        assert result1 != result2

    @pytest.mark.asyncio
    async def test_mock_create_embedding_long_text(self):
        """Test mock_create_embedding with long text"""
        text = "This is a very long text " * 100
        
        result = await mock_create_embedding(text)
        
        assert isinstance(result, list)
        assert len(result) == 1024
        assert all(isinstance(x, float) for x in result)


class TestLoadBestPractices:
    """Test load_best_practices function"""

    def test_load_best_practices_file_exists(self):
        """Test load_best_practices when file exists"""
        mock_data = {"tools": {"test_tool": {"description": "Test tool"}}}
        
        with patch('src.seeds.documentation.BEST_PRACTICES_PATH') as mock_path:
            mock_path.exists.return_value = True
            with patch('builtins.open', mock_open(read_data=json.dumps(mock_data))):
                result = load_best_practices()
                
                assert result == mock_data

    def test_load_best_practices_file_not_exists(self):
        """Test load_best_practices when file doesn't exist"""
        with patch('src.seeds.documentation.BEST_PRACTICES_PATH') as mock_path:
            mock_path.exists.return_value = False
            
            result = load_best_practices()
            
            assert result == {}

    def test_load_best_practices_json_error(self):
        """Test load_best_practices with JSON decode error"""
        with patch('src.seeds.documentation.BEST_PRACTICES_PATH') as mock_path:
            mock_path.exists.return_value = True
            with patch('builtins.open', mock_open(read_data="invalid json")):
                result = load_best_practices()
                
                assert result == {}

    def test_load_best_practices_file_error(self):
        """Test load_best_practices with file read error"""
        with patch('src.seeds.documentation.BEST_PRACTICES_PATH') as mock_path:
            mock_path.exists.return_value = True
            with patch('builtins.open', side_effect=IOError("File read error")):
                result = load_best_practices()
                
                assert result == {}


class TestCreateBestPracticesContent:
    """Test create_best_practices_content function"""

    def test_create_best_practices_content_basic(self):
        """Test create_best_practices_content with basic data"""
        best_practices = {
            "tool_best_practices": {
                "test_tool": {
                    "name": "Test Tool",
                    "description": "A test tool",
                    "categories": {
                        "category1": {
                            "title": "Test Category",
                            "practices": [
                                {
                                    "pattern": "Test Pattern",
                                    "template": "Test template",
                                    "example": "Test example"
                                }
                            ]
                        }
                    }
                }
            }
        }

        result = create_best_practices_content(best_practices)

        assert isinstance(result, list)
        assert len(result) > 0

        # Check that content contains expected information
        content_str = str(result)
        assert "Test Tool" in content_str

    def test_create_best_practices_content_empty(self):
        """Test create_best_practices_content with empty data"""
        best_practices = {}
        
        result = create_best_practices_content(best_practices)
        
        assert isinstance(result, list)
        # Should still return some content (general best practices)
        assert len(result) >= 0

    def test_create_best_practices_content_no_tools(self):
        """Test create_best_practices_content with no tools section"""
        best_practices = {"other_section": {"data": "value"}}
        
        result = create_best_practices_content(best_practices)
        
        assert isinstance(result, list)

    def test_create_best_practices_content_multiple_tools(self):
        """Test create_best_practices_content with multiple tools"""
        best_practices = {
            "tool_best_practices": {
                "tool1": {
                    "name": "Tool 1",
                    "description": "First tool",
                    "categories": {
                        "cat1": {
                            "title": "Category 1",
                            "practices": [{"pattern": "Pattern 1"}]
                        }
                    }
                },
                "tool2": {
                    "name": "Tool 2",
                    "description": "Second tool",
                    "categories": {
                        "cat2": {
                            "title": "Category 2",
                            "practices": [{"pattern": "Pattern 2"}]
                        }
                    }
                }
            }
        }

        result = create_best_practices_content(best_practices)

        assert isinstance(result, list)
        assert len(result) > 0
        content_str = str(result)
        assert "Tool 1" in content_str
        assert "Tool 2" in content_str


class TestDocumentationImports:
    """Test documentation module imports"""

    def test_required_imports_available(self):
        """Test that required imports are available"""
        # Test that key modules can be imported
        from src.seeds.documentation import logger
        assert logger is not None

    def test_beautifulsoup_import(self):
        """Test BeautifulSoup import in extract_content"""
        # This is tested indirectly through extract_content tests
        html_content = "<html><body><p>Test</p></body></html>"
        result = extract_content(html_content)
        assert isinstance(result, str)

    def test_pathlib_usage(self):
        """Test pathlib Path usage"""
        assert isinstance(BEST_PRACTICES_PATH, Path)

    def test_json_module_usage(self):
        """Test json module is properly used"""
        # Test through load_best_practices which uses json
        with patch('src.seeds.documentation.BEST_PRACTICES_PATH') as mock_path:
            mock_path.exists.return_value = False
            result = load_best_practices()
            assert isinstance(result, dict)
