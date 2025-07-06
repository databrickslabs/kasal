"""Unit tests for PerplexitySearchTool."""

import pytest
from unittest.mock import patch, MagicMock, AsyncMock
from src.engines.crewai.tools.custom.perplexity_tool import PerplexitySearchTool, PerplexitySearchInput


class TestPerplexitySearchTool:
    """Test cases for PerplexitySearchTool."""

    def test_initialization_default_values(self):
        """Test that the tool initializes with correct default values."""
        tool = PerplexitySearchTool()
        
        assert tool.name == "PerplexityTool"
        assert "performs web searches using Perplexity AI" in tool.description
        assert tool.args_schema == PerplexitySearchInput
        assert tool._model == "sonar"
        assert tool._temperature == 0.1
        assert tool._top_p == 0.9
        assert tool._max_tokens == 2000
        assert tool._frequency_penalty == 1

    def test_initialization_with_custom_values(self):
        """Test that the tool accepts custom configuration values."""
        custom_api_key = "test-api-key"
        custom_model = "sonar-pro"
        custom_temperature = 0.5
        custom_max_tokens = 3000
        
        tool = PerplexitySearchTool(
            api_key=custom_api_key,
            model=custom_model,
            temperature=custom_temperature,
            max_tokens=custom_max_tokens
        )
        
        assert tool._api_key == custom_api_key
        assert tool._model == custom_model
        assert tool._temperature == custom_temperature
        assert tool._max_tokens == custom_max_tokens

    @patch.dict('os.environ', {'PERPLEXITY_API_KEY': 'env-test-key'})
    def test_api_key_from_environment(self):
        """Test that the tool uses API key from environment when not provided."""
        tool = PerplexitySearchTool()
        assert tool._api_key == 'env-test-key'

    @patch.dict('os.environ', {}, clear=True)
    def test_api_key_fallback(self):
        """Test that the tool uses default API key when none provided."""
        tool = PerplexitySearchTool()
        assert tool._api_key == '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'

    @patch('requests.post')
    def test_run_successful_query(self, mock_post):
        """Test successful query execution without citations."""
        # Mock response
        mock_response = MagicMock()
        mock_response.ok = True
        mock_response.status_code = 200
        mock_response.json.return_value = {
            'choices': [{
                'message': {
                    'content': 'Paris is the capital of France.'
                }
            }]
        }
        mock_post.return_value = mock_response
        
        tool = PerplexitySearchTool(api_key="test-key")
        result = tool._run("What is the capital of France?")
        
        assert result == "Paris is the capital of France."
        
        # Verify API call
        mock_post.assert_called_once()
        call_args = mock_post.call_args
        
        # Check URL
        assert call_args[0][0] == "https://api.perplexity.ai/chat/completions"
        
        # Check payload
        payload = call_args[1]['json']
        assert payload['model'] == 'sonar'
        assert payload['messages'][1]['content'] == "What is the capital of France?"
        assert payload['max_tokens'] == 2000
        assert payload['temperature'] == 0.1

    @patch('requests.post')
    def test_run_with_custom_model(self, mock_post):
        """Test query execution with custom model."""
        # Mock response
        mock_response = MagicMock()
        mock_response.ok = True
        mock_response.status_code = 200
        mock_response.json.return_value = {
            'choices': [{
                'message': {
                    'content': 'Test response'
                }
            }]
        }
        mock_post.return_value = mock_response
        
        tool = PerplexitySearchTool(api_key="test-key", model="sonar-pro")
        result = tool._run("Test query")
        
        assert result == "Test response"
        
        # Verify the custom model was used
        call_args = mock_post.call_args
        payload = call_args[1]['json']
        assert payload['model'] == 'sonar-pro'

    @patch('requests.post')
    def test_run_api_error(self, mock_post):
        """Test handling of API errors."""
        # Mock error response
        mock_response = MagicMock()
        mock_response.ok = False
        mock_response.status_code = 400
        mock_response.text = '{"error": "Invalid model"}'
        mock_post.return_value = mock_response
        
        tool = PerplexitySearchTool(api_key="test-key")
        result = tool._run("Test query")
        
        assert "Error from Perplexity API: 400" in result
        assert '{"error": "Invalid model"}' in result

    @patch('requests.post')
    def test_run_network_error(self, mock_post):
        """Test handling of network errors."""
        mock_post.side_effect = Exception("Network error")
        
        tool = PerplexitySearchTool(api_key="test-key")
        result = tool._run("Test query")
        
        assert "Error executing Perplexity API request: Network error" in result

    @patch('requests.post')
    def test_run_with_optional_parameters(self, mock_post):
        """Test that optional parameters are included in the API call."""
        # Mock response
        mock_response = MagicMock()
        mock_response.ok = True
        mock_response.status_code = 200
        mock_response.json.return_value = {
            'choices': [{
                'message': {
                    'content': 'Test response'
                }
            }]
        }
        mock_post.return_value = mock_response
        
        tool = PerplexitySearchTool(
            api_key="test-key",
            search_domain_filter=["example.com"],
            return_images=True,
            return_related_questions=True,
            search_recency_filter="week"
        )
        result = tool._run("Test query")
        
        # Verify optional parameters were included
        call_args = mock_post.call_args
        payload = call_args[1]['json']
        assert payload['search_domain_filter'] == ["example.com"]
        assert payload['return_images'] is True
        assert payload['return_related_questions'] is True
        assert payload['search_recency_filter'] == "week"

    def test_input_schema(self):
        """Test that the input schema is correctly defined."""
        # Create an instance of the input schema
        input_data = PerplexitySearchInput(query="Test query")
        assert input_data.query == "Test query"
        
        # Test validation
        with pytest.raises(ValueError):
            PerplexitySearchInput()  # Missing required field

    @patch('requests.post')
    def test_run_with_citations(self, mock_post):
        """Test query execution with citations in response."""
        # Mock response with citations
        mock_response = MagicMock()
        mock_response.ok = True
        mock_response.status_code = 200
        mock_response.json.return_value = {
            'choices': [{
                'message': {
                    'content': 'Paris is the capital of France.'
                }
            }],
            'search_results': [
                {
                    'title': 'France - Wikipedia',
                    'url': 'https://wikipedia.org/wiki/France'
                },
                {
                    'title': 'Paris Tourism Guide',
                    'url': 'https://example.com/paris'
                }
            ]
        }
        mock_post.return_value = mock_response
        
        tool = PerplexitySearchTool(api_key="test-key")
        result = tool._run("What is the capital of France?")
        
        # Check that citations are included in the response
        assert "Paris is the capital of France." in result
        assert "**Sources:**" in result
        assert "[1] France - Wikipedia: https://wikipedia.org/wiki/France" in result
        assert "[2] Paris Tourism Guide: https://example.com/paris" in result

    @patch('requests.post')
    def test_run_with_old_citations_format(self, mock_post):
        """Test query execution with old citations format."""
        # Mock response with old citations format
        mock_response = MagicMock()
        mock_response.ok = True
        mock_response.status_code = 200
        mock_response.json.return_value = {
            'choices': [{
                'message': {
                    'content': 'Test answer'
                }
            }],
            'citations': ['https://example.com/1', 'https://example.com/2']
        }
        mock_post.return_value = mock_response
        
        tool = PerplexitySearchTool(api_key="test-key")
        result = tool._run("Test query")
        
        # Check that citations are included
        assert "Test answer" in result
        assert "**Sources:**" in result
        assert "[1] https://example.com/1" in result
        assert "[2] https://example.com/2" in result