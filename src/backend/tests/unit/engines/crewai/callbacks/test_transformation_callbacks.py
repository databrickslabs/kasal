"""
Unit tests for transformation callbacks.

Tests all transformation callback classes with comprehensive coverage.
"""
import pytest
import json
from unittest.mock import MagicMock, patch
from datetime import datetime
from typing import Any, Dict, List

from src.engines.crewai.callbacks.transformation_callbacks import (
    OutputFormatter,
    DataExtractor,
    OutputEnricher,
    OutputSummarizer
)


class TestOutputFormatter:
    """Test cases for OutputFormatter callback."""

    def test_init_default_values(self):
        """Test OutputFormatter initialization with default values."""
        formatter = OutputFormatter()
        assert formatter.format_type == "json"
        assert formatter.indent == 2
        assert formatter.max_length is None

    def test_init_custom_values(self):
        """Test OutputFormatter initialization with custom values."""
        formatter = OutputFormatter(
            format_type="text",
            indent=4,
            max_length=100,
            max_retries=5,
            task_key="test_task"
        )
        assert formatter.format_type == "text"
        assert formatter.indent == 4
        assert formatter.max_length == 100
        assert formatter.max_retries == 5
        assert formatter.task_key == "test_task"

    def test_execute_json_format_with_dict_method(self):
        """Test execute with JSON format for object with dict() method."""
        formatter = OutputFormatter(format_type="json", indent=2)
        
        # Create mock object with dict() method
        mock_obj = MagicMock()
        mock_obj.dict.return_value = {"key": "value", "number": 42}
        
        result = formatter.execute(mock_obj)
        expected = json.dumps({"key": "value", "number": 42}, indent=2)
        
        assert result == expected
        mock_obj.dict.assert_called_once()

    def test_execute_json_format_with_dict_attribute(self):
        """Test execute with JSON format for object with __dict__ attribute."""
        formatter = OutputFormatter(format_type="json", indent=2)
        
        # Create object with __dict__ but no dict() method
        class TestObj:
            def __init__(self):
                self.key = "value"
                self.number = 42
        
        obj = TestObj()
        result = formatter.execute(obj)
        expected = json.dumps({"key": "value", "number": 42}, indent=2)
        
        assert result == expected

    def test_execute_json_format_with_plain_object(self):
        """Test execute with JSON format for plain object."""
        formatter = OutputFormatter(format_type="json", indent=2)
        
        data = {"key": "value", "list": [1, 2, 3]}
        result = formatter.execute(data)
        expected = json.dumps(data, indent=2)
        
        assert result == expected

    def test_execute_non_json_format(self):
        """Test execute with non-JSON format."""
        formatter = OutputFormatter(format_type="text")
        
        data = {"key": "value"}
        result = formatter.execute(data)
        
        assert result == str(data)

    def test_execute_with_max_length_truncation(self):
        """Test execute with max_length causing truncation."""
        formatter = OutputFormatter(format_type="text", max_length=10)
        
        data = "This is a very long string that should be truncated"
        result = formatter.execute(data)
        
        assert result == "This is a ..."
        assert len(result) == 13  # 10 + len("...")

    def test_execute_with_max_length_no_truncation(self):
        """Test execute with max_length but no truncation needed."""
        formatter = OutputFormatter(format_type="text", max_length=100)
        
        data = "Short string"
        result = formatter.execute(data)
        
        assert result == data

    def test_execute_json_with_custom_indent(self):
        """Test execute with custom indentation."""
        formatter = OutputFormatter(format_type="json", indent=4)
        
        data = {"key": "value"}
        result = formatter.execute(data)
        expected = json.dumps(data, indent=4)
        
        assert result == expected


class TestDataExtractor:
    """Test cases for DataExtractor callback."""

    def test_init_default_values(self):
        """Test DataExtractor initialization with default values."""
        extractor = DataExtractor()
        assert extractor.fields == []
        assert extractor.patterns == {}

    def test_init_custom_values(self):
        """Test DataExtractor initialization with custom values."""
        fields = ["name", "age"]
        patterns = {"email": r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b"}
        
        extractor = DataExtractor(
            fields=fields,
            patterns=patterns,
            max_retries=5,
            task_key="test_task"
        )
        
        assert extractor.fields == fields
        assert extractor.patterns == patterns
        assert extractor.max_retries == 5
        assert extractor.task_key == "test_task"

    def test_execute_extract_fields_from_dict_method(self):
        """Test execute extracting fields from object with dict() method."""
        extractor = DataExtractor(fields=["name", "age", "missing"])
        
        mock_obj = MagicMock()
        mock_obj.dict.return_value = {"name": "John", "age": 30, "city": "NYC"}
        
        result = extractor.execute(mock_obj)
        
        assert result == {"name": "John", "age": 30}
        mock_obj.dict.assert_called_once()

    def test_execute_extract_fields_from_dict_attribute(self):
        """Test execute extracting fields from object with __dict__ attribute."""
        extractor = DataExtractor(fields=["name", "age"])
        
        class TestObj:
            def __init__(self):
                self.name = "Jane"
                self.age = 25
                self.city = "LA"
        
        obj = TestObj()
        result = extractor.execute(obj)
        
        assert result == {"name": "Jane", "age": 25}

    def test_execute_extract_fields_from_dict(self):
        """Test execute extracting fields from dictionary."""
        extractor = DataExtractor(fields=["name", "age"])
        
        data = {"name": "Bob", "age": 35, "city": "Chicago"}
        result = extractor.execute(data)
        
        assert result == {"name": "Bob", "age": 35}

    def test_execute_extract_fields_from_other_object(self):
        """Test execute extracting fields from non-dict object."""
        extractor = DataExtractor(fields=["content"])
        
        result = extractor.execute("Hello World")
        
        assert result == {"content": "Hello World"}

    def test_execute_extract_patterns_single_match(self):
        """Test execute extracting patterns with single match."""
        patterns = {"email": r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b"}
        extractor = DataExtractor(patterns=patterns)
        
        text = "Contact me at john@example.com for more info."
        result = extractor.execute(text)
        
        assert result == {"email": "john@example.com"}

    def test_execute_extract_patterns_multiple_matches(self):
        """Test execute extracting patterns with multiple matches."""
        patterns = {"numbers": r"\d+"}
        extractor = DataExtractor(patterns=patterns)
        
        text = "There are 5 cats and 3 dogs, total of 8 animals."
        result = extractor.execute(text)
        
        assert result == {"numbers": ["5", "3", "8"]}

    def test_execute_extract_patterns_no_matches(self):
        """Test execute extracting patterns with no matches."""
        patterns = {"email": r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b"}
        extractor = DataExtractor(patterns=patterns)
        
        text = "No email addresses in this text."
        result = extractor.execute(text)
        
        assert result == {}

    def test_execute_extract_fields_and_patterns(self):
        """Test execute extracting both fields and patterns."""
        extractor = DataExtractor(
            fields=["name"],
            patterns={"numbers": r"\d+"}
        )
        
        data = {"name": "Alice", "description": "Age 28, has 2 cats"}
        result = extractor.execute(data)
        
        assert result == {"name": "Alice", "numbers": ["28", "2"]}

    def test_execute_no_fields_no_patterns(self):
        """Test execute with no fields or patterns specified."""
        extractor = DataExtractor()
        
        result = extractor.execute("Some text")
        
        assert result == {}


class TestOutputEnricher:
    """Test cases for OutputEnricher callback."""

    def test_init_default_values(self):
        """Test OutputEnricher initialization with default values."""
        enricher = OutputEnricher()
        assert enricher.add_timestamp is True
        assert enricher.add_metadata is True
        assert enricher.custom_enrichments == {}

    def test_init_custom_values(self):
        """Test OutputEnricher initialization with custom values."""
        custom_enrichments = {"version": "1.0", "source": "test"}
        
        enricher = OutputEnricher(
            add_timestamp=False,
            add_metadata=False,
            custom_enrichments=custom_enrichments,
            max_retries=5,
            task_key="test_task"
        )
        
        assert enricher.add_timestamp is False
        assert enricher.add_metadata is False
        assert enricher.custom_enrichments == custom_enrichments
        assert enricher.max_retries == 5
        assert enricher.task_key == "test_task"

    @patch('src.engines.crewai.callbacks.transformation_callbacks.datetime')
    def test_execute_with_dict_method(self, mock_datetime):
        """Test execute with object that has dict() method."""
        mock_datetime.now.return_value.isoformat.return_value = "2023-01-01T12:00:00"
        
        enricher = OutputEnricher()
        enricher.metadata = {"callback": "test"}
        
        mock_obj = MagicMock()
        mock_obj.dict.return_value = {"key": "value"}
        
        result = enricher.execute(mock_obj)
        
        expected = {
            "key": "value",
            "timestamp": "2023-01-01T12:00:00",
            "metadata": {"callback": "test"}
        }
        
        assert result == expected
        mock_obj.dict.assert_called_once()

    @patch('src.engines.crewai.callbacks.transformation_callbacks.datetime')
    def test_execute_with_dict_attribute(self, mock_datetime):
        """Test execute with object that has __dict__ attribute."""
        mock_datetime.now.return_value.isoformat.return_value = "2023-01-01T12:00:00"
        
        enricher = OutputEnricher()
        enricher.metadata = {"callback": "test"}
        
        class TestObj:
            def __init__(self):
                self.key = "value"
        
        obj = TestObj()
        result = enricher.execute(obj)
        
        expected = {
            "key": "value",
            "timestamp": "2023-01-01T12:00:00",
            "metadata": {"callback": "test"}
        }
        
        assert result == expected

    @patch('src.engines.crewai.callbacks.transformation_callbacks.datetime')
    def test_execute_with_dict_object(self, mock_datetime):
        """Test execute with dictionary object."""
        mock_datetime.now.return_value.isoformat.return_value = "2023-01-01T12:00:00"
        
        enricher = OutputEnricher()
        enricher.metadata = {"callback": "test"}
        
        data = {"key": "value", "number": 42}
        result = enricher.execute(data)
        
        expected = {
            "key": "value",
            "number": 42,
            "timestamp": "2023-01-01T12:00:00",
            "metadata": {"callback": "test"}
        }
        
        assert result == expected
        # Original data should not be modified
        assert data == {"key": "value", "number": 42}

    @patch('src.engines.crewai.callbacks.transformation_callbacks.datetime')
    def test_execute_with_other_object(self, mock_datetime):
        """Test execute with other object types."""
        mock_datetime.now.return_value.isoformat.return_value = "2023-01-01T12:00:00"
        
        enricher = OutputEnricher()
        enricher.metadata = {"callback": "test"}
        
        result = enricher.execute("Hello World")
        
        expected = {
            "content": "Hello World",
            "timestamp": "2023-01-01T12:00:00",
            "metadata": {"callback": "test"}
        }
        
        assert result == expected

    def test_execute_without_timestamp(self):
        """Test execute without adding timestamp."""
        enricher = OutputEnricher(add_timestamp=False)
        enricher.metadata = {"callback": "test"}
        
        data = {"key": "value"}
        result = enricher.execute(data)
        
        expected = {
            "key": "value",
            "metadata": {"callback": "test"}
        }
        
        assert result == expected
        assert "timestamp" not in result

    def test_execute_without_metadata(self):
        """Test execute without adding metadata."""
        enricher = OutputEnricher(add_metadata=False)
        
        data = {"key": "value"}
        result = enricher.execute(data)
        
        assert "metadata" not in result
        assert "timestamp" in result  # timestamp should still be added

    @patch('src.engines.crewai.callbacks.transformation_callbacks.datetime')
    def test_execute_with_custom_enrichments(self, mock_datetime):
        """Test execute with custom enrichments."""
        mock_datetime.now.return_value.isoformat.return_value = "2023-01-01T12:00:00"
        
        custom_enrichments = {"version": "1.0", "source": "test"}
        enricher = OutputEnricher(custom_enrichments=custom_enrichments)
        enricher.metadata = {"callback": "test"}
        
        data = {"key": "value"}
        result = enricher.execute(data)
        
        expected = {
            "key": "value",
            "timestamp": "2023-01-01T12:00:00",
            "metadata": {"callback": "test"},
            "version": "1.0",
            "source": "test"
        }
        
        assert result == expected

    def test_execute_all_disabled(self):
        """Test execute with all enrichments disabled."""
        enricher = OutputEnricher(
            add_timestamp=False,
            add_metadata=False,
            custom_enrichments={}
        )
        
        data = {"key": "value"}
        result = enricher.execute(data)
        
        assert result == {"key": "value"}


class TestOutputSummarizer:
    """Test cases for OutputSummarizer callback."""

    def test_init_default_values(self):
        """Test OutputSummarizer initialization with default values."""
        summarizer = OutputSummarizer()
        assert summarizer.max_length == 200
        assert summarizer.include_stats is True

    def test_init_custom_values(self):
        """Test OutputSummarizer initialization with custom values."""
        summarizer = OutputSummarizer(
            max_length=100,
            include_stats=False,
            max_retries=5,
            task_key="test_task"
        )
        
        assert summarizer.max_length == 100
        assert summarizer.include_stats is False
        assert summarizer.max_retries == 5
        assert summarizer.task_key == "test_task"

    def test_execute_short_content_with_stats(self):
        """Test execute with short content that doesn't need truncation."""
        summarizer = OutputSummarizer(max_length=200, include_stats=True)
        
        content = "Hello world! This is a test. How are you?"
        result = summarizer.execute(content)
        
        expected = {
            'summary': content,
            'total_length': len(content),
            'word_count': 9,  # "Hello world! This is a test. How are you?" has 9 words
            'has_numbers': False,
            'sentence_count': 4  # re.split(r'[.!?]+', content) splits on punctuation
        }
        
        assert result == expected

    def test_execute_long_content_with_truncation(self):
        """Test execute with long content that needs truncation."""
        summarizer = OutputSummarizer(max_length=20, include_stats=True)
        
        content = "This is a very long string that will definitely be truncated because it exceeds the maximum length."
        result = summarizer.execute(content)
        
        expected_summary = content[:20] + "..."
        
        assert result['summary'] == expected_summary
        assert result['total_length'] == len(content)
        assert result['word_count'] == 17
        assert result['has_numbers'] == False
        assert result['sentence_count'] == 2  # re.split(r'[.!?]+', content) splits on the period

    def test_execute_without_stats(self):
        """Test execute without including statistics."""
        summarizer = OutputSummarizer(max_length=200, include_stats=False)
        
        content = "Hello world! This is a test."
        result = summarizer.execute(content)
        
        expected = {'summary': content}
        
        assert result == expected
        assert 'total_length' not in result
        assert 'word_count' not in result
        assert 'has_numbers' not in result
        assert 'sentence_count' not in result

    def test_execute_content_with_numbers(self):
        """Test execute with content containing numbers."""
        summarizer = OutputSummarizer(max_length=200, include_stats=True)
        
        content = "There are 5 cats and 3 dogs."
        result = summarizer.execute(content)
        
        assert result['has_numbers'] is True
        assert result['word_count'] == 7  # "There are 5 cats and 3 dogs." has 7 words
        assert result['total_length'] == len(content)

    def test_execute_content_without_numbers(self):
        """Test execute with content not containing numbers."""
        summarizer = OutputSummarizer(max_length=200, include_stats=True)
        
        content = "There are many cats and dogs."
        result = summarizer.execute(content)
        
        assert result['has_numbers'] is False

    def test_execute_multiple_sentences(self):
        """Test execute with multiple sentences."""
        summarizer = OutputSummarizer(max_length=200, include_stats=True)
        
        content = "First sentence. Second sentence! Third sentence? Fourth."
        result = summarizer.execute(content)
        
        assert result['sentence_count'] == 5  # re.split includes empty string at end

    def test_execute_single_sentence(self):
        """Test execute with single sentence."""
        summarizer = OutputSummarizer(max_length=200, include_stats=True)
        
        content = "Just one sentence"
        result = summarizer.execute(content)
        
        assert result['sentence_count'] == 1

    def test_execute_empty_content(self):
        """Test execute with empty content."""
        summarizer = OutputSummarizer(max_length=200, include_stats=True)
        
        content = ""
        result = summarizer.execute(content)
        
        expected = {
            'summary': "",
            'total_length': 0,
            'word_count': 0,  # split("") returns [] after filtering empty strings
            'has_numbers': False,
            'sentence_count': 1  # re.split returns at least one element
        }
        
        assert result == expected

    def test_execute_non_string_input(self):
        """Test execute with non-string input."""
        summarizer = OutputSummarizer(max_length=200, include_stats=True)
        
        data = {"key": "value", "number": 42}
        result = summarizer.execute(data)
        
        content_str = str(data)
        
        assert result['summary'] == content_str
        assert result['total_length'] == len(content_str)
        assert result['has_numbers'] is True  # "42" in string representation
        assert result['word_count'] > 0

    def test_execute_exact_max_length(self):
        """Test execute with content exactly at max_length."""
        summarizer = OutputSummarizer(max_length=10, include_stats=True)
        
        content = "1234567890"  # Exactly 10 characters
        result = summarizer.execute(content)
        
        assert result['summary'] == content  # No truncation needed
        assert "..." not in result['summary']

    def test_execute_one_char_over_max_length(self):
        """Test execute with content one character over max_length."""
        summarizer = OutputSummarizer(max_length=10, include_stats=True)
        
        content = "12345678901"  # 11 characters
        result = summarizer.execute(content)
        
        assert result['summary'] == "1234567890..."
        assert len(result['summary']) == 13  # 10 + len("...")


# Additional edge case tests for complete coverage
class TestEdgeCases:
    """Test edge cases and error conditions for transformation callbacks."""

    def test_output_formatter_json_serialization_error(self):
        """Test OutputFormatter handling of non-serializable objects."""
        formatter = OutputFormatter(format_type="json")
        
        # Create an object that can't be JSON serialized
        class NonSerializable:
            def __init__(self):
                self.circular_ref = self
        
        obj = NonSerializable()
        
        # This should raise a TypeError due to circular reference
        with pytest.raises(TypeError):
            formatter.execute(obj)

    def test_output_formatter_with_zero_max_length(self):
        """Test OutputFormatter with max_length of 0."""
        formatter = OutputFormatter(max_length=0)
        
        result = formatter.execute("Hello")
        
        # With max_length=0, the condition `if self.max_length` is False, so no truncation
        assert result == '"Hello"'  # JSON format by default

    def test_data_extractor_with_none_values(self):
        """Test DataExtractor with None values in fields and patterns."""
        extractor = DataExtractor(fields=None, patterns=None)
        
        result = extractor.execute("Test content")
        
        assert result == {}

    def test_data_extractor_complex_regex_patterns(self):
        """Test DataExtractor with complex regex patterns."""
        patterns = {
            "ip_addresses": r"\b(?:[0-9]{1,3}\.){3}[0-9]{1,3}\b",
            "urls": r"https?://(?:[-\w.])+(?:[:\d]+)?(?:/(?:[\w/_.])*(?:\?(?:[\w&=%.])*)?(?:#(?:[\w.])*)?)?",
            "dates": r"\d{4}-\d{2}-\d{2}"
        }
        
        extractor = DataExtractor(patterns=patterns)
        
        content = "Visit https://example.com:8080/path?param=value on 2023-12-25 from IP 192.168.1.1"
        result = extractor.execute(content)
        
        assert "ip_addresses" in result
        assert "urls" in result
        assert "dates" in result
        assert result["ip_addresses"] == "192.168.1.1"
        assert result["dates"] == "2023-12-25"

    def test_output_enricher_with_complex_objects(self):
        """Test OutputEnricher with complex nested objects."""
        enricher = OutputEnricher(
            custom_enrichments={"nested": {"key": "value", "list": [1, 2, 3]}}
        )
        
        class ComplexObj:
            def __init__(self):
                self.data = {"nested": {"inner": "value"}}
                self.list_data = [1, 2, 3]
        
        obj = ComplexObj()
        result = enricher.execute(obj)
        
        assert "nested" in result
        assert result["nested"]["key"] == "value"
        assert result["data"]["nested"]["inner"] == "value"

    def test_output_summarizer_with_special_characters(self):
        """Test OutputSummarizer with special characters and unicode."""
        summarizer = OutputSummarizer(max_length=50, include_stats=True)
        
        content = "Special chars: àáâãäå çèéêë ñòóôõö! Numbers: 123 ⭐️ 🌟"
        result = summarizer.execute(content)
        
        assert result['has_numbers'] is True
        assert result['total_length'] == len(content)
        assert "Special chars" in result['summary']

    def test_all_callbacks_with_empty_dict(self):
        """Test all callbacks with empty dictionary input."""
        empty_dict = {}
        
        formatter = OutputFormatter()
        extractor = DataExtractor(fields=["missing_field"])
        enricher = OutputEnricher()
        summarizer = OutputSummarizer()
        
        # Test all callbacks can handle empty dict
        formatter_result = formatter.execute(empty_dict)
        extractor_result = extractor.execute(empty_dict)
        enricher_result = enricher.execute(empty_dict)
        summarizer_result = summarizer.execute(empty_dict)
        
        assert formatter_result == "{}"
        assert extractor_result == {}
        assert "timestamp" in enricher_result
        assert summarizer_result['summary'] == "{}"

    def test_all_callbacks_with_none_input(self):
        """Test all callbacks with None input."""
        formatter = OutputFormatter()
        extractor = DataExtractor()
        enricher = OutputEnricher()
        summarizer = OutputSummarizer()
        
        # Test all callbacks can handle None
        formatter_result = formatter.execute(None)
        extractor_result = extractor.execute(None)
        enricher_result = enricher.execute(None)
        summarizer_result = summarizer.execute(None)
        
        assert formatter_result == "null"
        assert extractor_result == {}
        assert enricher_result['content'] == "None"
        assert summarizer_result['summary'] == "None"