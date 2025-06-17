"""Supplementary tests for PythonPPTXTool to boost coverage.

This file contains additional test cases that complement the existing
test_python_pptx_tool.py file to achieve higher coverage.
"""

import unittest
import os
import json
import tempfile
import shutil
from datetime import datetime
from unittest.mock import patch, MagicMock

# Use relative imports that will work with the project structure
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))
from src.engines.crewai.tools.custom.python_pptx_tool import (
    PythonPPTXTool, PPTXGenerator, Headline, get_pp_alignment,
    PresentationContent, ContentSlide, load_content_from_dict,
    create_presentation, BulletPoint
)


class TestSupplementaryCoverage(unittest.TestCase):
    """Supplementary tests to boost coverage."""
    
    def setUp(self):
        """Set up test environment."""
        self.test_dir = tempfile.mkdtemp()
        self.tool = PythonPPTXTool()
    
    def tearDown(self):
        """Clean up test environment."""
        shutil.rmtree(self.test_dir)
    
    def test_headline_date_parsing_edge_cases(self):
        """Test all date parsing edge cases for Headline class."""
        # Test all supported date formats
        test_cases = [
            "2023-08-15T12:00:00Z",  # ISO with Z
            "2023-08-15T12:00:00+00:00",  # ISO with timezone
            "2023-08-15",  # YYYY-MM-DD
            "15/08/2023",  # DD/MM/YYYY
            "08/15/2023",  # MM/DD/YYYY
            "August 15, 2023",  # Full month name
            "invalid-date",  # Invalid format
            "",  # Empty string
            None  # None value
        ]
        
        for date_input in test_cases:
            result = Headline.parse_date(date_input)
            self.assertIsInstance(result, datetime)
    
    def test_tool_edge_cases(self):
        """Test edge cases for PythonPPTXTool."""
        # Test with empty string
        result = self.tool._run("")
        self.assertTrue(result.success)
        
        # Test with None title
        result = self.tool._run("Content", None)
        self.assertEqual(result.title, "Presentation")
        
        # Test articles format transformation
        articles_content = {
            "articles": [
                {"title": "Article 1", "description": "Desc 1", "company": "Co 1"},
                {"title": "Article 2", "description": "Desc 2", "company": "Co 2"}
            ]
        }
        result = self.tool._run(articles_content, "Custom Title")
        self.assertTrue(result.success)
        self.assertEqual(result.title, "Custom Title")
        
        # Test articles format without title
        result = self.tool._run(articles_content)
        self.assertTrue(result.success)
        self.assertIn("Recent Events", result.title)
    
    def test_generator_edge_cases(self):
        """Test edge cases for PPTXGenerator."""
        generator = PPTXGenerator(output_dir=self.test_dir)
        
        # Test with very minimal content
        minimal_content = {"title": "Test"}
        result = generator.generate_from_json(minimal_content)
        self.assertIn("file_path", result)
        
        # Test with complex nested content
        complex_content = {
            "title": "Complex Test",
            "headline": {
                "title": "Main Title",
                "subtitle": "Subtitle",
                "date": "2023-08-15T12:00:00"
            },
            "author": "Author",
            "company": "Company",
            "keywords": ["test", "keywords"],
            "slides": [
                {
                    "title": "Mixed Bullet Types",
                    "bullet_points": [
                        "Simple string bullet",
                        {
                            "text": "Complex bullet",
                            "bold": True,
                            "italic": True,
                            "level": 1,
                            "color": [255, 0, 0]
                        }
                    ]
                },
                {
                    "title": "Chart Slide",
                    "chart_data": {
                        "title": "Test Chart",
                        "chart_type": "BAR",
                        "categories": ["A", "B", "C"],
                        "series": {"Series1": [1, 2, 3]}
                    }
                },
                {
                    "title": "Table Slide",
                    "table_data": {
                        "headers": ["Col1", "Col2"],
                        "rows": [["Data1", "Data2"]]
                    }
                },
                {
                    "title": "Shapes Slide",
                    "shapes": [
                        {
                            "type": "rectangle",
                            "x": 1, "y": 1, "width": 2, "height": 1,
                            "text": "Test Shape",
                            "fill_color": [255, 0, 0]
                        }
                    ]
                },
                {
                    "title": "Mixed Content",
                    "elements": [
                        {
                            "type": "text",
                            "content": "Mixed text",
                            "x": 1, "y": 1, "width": 3, "height": 1
                        }
                    ]
                }
            ]
        }
        
        result = generator.generate_from_json(complex_content)
        self.assertIn("file_path", result)
        self.assertTrue(os.path.exists(result["file_path"]))
    
    def test_utility_functions(self):
        """Test utility functions for complete coverage."""
        # Test get_pp_alignment with all values
        alignments = ["left", "center", "right", "justify", "unknown", None]
        for alignment in alignments:
            result = get_pp_alignment(alignment)
            self.assertIsNotNone(result)
    
    def test_presentation_functions(self):
        """Test presentation-level functions."""
        # Test load_content_from_dict
        content_dict = {
            "title": "Dict Test",
            "headline": {"title": "Headline"},
            "slides": [{"title": "Slide 1"}]
        }
        
        loaded = load_content_from_dict(content_dict)
        self.assertEqual(loaded.title, "Dict Test")
        
        # Test create_presentation
        test_file = os.path.join(self.test_dir, "test.pptx")
        result = create_presentation(loaded, test_file)
        self.assertIsNotNone(result)
        self.assertTrue(os.path.exists(test_file))
    
    def test_error_conditions(self):
        """Test various error conditions."""
        # Test invalid content type
        result = self.tool._run(12345)
        self.assertFalse(result.success)
        self.assertIn("Invalid content type", result.message)
        
        # Test malformed JSON string
        result = self.tool._run("{ invalid json }")
        self.assertTrue(result.success)  # Should fallback to text
        
        # Test generator with problematic content
        generator = PPTXGenerator(output_dir=self.test_dir)
        problematic_content = {
            "title": "Error Test",
            "headline": {"title": "Test"},
            "slides": [
                {
                    "title": "Bad Chart",
                    "chart_data": {
                        "title": "Invalid",
                        "chart_type": "INVALID_TYPE"
                    }
                }
            ]
        }
        
        # Should handle gracefully
        result = generator.generate_from_json(problematic_content)
        self.assertIn("file_path", result)
    
    def test_unicode_content(self):
        """Test Unicode and special character handling."""
        unicode_content = {
            "title": "Unicode Test 🚀",
            "headline": {"title": "Unicode 📊"},
            "slides": [
                {
                    "title": "Special Characters ✨",
                    "content": "Content with émojis 🎉 and spëcial çharacters",
                    "bullet_points": [
                        "Bullet with 中文",
                        "Emoji bullet 🔥"
                    ]
                }
            ]
        }
        
        result = self.tool._run(unicode_content)
        self.assertTrue(result.success)
    
    def test_chart_types_coverage(self):
        """Test different chart types."""
        generator = PPTXGenerator(output_dir=self.test_dir)
        
        chart_types = ["BAR", "LINE", "PIE", "COLUMN", "AREA"]
        for chart_type in chart_types:
            content = {
                "title": f"{chart_type} Test",
                "headline": {"title": "Chart Test"},
                "slides": [
                    {
                        "title": f"{chart_type} Chart",
                        "chart_data": {
                            "title": f"{chart_type} Chart",
                            "chart_type": chart_type,
                            "categories": ["A", "B", "C"],
                            "series": {"Series1": [1, 2, 3]}
                        }
                    }
                ]
            }
            
            result = generator.generate_from_json(content)
            self.assertIn("file_path", result)
    
    @patch('src.engines.crewai.tools.custom.python_pptx_tool.PPTXGenerator')
    def test_generator_exception_handling(self, mock_generator):
        """Test exception handling in tool."""
        # Mock generator to raise exception
        mock_instance = MagicMock()
        mock_instance.generate_from_json.side_effect = Exception("Test error")
        mock_generator.return_value = mock_instance
        
        result = self.tool._run("Test content")
        self.assertFalse(result.success)
        self.assertIn("Error creating presentation", result.message)


if __name__ == '__main__':
    unittest.main()