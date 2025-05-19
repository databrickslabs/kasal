"""
Global test configuration for pytest.

This file is automatically loaded by pytest and contains global fixtures
and configuration settings that apply to all tests.
"""
import os
import sys

# Add the backend directory to the Python path so that 'src' can be imported
backend_dir = os.path.abspath(os.path.dirname(os.path.dirname(__file__)))
if backend_dir not in sys.path:
    sys.path.insert(0, backend_dir) 