"""
Model provider enums and constants.

This module provides enumerations for model providers and 
lists of supported models per provider.
"""

from enum import Enum

class ModelProvider(str, Enum):
    """Enum for LLM model providers."""
    OPENAI = "openai"
    ANTHROPIC = "anthropic"
    OLLAMA = "ollama"
    DEEPSEEK = "deepseek"
    DATABRICKS = "databricks"
    GEMINI = "gemini"

# List of supported models per provider
SUPPORTED_MODELS = {
    ModelProvider.OPENAI: [
        "gpt-4-turbo-preview",
        "gpt-4",
        "gpt-4-0125-preview",
        "gpt-4-1106-preview",
        "gpt-3.5-turbo",
        "gpt-3.5-turbo-1106",
        "gpt-4o-mini",
        "gpt-4o",
        "o1-mini",
        "o1",
        "o3-mini",
        "o3-mini-high"
    ],
    ModelProvider.OLLAMA: [
        "qwen2.5:32b",
        "llama2",
        "llama2:13b",
        "llama3.2:latest",
        "mistral",
        "mixtral",
        "codellama",
        "mistral-nemo:12b-instruct-2407-q2_K",
        "llama3.2:3b-text-q8_0",
        "gemma2:27b",
        "deepseek-r1:32b",
        "milkey/QwQ-32B-0305:q4_K_M"
    ],
    ModelProvider.ANTHROPIC: [
        "claude-3-opus-20240229",
        "claude-3-5-sonnet-20241022",
        "claude-3-5-haiku-20241022",
        "claude-3-7-sonnet-20250219",
        "claude-3-7-sonnet-20250219-thinking",
        "claude-3-sonnet",
        "claude-2.1",
        "claude-2.0",
    ],
    ModelProvider.DATABRICKS: [
        "databricks-meta-llama-3-3-70b-instruct",
        "databricks-meta-llama-3-1-405b-instruct",
    ],
    ModelProvider.DEEPSEEK: [
        "deepseek-chat",
        "deepseek-reasoner",
    ],
    ModelProvider.GEMINI: [
        "gemini-2.5-pro",
        "gemini-2.0-flash",
    ]
} 