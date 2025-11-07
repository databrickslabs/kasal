"""
Base tool registry abstract class.

This module defines the base interface for tool registry implementations
across different engine types.
"""
from abc import ABC, abstractmethod
from typing import Dict, Any, Optional, List, Type

class BaseToolRegistry(ABC):
    """Abstract base class for tool registries across different engine implementations"""
    
    @abstractmethod
    def register_tool(self, tool_name: str, tool_class: Any, **kwargs) -> None:
        """
        Register a tool with the registry
        
        Args:
            tool_name: Name of the tool
            tool_class: Tool class or callable
            **kwargs: Additional tool configuration
        """
        pass
    
    @abstractmethod
    def get_tool(self, tool_name: str, **kwargs) -> Any:
        """
        Get a tool instance by name
        
        Args:
            tool_name: Name of the tool
            **kwargs: Tool initialization parameters
            
        Returns:
            Instantiated tool or None if not found
        """
        pass
    
    @abstractmethod
    def get_all_tools(self) -> List[str]:
        """
        Get list of all available tool names
        
        Returns:
            List of tool names
        """
        pass
    
    @abstractmethod
    async def load_api_keys(self, **kwargs) -> None:
        """
        Load API keys for tools
        
        Args:
            **kwargs: Engine-specific parameters
        """
        pass 