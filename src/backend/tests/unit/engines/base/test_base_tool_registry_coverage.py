"""
Coverage tests for engines/base/base_tool_registry.py
"""
import pytest
from src.engines.base.base_tool_registry import BaseToolRegistry


class ConcreteRegistry(BaseToolRegistry):
    """Concrete implementation to exercise abstract pass bodies."""

    def register_tool(self, tool_name: str, tool_class, **kwargs) -> None:
        return super().register_tool(tool_name, tool_class, **kwargs)

    def get_tool(self, tool_name: str, **kwargs):
        return super().get_tool(tool_name, **kwargs)

    def get_all_tools(self):
        return super().get_all_tools()

    async def load_api_keys(self, **kwargs) -> None:
        return await super().load_api_keys(**kwargs)


def test_register_tool_abstract_pass():
    registry = ConcreteRegistry()
    result = registry.register_tool("my_tool", object)
    assert result is None


def test_get_tool_abstract_pass():
    registry = ConcreteRegistry()
    result = registry.get_tool("my_tool")
    assert result is None


def test_get_all_tools_abstract_pass():
    registry = ConcreteRegistry()
    result = registry.get_all_tools()
    assert result is None


@pytest.mark.asyncio
async def test_load_api_keys_abstract_pass():
    registry = ConcreteRegistry()
    result = await registry.load_api_keys(provider="openai")
    assert result is None
