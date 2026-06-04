"""
Coverage tests for engines/base/base_engine_service.py
The abstract methods have 'pass' bodies that need to be covered.
"""
import pytest
from src.engines.base.base_engine_service import BaseEngineService


class ConcreteEngineService(BaseEngineService):
    """Concrete implementation to test abstract method bodies."""

    async def initialize(self, **kwargs) -> None:
        return await super().initialize(**kwargs)  # calls the 'pass' line

    async def run_execution(self, execution_id: str, config) -> dict:
        return await super().run_execution(execution_id, config)

    async def get_execution_status(self, execution_id: str) -> dict:
        return await super().get_execution_status(execution_id)

    async def cancel_execution(self, execution_id: str) -> bool:
        return await super().cancel_execution(execution_id)


@pytest.mark.asyncio
async def test_initialize_abstract_pass():
    svc = ConcreteEngineService()
    result = await svc.initialize(model="gpt4")
    assert result is None


@pytest.mark.asyncio
async def test_run_execution_abstract_pass():
    svc = ConcreteEngineService()
    result = await svc.run_execution("exec1", {})
    assert result is None


@pytest.mark.asyncio
async def test_get_execution_status_abstract_pass():
    svc = ConcreteEngineService()
    result = await svc.get_execution_status("exec1")
    assert result is None


@pytest.mark.asyncio
async def test_cancel_execution_abstract_pass():
    svc = ConcreteEngineService()
    result = await svc.cancel_execution("exec1")
    assert result is None
