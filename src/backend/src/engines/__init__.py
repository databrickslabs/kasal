"""
Engines package for Kasal.

This package provides various engine implementations for agent execution.

The ``EngineFactory`` export is LAZY (PEP 562). Importing it eagerly makes any
`src.engines.<anything>` import drag in the whole engine, which closes a cycle
the moment a service the engine imports also lives under a package the engine
imports back — exactly what happened when config_adapter moved to
``services/execution``.
"""

__all__ = ['EngineFactory']


def __getattr__(name):
    if name == 'EngineFactory':
        from src.engines.engine_factory import EngineFactory
        return EngineFactory
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
