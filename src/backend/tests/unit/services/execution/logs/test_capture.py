"""The import-time logging setup must still forward engine and library logs."""

import logging
from unittest.mock import MagicMock, patch

import pytest

from src.services.execution.logs.capture import ExecutionLogCapture


@pytest.fixture
def capture():
    names = ("kasal_engine", "langchain", "httpx", "openai", "src.services.converters")
    loggers = [logging.getLogger(name) for name in names]
    saved = [(log, log.handlers[:], log.propagate, log.level) for log in loggers]
    singleton = ExecutionLogCapture._instance
    manager = MagicMock()
    try:
        ExecutionLogCapture._instance = None
        with patch("src.services.execution.logs.capture.LoggerManager") as factory:
            factory.get_instance.return_value = manager
            yield ExecutionLogCapture(), manager.crew
    finally:
        ExecutionLogCapture._instance = singleton
        for log, handlers, propagate, level in saved:
            log.handlers = handlers
            log.propagate = propagate
            log.setLevel(level)


def test_initialization_installs_handlers_once(capture):
    instance, _ = capture
    engine = logging.getLogger("kasal_engine")
    handlers = engine.handlers[:]
    assert instance is ExecutionLogCapture()
    assert engine.handlers == handlers
    assert (
        sum(type(handler).__name__ == "EngineRedirectHandler" for handler in handlers)
        == 1
    )
    assert engine.propagate is False
    assert engine.level == logging.DEBUG


@pytest.mark.parametrize(
    "name", ["kasal_engine", "langchain", "httpx", "openai", "src.services.converters"]
)
def test_initialized_handler_forwards_message_and_severity(capture, name):
    _, crew = capture
    logging.getLogger(name).warning("task %s finished", "one")
    crew.log.assert_called_once_with(logging.WARNING, "ENGINE-LOG: task one finished")


def test_related_logger_failure_does_not_stop_other_handlers(capture):
    instance, crew = capture
    get_logger = logging.getLogger

    def lookup(name):
        if name == "httpx":
            raise RuntimeError("logger unavailable")
        return get_logger(name)

    with patch("logging.getLogger", side_effect=lookup):
        instance._setup_engine_logging()
    get_logger("openai").info("still connected")
    crew.log.assert_called_once_with(logging.INFO, "ENGINE-LOG: still connected")


def test_engine_logger_failure_is_reported(capture):
    instance, _ = capture
    with (
        patch("logging.getLogger", side_effect=RuntimeError("logger unavailable")),
        patch("src.services.execution.logs.capture.logger") as logger,
    ):
        instance._setup_engine_logging()
    logger.error.assert_called_once()
    assert "logger unavailable" in logger.error.call_args.args[0]
