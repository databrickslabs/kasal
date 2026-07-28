"""Databricks Apps OpenTelemetry log export to Unity Catalog.

When **App telemetry** is enabled in the Databricks App settings UI (and a UC
schema is chosen), the platform injects ``OTEL_EXPORTER_OTLP_ENDPOINT`` and
routes exported OTLP logs to the ``otel_logs`` table in that schema. This module
attaches an OTLP log handler to the root logger so the app's Python logs land
there. It is a no-op when the endpoint isn't present (telemetry not enabled).

Mirrors Kasal's own OTel app-telemetry setup.
"""

import logging
import os

# Standard LogRecord attribute names — anything else with a None value is an
# "extra" attribute the OTLP exporter can't encode, so it's stripped below.
_STANDARD_LOG_RECORD_ATTRS = {
    "name", "msg", "args", "levelname", "levelno", "pathname", "filename",
    "module", "exc_info", "exc_text", "stack_info", "lineno", "funcName",
    "created", "msecs", "relativeCreated", "thread", "threadName",
    "processName", "process", "taskName", "message", "asctime",
}


class _NoneAttributeFilter(logging.Filter):
    """Strip None-valued non-standard attributes (OTLP can't encode None)."""

    def filter(self, record: logging.LogRecord) -> bool:
        for key in list(record.__dict__):
            if (
                record.__dict__[key] is None
                and not key.startswith("_")
                and key not in _STANDARD_LOG_RECORD_ATTRS
            ):
                del record.__dict__[key]
        return True


def setup_otel_logging(log_level: str = "INFO") -> None:
    """Export app logs to the UC ``otel_logs`` table when App telemetry is on."""
    endpoint = os.environ.get("OTEL_EXPORTER_OTLP_ENDPOINT")
    if not endpoint:
        print(
            "OTEL_EXPORTER_OTLP_ENDPOINT not set; skipping UC log export. Enable "
            "'App telemetry' (and pick a Unity Catalog schema) in the Databricks "
            "App settings to write logs to the otel_logs table."
        )
        return

    # OTEL_SDK_DISABLED would silently no-op the exporter.
    os.environ.pop("OTEL_SDK_DISABLED", None)
    try:
        from opentelemetry.sdk._logs import LoggerProvider, LoggingHandler
        from opentelemetry.sdk._logs.export import BatchLogRecordProcessor
        from opentelemetry.sdk.resources import Resource

        protocol = os.environ.get("OTEL_EXPORTER_OTLP_PROTOCOL", "grpc").lower()
        if protocol.startswith("http"):
            from opentelemetry.exporter.otlp.proto.http._log_exporter import (
                OTLPLogExporter,
            )
        else:
            from opentelemetry.exporter.otlp.proto.grpc._log_exporter import (
                OTLPLogExporter,
            )

        logs_endpoint = os.environ.get("OTEL_EXPORTER_OTLP_LOGS_ENDPOINT", endpoint)
        # Localhost (Databricks Apps sidecar) / http:// endpoints are insecure.
        use_insecure = bool(logs_endpoint) and (
            "localhost" in logs_endpoint
            or "127.0.0.1" in logs_endpoint
            or logs_endpoint.startswith("http://")
        )
        service_name = os.environ.get("OTEL_SERVICE_NAME", "{{APP_NAME}}")
        resource = Resource.create({"service.name": service_name})
        exporter = OTLPLogExporter(endpoint=logs_endpoint, insecure=use_insecure)
        provider = LoggerProvider(resource=resource)
        provider.add_log_record_processor(BatchLogRecordProcessor(exporter))

        handler = LoggingHandler(
            level=getattr(logging, log_level.upper(), logging.INFO),
            logger_provider=provider,
        )
        handler.addFilter(_NoneAttributeFilter())
        logging.getLogger().addHandler(handler)
        print(
            f"OTel app telemetry -> otel_logs "
            f"(endpoint={logs_endpoint}, protocol={protocol}, service={service_name})"
        )
    except ImportError as e:
        print(
            f"OTel log export: packages missing ({e}). Add "
            "'opentelemetry-exporter-otlp-proto-grpc' to dependencies."
        )
    except Exception as e:  # noqa: BLE001
        print(f"OTel log export setup failed: {e}")
