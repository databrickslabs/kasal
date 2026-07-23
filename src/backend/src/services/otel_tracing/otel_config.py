"""OTel configuration for Kasal tracing.

Creates an explicit TracerProvider (not global default) to avoid conflicts
with MLflow's OTEL_SDK_DISABLED handling. The provider is scoped per-execution.
"""

import logging
import os
from typing import Optional

from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider

logger = logging.getLogger(__name__)

# Module-level reference to the active provider for shutdown
_active_provider: Optional[TracerProvider] = None


def is_otel_tracing_enabled() -> bool:
    """Check if OTel tracing is enabled via environment variable."""
    return os.environ.get("KASAL_OTEL_TRACING", "true").lower() == "true"


def create_kasal_tracer_provider(
    job_id: str,
    service_name: str = "kasal-crew-engine",
) -> TracerProvider:
    """Create an explicit TracerProvider for a crew/flow execution.

    Uses explicit provider (not global default) to avoid conflicts with
    MLflow's OTel SDK disabled handling in subprocess mode.

    Args:
        job_id: The execution/job ID for resource attribution.
        service_name: Service name for OTel resource attributes.

    Returns:
        A configured TracerProvider ready for span processors.
    """
    global _active_provider

    resource = Resource.create(
        {
            "service.name": service_name,
            "kasal.job_id": job_id,
            "kasal.process_id": str(os.getpid()),
        }
    )

    provider = TracerProvider(resource=resource)
    _active_provider = provider

    logger.info(
        f"[OTel] Created TracerProvider for job {job_id} (service={service_name})"
    )
    return provider


def shutdown_provider() -> None:
    """Shutdown the active provider, flushing remaining spans."""
    global _active_provider
    if _active_provider is not None:
        try:
            _active_provider.shutdown()
            logger.info("[OTel] TracerProvider shutdown complete")
        except Exception as e:
            logger.warning(f"[OTel] Error during provider shutdown: {e}")
        finally:
            _active_provider = None
