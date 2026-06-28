"""
Shared embedder resolution for knowledge ingest + search.

Knowledge upload (write) and knowledge search (query) MUST embed with the SAME
model, otherwise query vectors never match the stored chunk vectors. Production
uses the Databricks endpoint (``databricks-gte-large-en``, 1024-dim). When
Databricks auth is unavailable (local dev), both sides fall back to a local
Ollama model.

The fallback only ever runs when Databricks is unavailable — i.e. local dev,
where the knowledge store is SQLite. There the ``Vector`` column is stored as
plain ``TEXT`` (a JSON-encoded list, see ``models/documentation_embedding.py``),
so it does NOT enforce the nominal ``vector(1024)`` width: a 768-dim
``nomic-embed-text`` vector is fine as long as ingest and search use the SAME
model (they both call this resolver). The 1024 width only binds real pgvector,
which only runs in production — and there the Databricks embedder (1024-dim) is
used, never this fallback. So the default mirrors the memory embedder
(``nomic-embed-text``) for one consistent local embedding model.

Override with ``KNOWLEDGE_OLLAMA_EMBED_MODEL`` — set it to a 1024-dim model
(e.g. ``mxbai-embed-large``, requires ``ollama pull``) ONLY if you run local dev
against real Postgres/pgvector without Databricks, where the column width binds.

This mirrors the Databricks→Ollama fallback in
``engines/crewai/config/embedder_config_builder.py`` so memory and knowledge stay
consistent. Both the embed and search paths call
:func:`resolve_knowledge_embedder_config` and pass the result straight to
``LLMManager.get_embedding(s)`` as ``embedder_config``.
"""
import os
from typing import Any, Dict, Optional
import logging

logger = logging.getLogger(__name__)

# Databricks GTE-large = 1024 dims (matches documentation_embeddings.embedding).
KNOWLEDGE_EMBEDDING_MODEL = "databricks-gte-large-en"

# Local fallback (dev only). Defaults to nomic-embed-text — the same model the
# memory embedder uses locally — because local dev stores vectors in SQLite as
# TEXT, which ignores the nominal 1024 width. Override with
# KNOWLEDGE_OLLAMA_EMBED_MODEL (e.g. mxbai-embed-large) for local pgvector setups.
KNOWLEDGE_OLLAMA_EMBED_MODEL = os.getenv(
    "KNOWLEDGE_OLLAMA_EMBED_MODEL", "nomic-embed-text"
)


async def resolve_knowledge_embedder_config(
    user_token: Optional[str] = None,
    group_id: Optional[str] = None,
) -> Dict[str, Any]:
    """Resolve the embedder config that BOTH knowledge ingest and search must use.

    Returns a Databricks embedder when Databricks auth resolves, otherwise a
    local Ollama embedder. The returned shape matches what
    ``LLMManager.get_embedding`` / ``get_embeddings`` expect for
    ``embedder_config``.

    Args:
        user_token: Optional user token for OBO authentication.
        group_id: Optional group ID for PAT lookup / tenant isolation.

    Returns:
        ``{"provider": "databricks"|"ollama", "config": {"model": ..., ...}}``
    """
    if await _databricks_available(user_token=user_token, group_id=group_id):
        return {
            "provider": "databricks",
            "config": {"model": KNOWLEDGE_EMBEDDING_MODEL},
        }

    # The Ollama HTTP host is read from OLLAMA_API_BASE by LLMManager; we include
    # it here too so the config is self-describing for logging/diagnostics.
    ollama_url = os.getenv("OLLAMA_API_BASE", "http://localhost:11434")
    logger.info(
        "Databricks embeddings unavailable; knowledge embedder falling back to "
        f"Ollama '{KNOWLEDGE_OLLAMA_EMBED_MODEL}' at {ollama_url}"
    )
    return {
        "provider": "ollama",
        "config": {"model": KNOWLEDGE_OLLAMA_EMBED_MODEL, "url": ollama_url},
    }


async def _databricks_available(
    user_token: Optional[str], group_id: Optional[str]
) -> bool:
    """Best-effort probe for usable Databricks embedding auth.

    Mirrors how the memory embedder decides Databricks is reachable. Any failure
    is treated as "unavailable" so the caller falls back to the local embedder
    rather than erroring.
    """
    try:
        from src.utils.databricks_auth import get_auth_context
        from src.utils.user_context import UserContext

        token = user_token or UserContext.get_user_token()
        auth = await get_auth_context(user_token=token, group_id=group_id)
        return auth is not None
    except Exception as e:  # pragma: no cover - defensive, auth probe is best-effort
        logger.debug(f"Databricks auth probe failed; using local embedder: {e}")
        return False
