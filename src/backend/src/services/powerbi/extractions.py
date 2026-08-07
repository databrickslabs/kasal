"""Recording what a PowerBI config-generation run extracted.

One row per config-gen run: the measures, relationships and tables it pulled out of
a semantic model, kept as an audit trail of what a run actually saw.

This service exists because ``PowerBIExtractionRepository`` had NO owner. Its only
caller was the pipeline-config tool, which reached for the repository through
``ToolSessionProvider.powerbi_extraction_repo()`` and stamped ``group_id`` by hand —
so the tenant stamp lived in a tool rather than with the data. PowerBI extraction is
this domain's data, so the writes and the stamping belong here, matching how
``ConverterService`` owns conversion history next door.
"""

import logging
from typing import Any, List, Optional

from src.repositories.powerbi_extraction_repository import PowerBIExtractionRepository

logger = logging.getLogger(__name__)


class PowerBIExtractionService:
    """Owns ``powerbi_extraction`` rows."""

    def __init__(self, session, group_context: Optional[Any] = None):
        """
        Args:
            session: database session, chosen by the caller's entry point.
            group_context: tenant context; supplies the ``group_id`` and
                ``created_by_email`` stamps so callers do not set them themselves.
        """
        self.session = session
        self.group_context = group_context
        self.repository = PowerBIExtractionRepository(session)

    async def record_extraction(self, data: Any) -> Any:
        """Persist one extraction, stamping the tenant.

        Args:
            data: a pydantic model or dict of column values.

        Returns:
            The created row.
        """
        values = data.model_dump() if hasattr(data, "model_dump") else dict(data)
        if self.group_context:
            values.setdefault(
                "group_id", getattr(self.group_context, "primary_group_id", None)
            )
            # group_email first: that is the field GroupContext actually carries here
            # (the tool this replaced tried group_email then email, never user_email).
            values.setdefault(
                "created_by_email",
                getattr(self.group_context, "group_email", None)
                or getattr(self.group_context, "email", None)
                or getattr(self.group_context, "user_email", None),
            )
        return await self.repository.create(values)

    async def list_for_execution(self, execution_id: str) -> List[Any]:
        """Every extraction recorded for one crew/flow run, newest first."""
        return await self.repository.find_by_execution_id(execution_id)
