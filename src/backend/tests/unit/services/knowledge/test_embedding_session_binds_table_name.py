"""The knowledge table probe binds the table name instead of interpolating it."""

import asyncio
from unittest.mock import AsyncMock, MagicMock

from src.services.knowledge.embedding_session import ensure_lakebase_doc_table


def test_the_column_probe_binds_its_table_name():
    session = MagicMock()
    present = MagicMock()
    present.fetchall.return_value = []
    session.execute = AsyncMock(return_value=present)

    asyncio.run(ensure_lakebase_doc_table(session))

    probe_sql, probe_params = session.execute.await_args_list[0].args
    assert "table_name = :table_name" in str(probe_sql)
    assert probe_params == {"table_name": "knowledge_embeddings"}
