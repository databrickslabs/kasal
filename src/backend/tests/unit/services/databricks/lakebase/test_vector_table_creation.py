"""Vector-column tables are created WITHOUT the vector column, never skipped.

pgvector is not a trusted PostgreSQL extension, so ``CREATE EXTENSION vector``
needs ``databricks_superuser`` while a deployed Databricks App's service principal
only has CONNECT/CREATE/DML. On any Lakebase where an owner has not enabled it,
``CREATE TABLE ... embedding vector(1024)`` fails with 42704
``type "vector" does not exist``.

Two bugs, both fixed together, both found by ``workflow_recipes``:

1. The set of vector tables was the LITERAL
   ``{"documentation_embeddings", "knowledge_embeddings"}``, repeated in four
   places. It was right when written and went stale silently: ``workflow_recipes``
   later gained ``embedding = Column(Vector(1024))``, wasn't listed, and so took
   the normal CREATE path and failed with 42704.

2. Being on the list wouldn't have saved it either. The handling was
   ``if table.name == "documentation_embeddings"`` with hand-written SQL — every
   other vector table was logged as "skipping" and then created by NOTHING. So the
   choice was "fails loudly" or "silently has no table"; the feature is broken
   either way.

Now the set is derived from the model metadata and the DDL is generated from the
model, so a new vector column is handled the day it is added.
"""

import pytest

# Registering every model is required: these helpers read Base.metadata, and
# workflow_recipes is absent from it unless its module has been imported. That is
# also why the stale literal was never caught by a metadata-driven check.
import src.db.all_models  # noqa: F401
from src.db.base import Base
from src.services.databricks.lakebase.schema import (
    create_table_without_vector_sql,
    vector_column_tables,
)


class TestVectorColumnTablesIsDerived:
    def test_it_finds_every_table_with_a_vector_column(self):
        from src.models.documentation_embedding import Vector

        expected = {
            table.name
            for table in Base.metadata.sorted_tables
            if any(isinstance(col.type, Vector) for col in table.columns)
        }
        assert vector_column_tables() == expected
        # Guard the specific regression: this is the one the literal missed.
        assert "workflow_recipes" in vector_column_tables()

    def test_the_known_vector_tables_are_all_present(self):
        """A literal list would drift again; assert the current, full set."""
        assert vector_column_tables() == {
            "documentation_embeddings",
            "knowledge_embeddings",
            "workflow_recipes",
        }


class TestGeneratedDdl:
    @pytest.mark.parametrize(
        "table_name",
        sorted(
            {
                "documentation_embeddings",
                "knowledge_embeddings",
                "workflow_recipes",
            }
        ),
    )
    def test_the_vector_column_is_gone(self, table_name):
        sql = create_table_without_vector_sql(table_name)
        assert sql, f"no DDL generated for {table_name}"
        assert "vector" not in sql.lower(), sql

    @pytest.mark.parametrize(
        "table_name",
        ["documentation_embeddings", "knowledge_embeddings", "workflow_recipes"],
    )
    def test_it_is_idempotent_and_names_the_table(self, table_name):
        sql = create_table_without_vector_sql(table_name)
        assert "IF NOT EXISTS" in sql, sql
        assert table_name in sql, sql

    def test_the_non_vector_columns_all_survive(self):
        """Dropping the embedding must not drop anything else.

        workflow_recipes carries the intent hash, the YAML and the reuse counters —
        the table is useful without embeddings, which is the whole reason to create
        it rather than skip it.
        """
        sql = create_table_without_vector_sql("workflow_recipes")
        for column in Base.metadata.tables["workflow_recipes"].columns:
            if column.name == "embedding":
                continue
            assert column.name in sql, f"{column.name} was dropped too:\n{sql}"

    def test_embedding_is_the_only_thing_removed(self):
        sql = create_table_without_vector_sql("workflow_recipes")
        assert "embedding" not in sql

    def test_an_unknown_table_returns_none_rather_than_raising(self):
        """Callers treat None as "cannot build DDL" and log, not crash."""
        assert create_table_without_vector_sql("no_such_table") is None

    def test_the_source_metadata_is_not_mutated(self):
        """The generator copies the table; the real metadata keeps its column.

        Removing the column in place would silently break every other consumer of
        the model — the ORM mapping, migrations, and the local SQLite path where
        pgvector is irrelevant.
        """
        before = {c.name for c in Base.metadata.tables["workflow_recipes"].columns}
        create_table_without_vector_sql("workflow_recipes")
        after = {c.name for c in Base.metadata.tables["workflow_recipes"].columns}
        assert "embedding" in before
        assert before == after


class TestEveryVectorTableIsRoutedAwayFromNormalCreation:
    """The wave batcher must never see a vector table.

    ``_create_tables_batch_sync`` issues the model's full CREATE, so a vector table
    reaching it is exactly the 42704 the user hit.
    """

    def test_no_vector_table_lands_in_the_normal_batch(self):
        from src.services.databricks.lakebase.schema import LakebaseSchemaService

        skip = vector_column_tables()
        waves, _ = LakebaseSchemaService()._get_dependency_waves(
            Base.metadata.sorted_tables
        )
        normal = [name for wave in waves for name in wave if name not in skip]
        assert not (set(normal) & skip)
        # And the tables are still accounted for somewhere.
        special = [name for wave in waves for name in wave if name in skip]
        assert set(special) == skip
