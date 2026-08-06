"""The hand-rolled ``Vector`` type exposes pgvector's distance operators.

``Vector`` is a local ``UserDefinedType`` rather than the pgvector library's, so
the same models work on SQLite (where the column is TEXT holding JSON). Being
hand-rolled, it shipped without the comparator methods its callers assume, and
``WorkflowRecipe.embedding.cosine_distance(...)`` raised::

    Neither 'InstrumentedAttribute' object nor 'Comparator' object associated
    with WorkflowRecipe.embedding has an attribute 'cosine_distance'

The recipe repository catches that and skips, so on every Postgres/Lakebase run
exemplar lookup silently degraded to "no exemplars" — the feature was off, not
broken, which is why it produced a warning nobody chased:

    [WorkflowRecipes] Exemplar lookup skipped: ... has no attribute
    'cosine_distance'

Two details are load-bearing and were both found by running against the LIVE
database, not by compiling SQL:

1. The parameter needs an explicit CAST. Without it the driver sends the
   formatted string as ``unknown`` and PostgreSQL cannot resolve the operator —
   ``operator does not exist: public.vector <=> unknown``. The compiled SQL looks
   perfectly correct; only the server rejects it.
2. ``cache_ok`` must be set, or SQLAlchemy refuses to cache any statement
   touching an embedding column and warns on every one.
"""

import pytest
from sqlalchemy import select
from sqlalchemy.dialects import postgresql

import src.db.all_models  # noqa: F401  (register every model)
from src.models.documentation_embedding import DocumentationEmbedding, Vector
from src.models.workflow_recipe import WorkflowRecipe

QUERY = [0.1, 0.2, 0.3]


def _sql(expr) -> str:
    return str(expr.compile(dialect=postgresql.dialect()))


class TestTheOperatorsExist:
    @pytest.mark.parametrize(
        "method,operator",
        [
            ("cosine_distance", "<=>"),
            ("l2_distance", "<->"),
            ("max_inner_product", "<#>"),
        ],
    )
    def test_each_emits_its_pgvector_operator(self, method, operator):
        expr = getattr(WorkflowRecipe.embedding, method)(QUERY)
        assert operator in _sql(expr)

    def test_the_documentation_embedding_column_has_them_too(self):
        """Both models share the type, so both get the comparator."""
        expr = DocumentationEmbedding.embedding.cosine_distance(QUERY)
        assert "<=>" in _sql(expr)


class TestTheParameterIsCast:
    def test_the_bind_is_cast_to_vector(self):
        """THE live-only failure: an uncast bind arrives as `unknown`."""
        sql = _sql(WorkflowRecipe.embedding.cosine_distance(QUERY))
        assert "CAST" in sql.upper(), sql
        assert "vector" in sql.lower(), sql

    def test_the_value_travels_as_a_bound_parameter(self):
        """Not inlined — a 1024-float literal per query would be absurd."""
        compiled = WorkflowRecipe.embedding.cosine_distance(QUERY).compile(
            dialect=postgresql.dialect()
        )
        assert QUERY in list(compiled.params.values())

    def test_it_composes_into_the_repository_query(self):
        """Shape of _find_similar_postgres: select + label + order_by."""
        distance = WorkflowRecipe.embedding.cosine_distance(QUERY)
        stmt = (
            select(WorkflowRecipe.id)
            .add_columns(distance.label("distance"))
            .order_by(distance)
            .limit(5)
        )
        sql = _sql(stmt)
        assert "<=>" in sql
        assert "ORDER BY" in sql


class TestStatementCaching:
    def test_cache_ok_is_set(self):
        """Unset, SQLAlchemy warns on and refuses to cache EVERY such statement.

        The type's only state is ``dim``, immutable per column, so it is safe in a
        cache key.
        """
        assert Vector.cache_ok is True

    def test_building_the_expression_emits_no_sqlalchemy_warning(self):
        import warnings

        with warnings.catch_warnings():
            warnings.simplefilter("error")
            _sql(WorkflowRecipe.embedding.cosine_distance(QUERY))


class TestSqliteIsUnaffected:
    """SQLite has no vector operators; the repository takes a Python-side path.

    These comparators must therefore never be REQUIRED for the SQLite branch to
    work — asserting the column still behaves as a plain TEXT-backed column there.
    """

    def test_the_column_spec_is_still_a_vector_on_postgres(self):
        assert Vector(1024).get_col_spec() == "vector(1024)"

    def test_a_list_binds_as_json_on_sqlite(self):
        import json

        from sqlalchemy.dialects import sqlite

        processor = Vector(3).bind_processor(sqlite.dialect())
        assert json.loads(processor([1.0, 2.0, 3.0])) == [1.0, 2.0, 3.0]

    def test_a_list_binds_as_a_vector_literal_on_postgres(self):
        processor = Vector(3).bind_processor(postgresql.dialect())
        assert processor([1.0, 2.0, 3.0]) == "[1.0,2.0,3.0]"
