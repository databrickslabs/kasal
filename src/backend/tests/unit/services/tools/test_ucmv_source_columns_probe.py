"""Reading the real columns of the source-layer views through the deployer's session."""

from src.services.tools.ucmv_source_columns import columns_query, read_source_columns

DEPLOY_BASE = {
    "catalog": "acme-dev-catalog",
    "schema_name": "sandbox",
    "warehouse_id": "w1",
}


class _FakeDeployer:
    def __init__(self, rows=None, success=True, session_error=""):
        self.rows = rows or []
        self.success = success
        self.session_error = session_error
        self.statements: list[str] = []
        self.opts = None

    def _open_session(self, opts):
        self.opts = opts
        if self.session_error:
            return None, self.session_error

        def run(sql):
            self.statements.append(sql)
            if not self.success:
                return {"success": False, "error": "boom"}
            return {"success": True, "data": {"result": {"data_array": self.rows}}}

        return run, None


def test_query_lists_columns_of_the_named_views_in_the_namespace():
    sql = columns_query("acme-dev-catalog", "sandbox", ["src_fact_a", "Dim_B"])
    assert "`acme-dev-catalog`.information_schema.columns" in sql
    assert "lower(table_schema) = lower('sandbox')" in sql
    assert "lower(table_name) IN ('src_fact_a', 'dim_b')" in sql


def test_names_and_types_come_back_per_view():
    deployer = _FakeDeployer(
        rows=[
            ["src_fact_a", "id", "INT"],
            ["src_fact_a", "source", "STRING"],
            ["src_dim_b", "month", None],
        ]
    )
    found = read_source_columns(deployer, DEPLOY_BASE, ["src_fact_a", "src_dim_b"])
    assert found == {
        "src_fact_a": [
            {"name": "id", "type": "INT"},
            {"name": "source", "type": "STRING"},
        ],
        "src_dim_b": [{"name": "month", "type": ""}],
    }
    assert deployer.opts.catalog == "acme-dev-catalog"
    assert deployer.opts.schema == "sandbox"
    assert len(deployer.statements) == 1


def test_a_failed_query_gives_no_columns_not_an_error():
    assert (
        read_source_columns(_FakeDeployer(success=False), DEPLOY_BASE, ["src_x"]) == {}
    )


def test_no_session_gives_no_columns():
    deployer = _FakeDeployer(session_error="Authentication failed")
    assert read_source_columns(deployer, DEPLOY_BASE, ["src_x"]) == {}
    assert deployer.statements == []


def test_no_views_means_no_query():
    deployer = _FakeDeployer()
    assert read_source_columns(deployer, DEPLOY_BASE, []) == {}
    assert deployer.statements == []
