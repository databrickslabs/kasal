"""Tests for YAML emitter (matches original monolith's emit_yaml signature)."""
import pytest
from src.engines.crewai.tools.custom.metric_view_utils.data_classes import MetricViewSpec, TranslationResult
from src.engines.crewai.tools.custom.metric_view_utils.yaml_emitter import emit_yaml


@pytest.fixture
def basic_spec():
    return MetricViewSpec(
        fact_table_key='fact_test',
        source_table='catalog.schema.test_table',
        view_name='fact_test_uc_metric_view',
        comment='Test metric view',
        joins=[],
        dimensions=[{'name': 'region', 'expr': 'source.region', 'comment': 'Region'}],
        measures=[
            TranslationResult(
                measure_name='total_sales',
                original_name='Total Sales',
                sql_expr='SUM(source.sales)',
                is_translatable=True,
                skip_reason='Total sales',
                dax_expression='SUM(Sales[Amount])',
                confidence='high',
                category='base',
            )
        ],
        untranslatable=[],
    )


class TestEmitYaml:
    def test_basic_output(self, basic_spec):
        yaml = emit_yaml(basic_spec)
        assert "version: '1.1'" in yaml
        assert 'source: catalog.schema.test_table' in yaml
        assert 'measures:' in yaml
        assert 'total_sales' in yaml
        assert 'SUM(source.sales)' in yaml
        # Note: 'region' dimension gets dropped by phantom detection
        # (it doesn't appear in any measure expression) — correct behavior

    def test_with_joins(self):
        spec = MetricViewSpec(
            fact_table_key='fact',
            source_table='cat.sch.tbl',
            view_name='test_view',
            comment='Test',
            joins=[{'name': 'dim_geo', 'source': 'cat.sch.dim', 'join_on': 'source.key = dim_geo.key'}],
            dimensions=[],
            measures=[
                TranslationResult(
                    measure_name='val', original_name='val',
                    sql_expr='SUM(source.val)', is_translatable=True,
                    skip_reason='Val', dax_expression='', confidence='high', category='base',
                )
            ],
            untranslatable=[],
        )
        yaml = emit_yaml(spec)
        assert 'joins:' in yaml
        assert 'dim_geo' in yaml

    def test_with_source_sql(self):
        spec = MetricViewSpec(
            fact_table_key='fact',
            source_table='cat.sch.tbl',
            view_name='test_view',
            comment='Test',
            joins=[],
            dimensions=[],
            measures=[
                TranslationResult(
                    measure_name='val', original_name='val',
                    sql_expr='SUM(source.val)', is_translatable=True,
                    skip_reason='Val', dax_expression='', confidence='high', category='base',
                )
            ],
            untranslatable=[],
            source_sql='SELECT * FROM cat.sch.tbl WHERE x = 1',
        )
        yaml = emit_yaml(spec)
        assert 'source: |-' in yaml
        assert 'SELECT * FROM' in yaml

    def test_empty_measures_returns_empty(self):
        spec = MetricViewSpec(
            fact_table_key='fact',
            source_table='cat.sch.tbl',
            view_name='test_view',
            comment='Test',
            joins=[],
            dimensions=[],
            measures=[],
            untranslatable=[],
        )
        yaml = emit_yaml(spec)
        assert yaml == ''

    def test_dangerous_sql_blocked(self):
        """Measures with dangerous SQL are removed from output (emit_yaml deep-copies)."""
        spec = MetricViewSpec(
            fact_table_key='fact',
            source_table='cat.sch.tbl',
            view_name='test_view',
            comment='Test',
            joins=[],
            dimensions=[],
            measures=[
                TranslationResult(
                    measure_name='safe_val', original_name='Safe Val',
                    sql_expr='SUM(source.val)', is_translatable=True,
                    skip_reason='', dax_expression='', confidence='high', category='base',
                ),
                TranslationResult(
                    measure_name='evil', original_name='Evil',
                    sql_expr='SUM(source.val); DROP TABLE users', is_translatable=True,
                    skip_reason='', dax_expression='', confidence='high', category='base',
                ),
            ],
            untranslatable=[],
        )
        yaml = emit_yaml(spec)
        assert 'safe_val' in yaml
        # The dangerous measure should NOT appear in the measures section of YAML output
        # It appears only in the Untranslatable comments section
        assert 'Untranslatable' in yaml
        assert '# Evil: Blocked' in yaml

    def test_dax_measures_emitted(self):
        """DAX-translated measures appear in a separate section."""
        spec = MetricViewSpec(
            fact_table_key='fact',
            source_table='cat.sch.tbl',
            view_name='test_view',
            comment='Test',
            joins=[],
            dimensions=[],
            measures=[
                TranslationResult(
                    measure_name='base_val', original_name='Base Val',
                    sql_expr='SUM(source.val)', is_translatable=True,
                    skip_reason='', dax_expression='', confidence='high', category='base',
                ),
                TranslationResult(
                    measure_name='dax_ratio', original_name='DAX Ratio',
                    sql_expr='MEASURE(base_val) / NULLIF(SUM(source.val), 0)',
                    is_translatable=True, skip_reason='', dax_expression='DIVIDE(...)',
                    confidence='medium', category='single_table',
                ),
            ],
            untranslatable=[],
        )
        yaml = emit_yaml(spec)
        assert 'base_val' in yaml
        assert 'dax_ratio' in yaml
        assert 'DAX-Translated' in yaml

    def test_switch_measures_emitted(self):
        """SWITCH-decomposed measures appear in their own section."""
        spec = MetricViewSpec(
            fact_table_key='fact',
            source_table='cat.sch.tbl',
            view_name='test_view',
            comment='Test',
            joins=[],
            dimensions=[],
            measures=[
                TranslationResult(
                    measure_name='base_val', original_name='Base Val',
                    sql_expr='SUM(source.val)', is_translatable=True,
                    skip_reason='', dax_expression='', confidence='high', category='base',
                ),
                TranslationResult(
                    measure_name='sw_branch', original_name='SW Branch',
                    sql_expr='SUM(source.val) FILTER (WHERE source.type = \'A\')',
                    is_translatable=True, skip_reason='SWITCH branch: type=A',
                    dax_expression='', confidence='high', category='switch_decomposition',
                ),
            ],
            untranslatable=[],
        )
        yaml = emit_yaml(spec)
        assert 'sw_branch' in yaml
        assert 'SWITCH-Decomposed' in yaml

    def test_source_filter_emitted(self):
        """source_filter appears as filter: key in YAML."""
        spec = MetricViewSpec(
            fact_table_key='fact',
            source_table='cat.sch.tbl',
            view_name='test_view',
            comment='Test',
            joins=[],
            dimensions=[],
            measures=[
                TranslationResult(
                    measure_name='val', original_name='Val',
                    sql_expr='SUM(source.val)', is_translatable=True,
                    skip_reason='', dax_expression='', confidence='high', category='base',
                ),
            ],
            untranslatable=[],
            source_filter="source.active = 1",
        )
        yaml = emit_yaml(spec)
        assert 'filter:' in yaml

    def test_untranslatable_listed_as_comments(self):
        """Untranslatable measures appear as YAML comments."""
        spec = MetricViewSpec(
            fact_table_key='fact',
            source_table='cat.sch.tbl',
            view_name='test_view',
            comment='Test',
            joins=[],
            dimensions=[],
            measures=[
                TranslationResult(
                    measure_name='val', original_name='Val',
                    sql_expr='SUM(source.val)', is_translatable=True,
                    skip_reason='', dax_expression='', confidence='high', category='base',
                ),
            ],
            untranslatable=[
                TranslationResult(
                    measure_name='complex', original_name='Complex Measure',
                    sql_expr=None, is_translatable=False,
                    skip_reason='Cross-table reference unsupported',
                    dax_expression='CALCULATE(...)', confidence='none', category='cross_table',
                ),
            ],
        )
        yaml = emit_yaml(spec)
        assert 'Untranslatable' in yaml
        assert '# Complex Measure' in yaml


# ─── _check_dangerous_sql Tests ─────────────────────────────────────────────

from src.engines.crewai.tools.custom.metric_view_utils.yaml_emitter import _check_dangerous_sql


class TestCheckDangerousSql:
    """Security-critical tests for SQL injection prevention."""

    def test_safe_simple_aggregate(self):
        assert _check_dangerous_sql("SUM(source.amount)") is True

    def test_safe_division(self):
        assert _check_dangerous_sql("SUM(source.a) / NULLIF(SUM(source.b), 0)") is True

    def test_safe_measure_ref(self):
        assert _check_dangerous_sql("MEASURE(total_sales) - MEASURE(total_cost)") is True

    def test_safe_filter(self):
        assert _check_dangerous_sql("SUM(source.val) FILTER (WHERE source.type = 'A')") is True

    def test_safe_count_distinct(self):
        assert _check_dangerous_sql("COUNT(DISTINCT source.customer_id)") is True

    def test_safe_empty_string(self):
        assert _check_dangerous_sql("") is True

    def test_safe_none(self):
        assert _check_dangerous_sql(None) is True

    def test_safe_case_when(self):
        assert _check_dangerous_sql("CASE WHEN source.x > 0 THEN SUM(source.y) ELSE 0 END") is True

    def test_safe_coalesce(self):
        assert _check_dangerous_sql("COALESCE(SUM(source.val), 0)") is True

    def test_dangerous_drop_table(self):
        assert _check_dangerous_sql("DROP TABLE users") is False

    def test_dangerous_semicolon_drop(self):
        assert _check_dangerous_sql("SUM(x); DROP TABLE users") is False

    def test_dangerous_drop_view(self):
        assert _check_dangerous_sql("DROP VIEW my_view") is False

    def test_dangerous_drop_schema(self):
        assert _check_dangerous_sql("DROP SCHEMA public") is False

    def test_dangerous_drop_database(self):
        assert _check_dangerous_sql("DROP DATABASE production") is False

    def test_dangerous_delete_from(self):
        assert _check_dangerous_sql("DELETE FROM users") is False

    def test_dangerous_semicolon_delete(self):
        assert _check_dangerous_sql("; DELETE FROM orders") is False

    def test_dangerous_truncate(self):
        assert _check_dangerous_sql("TRUNCATE TABLE logs") is False

    def test_dangerous_alter_table(self):
        assert _check_dangerous_sql("ALTER TABLE users ADD COLUMN x INT") is False

    def test_dangerous_insert_into(self):
        assert _check_dangerous_sql("INSERT INTO users VALUES (1)") is False

    def test_dangerous_semicolon_insert(self):
        assert _check_dangerous_sql("; INSERT INTO logs") is False

    def test_dangerous_update_set(self):
        assert _check_dangerous_sql("UPDATE users SET role='admin'") is False

    def test_dangerous_semicolon_update(self):
        assert _check_dangerous_sql("; UPDATE users SET x=1") is False

    def test_dangerous_grant(self):
        assert _check_dangerous_sql("GRANT ALL ON schema TO user") is False

    def test_dangerous_revoke(self):
        assert _check_dangerous_sql("REVOKE SELECT ON table FROM user") is False

    def test_dangerous_create_user(self):
        assert _check_dangerous_sql("CREATE USER hacker WITH PASSWORD 'x'") is False

    def test_exec_parens_limitation(self):
        """EXEC(...) is not caught due to trailing \\b in regex after '(' — known limitation.

        EXEC without parens (e.g. EXEC sp_executesql) is not caught
        because it could be a legitimate identifier prefix.
        """
        assert _check_dangerous_sql("EXEC sp_executesql") is True

    def test_exec_parens_caught(self):
        """EXEC('cmd') is caught — dangerous stored procedure execution."""
        assert _check_dangerous_sql("EXEC('cmd')") is False

    def test_execute_parens_caught(self):
        """EXECUTE('...') is caught — dangerous dynamic SQL execution."""
        assert _check_dangerous_sql("EXECUTE('SELECT 1')") is False

    def test_dangerous_xp_cmdshell(self):
        assert _check_dangerous_sql("xp_cmdshell 'dir'") is False

    def test_dangerous_union_info_schema(self):
        assert _check_dangerous_sql(
            "UNION SELECT table_name FROM information_schema"
        ) is False

    def test_dangerous_case_insensitive(self):
        assert _check_dangerous_sql("drop table Users") is False
        assert _check_dangerous_sql("DELETE from Orders") is False
        assert _check_dangerous_sql("Insert Into logs VALUES (1)") is False


# ─── _check_metadata_limits Tests ───────────────────────────────────────────

from src.engines.crewai.tools.custom.metric_view_utils.yaml_emitter import _check_metadata_limits


class TestCheckMetadataLimits:
    def test_no_warnings_small_spec(self, basic_spec):
        warnings = _check_metadata_limits(basic_spec)
        assert warnings == []

    def test_too_many_measures(self):
        measures = [
            TranslationResult(
                measure_name=f'm_{i}', original_name=f'M {i}',
                sql_expr='SUM(source.x)', is_translatable=True,
                skip_reason='', dax_expression='', confidence='high', category='base',
            )
            for i in range(501)
        ]
        spec = MetricViewSpec(
            fact_table_key='f', source_table='c.s.t', view_name='v',
            comment='c', joins=[], dimensions=[], measures=measures, untranslatable=[],
        )
        warnings = _check_metadata_limits(spec)
        assert any('measures' in w.lower() for w in warnings)

    def test_too_many_dimensions(self):
        dims = [{'name': f'd_{i}', 'expr': f'source.d_{i}'} for i in range(201)]
        spec = MetricViewSpec(
            fact_table_key='f', source_table='c.s.t', view_name='v',
            comment='c', joins=[], dimensions=dims, measures=[], untranslatable=[],
        )
        warnings = _check_metadata_limits(spec)
        assert any('dimensions' in w.lower() for w in warnings)

    def test_comment_too_long(self):
        spec = MetricViewSpec(
            fact_table_key='f', source_table='c.s.t', view_name='v',
            comment='x' * 4001, joins=[], dimensions=[], measures=[], untranslatable=[],
        )
        warnings = _check_metadata_limits(spec)
        assert any('comment' in w.lower() for w in warnings)


# ─── YAML formatting helper tests ───────────────────────────────────────────

from src.engines.crewai.tools.custom.metric_view_utils.yaml_emitter import (
    _yaml_scalar, _yaml_needs_quoting, _yaml_val, _clean_filter_prefixes,
)


class TestYamlScalar:
    def test_empty(self):
        assert _yaml_scalar('') == "''"

    def test_plain(self):
        assert _yaml_scalar('hello') == 'hello'

    def test_special_chars_quoted(self):
        result = _yaml_scalar('key: value')
        assert result.startswith('"') or result.startswith("'")

    def test_backtick_single_quoted(self):
        result = _yaml_scalar('`column`')
        assert result == "'`column`'"

    def test_multiline_block(self):
        result = _yaml_scalar('line1\nline2')
        assert result.startswith('|-')

    def test_double_quote_passthrough(self):
        """Double quotes alone don't trigger quoting — they're not in the special-char list."""
        result = _yaml_scalar('say "hello"')
        assert result == 'say "hello"'

    def test_colon_with_quotes_escaped(self):
        """A value with both colon and double quotes gets escaped properly."""
        result = _yaml_scalar('key: say "hello"')
        assert result.startswith('"')
        assert '\\"' in result


class TestYamlNeedsQuoting:
    def test_empty(self):
        assert _yaml_needs_quoting('') is True

    def test_plain(self):
        assert _yaml_needs_quoting('hello') is False

    def test_colon(self):
        assert _yaml_needs_quoting('key: val') is True

    def test_hash(self):
        assert _yaml_needs_quoting('# comment') is True

    def test_leading_space(self):
        assert _yaml_needs_quoting(' hello') is True

    def test_trailing_space(self):
        assert _yaml_needs_quoting('hello ') is True


class TestYamlVal:
    def test_safe_value(self):
        assert _yaml_val('hello') == 'hello'

    def test_colon_quoted(self):
        result = _yaml_val('key: val')
        assert result.startswith('"')


class TestCleanFilterPrefixes:
    def test_strip_source_prefix(self):
        result = _clean_filter_prefixes(
            "SUM(x) FILTER (WHERE source.type = 'A')",
            fact_table_key='fact',
        )
        assert "type = 'A'" in result
        assert 'source.type' not in result

    def test_strip_fact_table_prefix(self):
        result = _clean_filter_prefixes(
            "SUM(x) FILTER (WHERE fact.type = 'A')",
            fact_table_key='fact',
        )
        assert "type = 'A'" in result
        assert 'fact.type' not in result

    def test_deduplicate_and_conditions(self):
        result = _clean_filter_prefixes(
            "SUM(x) FILTER (WHERE source.a = 1 AND source.a = 1)",
            fact_table_key='fact',
        )
        assert result.count('a = 1') == 1

    def test_no_filter_passthrough(self):
        expr = "SUM(source.val)"
        result = _clean_filter_prefixes(expr, fact_table_key='fact')
        assert result == expr
