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
        # The dangerous measure appears only in the not-emitted comments section
        assert 'Not emitted as measures' in yaml
        assert 'Evil' in yaml

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
        assert 'Not emitted as measures' in yaml
        assert 'Complex Measure' in yaml
        # The full original DAX is preserved in the comment block so a reviewer
        # can hand-translate without re-opening the PBIX.
        assert 'DAX:' in yaml
        assert 'CALCULATE(...)' in yaml

    def test_untranslatable_preserves_multiline_dax_as_valid_yaml(self):
        """Multi-line DAX is emitted as one comment line each, keeping the YAML
        parseable (a raw newline in a comment would otherwise break it)."""
        import yaml as _yaml
        multiline_dax = 'IF([Same Month Flag]="N",\n   MONTH(source.a),\n   MONTH(source.b))'
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
                    measure_name='cur_yr', original_name='Cur Yr',
                    sql_expr=None, is_translatable=False,
                    skip_reason='complex', dax_expression=multiline_dax,
                    confidence='none', category='cross_table',
                ),
            ],
        )
        yaml_out = emit_yaml(spec)
        # Every DAX line is present as its own comment line
        assert 'MONTH(source.a),' in yaml_out
        assert 'MONTH(source.b))' in yaml_out
        # And the document still parses (no bare newline leaked out of a comment)
        assert _yaml.safe_load(yaml_out) is not None


class TestMeasureUsageSurfacing:
    """referenced_by count surfaced in per-measure comments + sorted TODO block.

    Counts measure→measure references so reviewers prioritize high-impact gaps.
    """

    def _base(self, name, ref):
        return TranslationResult(
            measure_name=name, original_name=name,
            sql_expr=f'SUM(source.{name})', is_translatable=True,
            skip_reason='', dax_expression='', confidence='high', category='base',
            referenced_by=ref,
        )

    def test_comment_shows_count_when_referenced(self):
        spec = MetricViewSpec(
            fact_table_key='fact', source_table='cat.sch.tbl', view_name='v',
            comment='Test', joins=[], dimensions=[],
            measures=[self._base('epl', 1)], untranslatable=[],
        )
        yaml = emit_yaml(spec)
        assert 'referenced by 1 measure' in yaml
        # singular, not "1 measures"
        assert 'referenced by 1 measures' not in yaml

    def test_plural_wording(self):
        spec = MetricViewSpec(
            fact_table_key='fact', source_table='cat.sch.tbl', view_name='v',
            comment='Test', joins=[], dimensions=[],
            measures=[self._base('epl', 3)], untranslatable=[],
        )
        yaml = emit_yaml(spec)
        assert 'referenced by 3 measures' in yaml

    def test_no_suffix_at_zero(self):
        spec = MetricViewSpec(
            fact_table_key='fact', source_table='cat.sch.tbl', view_name='v',
            comment='Test', joins=[], dimensions=[],
            measures=[self._base('epl', 0)], untranslatable=[],
        )
        yaml = emit_yaml(spec)
        assert 'referenced by' not in yaml

    def test_untranslatable_block_sorted_desc_and_annotated(self):
        def _todo(name, ref):
            return TranslationResult(
                measure_name=name.lower(), original_name=name,
                sql_expr=None, is_translatable=False,
                skip_reason='TODO gap', dax_expression='SWITCH(...)',
                confidence='none', category='unassigned', referenced_by=ref,
            )
        spec = MetricViewSpec(
            fact_table_key='fact', source_table='cat.sch.tbl', view_name='v',
            comment='Test', joins=[], dimensions=[],
            measures=[self._base('anchor', 0)],
            untranslatable=[_todo('LowImpact', 1), _todo('HighImpact', 9),
                            _todo('MidImpact', 4)],
        )
        yaml = emit_yaml(spec)
        # Annotated with counts
        assert 'referenced by 9 measures' in yaml
        assert 'referenced by 1 measure' in yaml
        # Sorted highest-first: HighImpact appears before MidImpact before LowImpact
        i_high = yaml.index('HighImpact')
        i_mid = yaml.index('MidImpact')
        i_low = yaml.index('LowImpact')
        assert i_high < i_mid < i_low


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


class TestDimensionAndJoinDedup:
    """P2: duplicate dimension/join names are invalid UCMV YAML — must be deduped."""

    def _spec(self, dims, joins):
        return MetricViewSpec(
            fact_table_key='fact_test',
            source_table='cat.sch.test_table',
            view_name='fact_test_uc_metric_view',
            comment='c',
            joins=joins,
            dimensions=dims,
            measures=[TranslationResult(
                measure_name='m', original_name='M', sql_expr='SUM(source.x)',
                is_translatable=True, skip_reason='m', dax_expression='SUM(T[x])',
                confidence='high', category='base')],
            untranslatable=[],
        )

    def test_duplicate_dimension_names_collapsed(self):
        # Use join-alias exprs (no source. prefix) so they survive the phantom
        # dimension validation and we test dedup specifically.
        dims = [
            {'name': 'date', 'expr': 'dim_calendar.date', 'comment': 'Date'},
            {'name': 'date', 'expr': 'dim_calendar_dummy.date', 'comment': 'Dup'},
            {'name': 'date', 'expr': 'c_dim_calendar.date', 'comment': 'Dup2'},
            {'name': 'market', 'expr': 'dim_geo.market', 'comment': 'Market'},
        ]
        spec = self._spec(dims, [])
        out = emit_yaml(spec)
        # 'date' emitted exactly once, market once
        assert out.count('  - name: date') == 1
        assert out.count('  - name: market') == 1
        # first occurrence wins
        assert 'dim_calendar.date' in out
        assert 'dim_calendar_dummy.date' not in out

    def test_duplicate_join_names_collapsed(self):
        joins = [
            {'name': 'dim_plant', 'source': 'cat.sch.plant_a', 'on': 'source.p = dim_plant.p'},
            {'name': 'dim_plant', 'source': 'cat.sch.plant_b', 'on': 'source.p = dim_plant.p'},
        ]
        spec = self._spec([{'name': 'region', 'expr': 'source.region'}], joins)
        out = emit_yaml(spec)
        assert out.count('- name: dim_plant') == 1
        assert 'cat.sch.plant_a' in out  # first wins


class TestDimMeasureNameCollision:
    """P7: a UCMV cannot have a dimension and a measure with the same name."""

    def test_colliding_dimension_dropped(self):
        spec = MetricViewSpec(
            fact_table_key='fact_x',
            source_table='cat.sch.t',
            view_name='fact_x_uc_metric_view',
            comment='c',
            joins=[],
            dimensions=[
                {'name': 'kbi_value', 'expr': 'source.kbi_value'},   # collides with measure
                {'name': 'region', 'expr': 'dim_geo.region'},         # keep
            ],
            measures=[TranslationResult(
                measure_name='kbi_value', original_name='KBI Value',
                sql_expr='SUM(source.kbi_value)', is_translatable=True,
                skip_reason='kbi', dax_expression='SUM(T[kbi_value])',
                confidence='high', category='base')],
            untranslatable=[],
        )
        out = emit_yaml(spec)
        # kbi_value appears once, as a measure (SUM), not as a bare dimension
        assert out.count('- name: kbi_value') == 1
        assert 'SUM(source.kbi_value)' in out
        # region dimension survives
        assert '- name: region' in out


class TestUntranslatableCategorization:
    """Not-emitted measures are grouped by a clear human category + why (PROP #19)."""

    def _emit(self, untranslatable):
        spec = MetricViewSpec(
            fact_table_key='f', source_table='c.s.t', view_name='v', comment='c',
            joins=[], dimensions=[],
            measures=[TranslationResult(
                measure_name='base', original_name='base', sql_expr='SUM(source.x)',
                is_translatable=True, skip_reason='', dax_expression='', confidence='high',
                category='base')],
            untranslatable=untranslatable,
        )
        return emit_yaml(spec)

    def _u(self, name, dax, reason='no matching pattern'):
        return TranslationResult(
            measure_name=name.lower(), original_name=name, sql_expr=None,
            is_translatable=False, skip_reason=reason, dax_expression=dax,
            confidence='none', category='unassigned')

    def test_prior_year_grouped_with_reason(self):
        y = self._emit([self._u('NSR PY', 'CALCULATE(SUM(t[nsr]), SAMEPERIODLASTYEAR(cal[d]))')])
        assert '[prior-year time-intelligence]' in y
        assert 'NSR PY' in y
        assert 'not expressible in a static' in y

    def test_dynamic_selector_grouped(self):
        y = self._emit([self._u('Mega', 'var x=SELECTEDVALUE(m[k]) return SWITCH(TRUE(), x=1, [A])')])
        assert '[dynamic KPI selector]' in y

    def test_display_artifact_grouped(self):
        y = self._emit([self._u('CF_vs_%_Color', 'KBI_Display_calculate')])
        assert '[display artifact]' in y

    def test_complex_dax_bucket(self):
        y = self._emit([self._u('Weird', 'SOMEFUNC(x, y, z)')])
        assert 'needs manual translation' in y

    # ── Construct-specific guidance (Tier 3): honest unlock or honest skip ────

    def test_treatas_dispatch_grouped(self):
        y = self._emit([self._u('KPI Dispatch',
            'CALCULATE([M], TREATAS({kbiName}, T[Name]))')])
        assert '[disconnected-slicer dispatch (TREATAS)]' in y
        assert 'display-layer' in y and 'no source-view unlock' in y

    def test_lookupvalue_label_grouped(self):
        y = self._emit([self._u('Title',
            'LOOKUPVALUE(p[Parameter], p[Field], SELECTEDVALUE(p[Field]))')])
        assert '[parameter/label lookup (LOOKUPVALUE)]' in y
        assert 'join (RELATED)' in y

    def test_topn_grouped(self):
        y = self._emit([self._u('TopPrice',
            'CALCULATE(SELECTEDVALUE(P[Price]), TOPN(1, SUMMARIZE(P, P[Price], "c", COUNTROWS(P)), [c], DESC))')])
        # SUMMARIZE also present, but TOPN guidance is what a reviewer needs first
        assert '[top-N row selection (TOPN)]' in y
        assert 'ROW_NUMBER' in y

    def test_allexcept_fixed_lod_grouped(self):
        y = self._emit([self._u('YearWeight',
            "CALCULATE(SUM(t[weight]), ALLEXCEPT(t, t[Year]))")])
        assert '[fixed-LOD (ALLEXCEPT)]' in y
        assert 'kept-column grain' in y

    def test_summarize_group_then_aggregate_grouped(self):
        y = self._emit([self._u('MatContr',
            'SUMX(SUMMARIZE(F, F[comp_code], F[material]), [x]*CALCULATE(SUM(F[s])))')])
        assert '[group-then-aggregate (SUMMARIZE/CALCULATETABLE)]' in y
        assert 'GROUP BY' in y and 'identity dimension' in y
