"""Tests for path-aware router condition lookup.

The router UI cannot express anything but ``state.get("<field>", "")``, so every
case here is written the way the UI would emit it unless it is explicitly
testing a hand-authored path.
"""

import pytest

from src.services.flow_builder.conversation.state_model import build_state_model
from src.services.flow_builder.modules.flow_conditions import (
    MISSING,
    ConditionState,
    MatchList,
    find_leaf,
    resolve_path,
    state_snapshot,
)
from src.utils.safe_eval import safe_eval

_CALLS = frozenset({"int", "float", "str", "bool", "len", "abs", "min", "max"})

CATEGORIES = ["technology", "politics", "world news", "business", "sports"]


def articles(count: int = 29):
    return [
        {"id": i, "title": f"t{i}", "category": CATEGORIES[i % 5], "score": i}
        for i in range(count)
    ]


def evaluate(state, condition):
    context = {
        "state": ConditionState(state),
        "int": int,
        "float": float,
        "str": str,
        "bool": bool,
        "len": len,
        "abs": abs,
        "min": min,
        "max": max,
    }
    return safe_eval(condition, context, allowed_call_names=_CALLS)


class TestExactKeyStillWins:
    """A flat schema must behave exactly as it did before this module existed."""

    def test_top_level_key_is_returned_unchanged(self):
        assert evaluate(
            {"category": "politics"}, 'state.get("category", "") == "politics"'
        )

    def test_top_level_key_beats_a_nested_one_of_the_same_name(self):
        state = {"category": "sports", "inner": {"category": "politics"}}
        assert evaluate(state, 'state.get("category", "") == "sports"')
        assert not evaluate(state, 'state.get("category", "") == "politics"')

    @pytest.mark.parametrize("empty", [None, "", [], {}])
    def test_a_declared_but_unfilled_key_falls_through(self, empty):
        """The observed shape when structured output failed to parse.

        Treating it as an answer is what kept the real value, one level down,
        invisible.
        """
        state = {"category": empty, "inner": {"category": "politics"}}
        assert evaluate(state, 'state.get("category", "") == "politics"')


class TestNestedObject:
    def test_leaf_is_found_through_an_object(self):
        state = {"classification": {"category": "politics", "confidence": 0.91}}
        assert evaluate(state, 'state.get("category", "") == "politics"')
        assert evaluate(state, 'state.get("confidence", 0) > 0.5')

    def test_explicit_dotted_path(self):
        state = {"classification": {"category": "politics"}}
        assert evaluate(state, 'state.get("classification.category", "") == "politics"')

    def test_the_hand_written_workaround_still_works(self):
        state = {"classification": {"category": "politics"}}
        assert evaluate(
            state, 'state.get("classification", {}).get("category", "") == "politics"'
        )


class TestListOfObjects:
    """The 29-article case, asked through the UI's ordinary operators."""

    def test_equals_means_any_element_matches(self):
        assert evaluate(
            {"articles": articles()}, 'state.get("category", "") == "politics"'
        )

    def test_equals_is_false_when_no_element_matches(self):
        assert not evaluate(
            {"articles": articles()}, 'state.get("category", "") == "weather"'
        )

    def test_contains_operator(self):
        assert evaluate(
            {"articles": articles()}, '"politics" in state.get("category", "")'
        )

    def test_starts_with_operator(self):
        assert evaluate(
            {"articles": articles()}, 'state.get("category", "").startswith("pol")'
        )

    def test_numeric_comparison_across_the_list(self):
        state = {"articles": articles()}
        assert evaluate(state, 'state.get("score", "") > 25')
        assert not evaluate(state, 'state.get("score", "") > 100')

    def test_explicit_projection_path(self):
        assert evaluate(
            {"articles": articles()},
            'state.get("articles[].category", "") == "politics"',
        )

    def test_explicit_index(self):
        assert evaluate(
            {"articles": articles()},
            'state.get("articles[0].category", "") == "technology"',
        )

    def test_many_elements_of_one_list_are_gathered_not_ambiguous(self):
        """29 elements each carrying `category` is ONE path, not 29 candidates."""
        value, note = find_leaf({"articles": articles()}, "category")
        assert isinstance(value, MatchList)
        assert len(value) == 29
        assert note == "articles.category[]"


class TestDeeperNesting:
    """Real output nests further than one level: a report of sections of items,
    an order of line items. Every operator must keep meaning "any, at any depth".
    """

    ORDERS = {
        "orders": [
            {"lines": [{"sku": "ABC", "qty": 3}, {"sku": "BXX", "qty": 1}]},
            {"lines": [{"sku": "CZZ", "qty": 99}]},
        ]
    }

    def test_objects_nest_to_any_depth(self):
        state = {"a": {"b": {"c": {"d": "politics"}}}}
        assert evaluate(state, 'state.get("a.b.c.d", "") == "politics"')

    def test_a_list_with_objects_below_it(self):
        state = {"articles": [{"meta": {"category": "politics"}}]}
        assert evaluate(
            state, 'state.get("articles[].meta.category", "") == "politics"'
        )

    @pytest.mark.parametrize(
        "condition,expected",
        [
            ('state.get("orders[].lines[].sku", "") == "CZZ"', True),
            ('state.get("orders[].lines[].sku", "") == "NOPE"', False),
            ('state.get("orders[].lines[].sku", "") != "NOPE"', True),
            ('state.get("orders[].lines[].sku", "") != "CZZ"', False),
            ('"CZ" in state.get("orders[].lines[].sku", "")', True),
            ('state.get("orders[].lines[].sku", "").startswith("AB")', True),
            ('state.get("orders[].lines[].sku", "").endswith("ZZ")', True),
            ('state.get("orders[].lines[].qty", "") > 50', True),
            ('state.get("orders[].lines[].qty", "") > 500', False),
        ],
    )
    def test_a_list_inside_a_list(self, condition, expected):
        assert evaluate(self.ORDERS, condition) is expected

    def test_leaf_search_flattens_through_two_lists(self):
        assert evaluate(self.ORDERS, 'state.get("sku", "") == "CZZ"')


class TestMatchListSemantics:
    def test_not_equals_means_no_element_matches(self):
        """A list subclass inherits list.__ne__, which is always True against a
        scalar. Without an explicit __ne__ this is an always-firing route."""
        gathered = MatchList(["sports", "politics"])
        assert (gathered != "politics") is False
        assert (gathered != "weather") is True

    def test_comparisons_are_total_over_a_mixed_list(self):
        """A raising comparison propagates out of safe_eval and the router
        swallows it — reinstating the silent miss this module removes."""
        assert MatchList([3, None, "9", 12]) > 5
        assert not MatchList([None, "x"]) > 5

    def test_contains_matches_element_or_substring(self):
        assert "politics" in MatchList(["sports", "politics"])
        assert "poli" in MatchList(["sports", "politics"])
        assert "weather" not in MatchList(["sports", "politics"])

    def test_is_unhashable_because_it_defines_eq(self):
        with pytest.raises(TypeError):
            hash(MatchList(["a"]))


class TestAmbiguityAndBounds:
    def test_two_distinct_paths_do_not_resolve(self):
        state = {"a": {"category": "x"}, "b": {"category": "politics"}}
        assert not evaluate(state, 'state.get("category", "") == "politics"')

    def test_ambiguity_is_resolvable_with_an_explicit_path(self):
        state = {"a": {"category": "x"}, "b": {"category": "politics"}}
        assert evaluate(state, 'state.get("b.category", "") == "politics"')

    def test_ambiguity_note_names_both_paths(self):
        _, note = find_leaf({"a": {"c": 1}, "b": {"c": 2}}, "c")
        assert note == "ambiguous: a.c, b.c"

    def test_a_self_referencing_state_terminates(self):
        state = {"category": None}
        state["self"] = state
        assert not evaluate(state, 'state.get("category", "") == "x"')

    def test_a_missing_path_returns_the_default(self):
        assert resolve_path({"a": 1}, "b.c.d") is MISSING


class TestStateSnapshot:
    def test_survives_items_being_shadowed_by_a_list(self):
        """merge_parsed_json writes state["items"] = [...] on any array output.

        On a typed state that shadows the items() method, and the next call
        raised TypeError, which escaped the router and stopped the flow.
        """
        state = build_state_model(
            {"type": "object", "properties": {"c": {"type": "string"}}}
        )()
        state["items"] = [{"category": "politics"}]

        snapshot = state_snapshot(state)

        assert "items" in snapshot
        assert snapshot["items"] == [{"category": "politics"}]

    def test_reads_a_plain_dict(self):
        assert state_snapshot({"a": 1}) == {"a": 1}

    def test_returns_empty_for_something_unreadable(self):
        assert state_snapshot(object()) == {}


class TestBackwardCompatibleAccessForms:
    """The five forms pinned by TestConditionsKeepWorking must survive the wrap.

    A Mapping wrapper silently breaks the attribute form, which is why
    ConditionState is a transparent proxy instead.
    """

    SCHEMA = {
        "type": "object",
        "properties": {
            "has_results": {"type": "boolean"},
            "region": {"type": "string"},
        },
    }

    @pytest.fixture()
    def state(self):
        built = build_state_model(self.SCHEMA)()
        built["has_results"] = True
        built["region"] = "DACH"
        return built

    @pytest.mark.parametrize(
        "condition",
        [
            'state.get("has_results", "") == True',
            'state["has_results"] == True',
            "state.has_results == True",
            '"has_results" in state',
            "len(state.keys()) > 0",
            'state.region == "DACH"',
        ],
    )
    def test_every_access_form_evaluates(self, state, condition):
        assert evaluate(state, condition) is True

    def test_repr_still_shows_state_contents(self, state):
        """The router logs "State contents: {...}" unconditionally."""
        assert "DACH" in repr(ConditionState(state))


class TestTolerantMerge:
    def test_an_undeclared_key_does_not_raise(self):
        """DictLikeState.update raises on a field it does not declare, and that
        raise used to escape build_eval_context and stop the flow."""
        state = build_state_model(
            {"type": "object", "properties": {"category": {"type": "string"}}}
        )()
        wrapped = ConditionState(state)

        wrapped.update({"articles": articles(), "category": "politics"})

        assert wrapped.get("category", "") == "politics"

    def test_the_undeclared_value_is_still_addressable(self):
        state = build_state_model(
            {"type": "object", "properties": {"category": {"type": "string"}}}
        )()
        wrapped = ConditionState(state)
        wrapped.update({"articles": articles()})

        assert wrapped.get("articles[].category", "") == "politics"


class TestDiagnostics:
    def test_describe_names_the_paths_that_would_have_worked(self):
        described = ConditionState(
            {"classification": {"category": "politics"}, "articles": articles(3)}
        ).describe()

        assert "classification.category" in described
        assert "articles" in described

    def test_a_failed_lookup_is_recorded(self):
        wrapped = ConditionState({"a": {"c": 1}, "b": {"c": 2}})
        wrapped.get("c", "")

        assert "ambiguous" in wrapped.misses["c"]


class TestCaseInsensitiveComparison:
    """A router routes on a label the MODEL chose, and models are inconsistent
    about capitalisation. The same classify step emitted `politics` one run and
    `Politics` the next; the second failed `== "politics"` and the branch never
    ran (execution afc87a6e)."""

    CAPS = {
        "classification": [
            {"category": "Politics"},
            {"category": "Sports"},
            {"category": "Technology"},
        ]
    }

    @pytest.mark.parametrize(
        "condition,expected",
        [
            ('state.get("classification[].category", "") == "politics"', True),
            ('state.get("classification[].category", "") == "POLITICS"', True),
            ('state.get("classification[].category", "") == "weather"', False),
            ('state.get("classification[].category", "") != "weather"', True),
            ('state.get("classification[].category", "") != "politics"', False),
            ('"POLIT" in state.get("classification[].category", "")', True),
            ('state.get("classification[].category", "").startswith("pol")', True),
            ('state.get("classification[].category", "").endswith("ICS")', True),
        ],
    )
    def test_across_a_projection(self, condition, expected):
        assert evaluate(self.CAPS, condition) is expected

    @pytest.mark.parametrize(
        "condition,expected",
        [
            ('state.get("category", "") == "politics"', True),
            ('state.get("category", "") == "Politics"', True),
            ('state.get("category", "") == "sports"', False),
        ],
    )
    def test_on_a_plain_scalar(self, condition, expected):
        assert evaluate({"category": "Politics"}, condition) is expected

    def test_non_strings_are_left_alone(self):
        assert evaluate({"ok": True}, 'state.get("ok", "") == True')
        assert evaluate({"n": 7}, 'state.get("n", 0) > 5')

    def test_the_hash_contract_holds(self):
        """__eq__ is overridden, so __hash__ must fold too or a dict lookup
        would disagree with a comparison."""
        from src.services.flow_builder.modules.flow_conditions import MatchStr

        assert MatchStr("Politics") == "politics"
        assert hash(MatchStr("Politics")) == hash(MatchStr("politics"))
