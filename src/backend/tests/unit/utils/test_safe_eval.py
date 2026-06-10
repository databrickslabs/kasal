"""
Unit tests for src.utils.safe_eval.

These guard the C1 fix: user-authored flow router/state expressions are
evaluated with a restricted AST walker instead of eval(), which must block the
introspection escapes that an empty __builtins__ does NOT prevent.
"""
import pytest

from src.utils.safe_eval import safe_eval, UnsafeExpressionError

# Bare names the flow engine permits (mirrors flow_builder._FLOW_CONDITION_CALLS).
CALLS = frozenset({"int", "float", "str", "bool", "len", "abs", "min", "max"})


class TestLegitimateExpressions:
    """Expressions the flow engine legitimately evaluates must keep working."""

    @pytest.mark.parametrize(
        "expr,names,expected",
        [
            ("number > 40", {"number": 43}, True),
            ("number > 40", {"number": 10}, False),
            ("success == True", {"success": True}, True),
            ("state['x'] == 'y'", {"state": {"x": "y"}}, True),
            ("state.get('x') > 5", {"state": {"x": 7}}, True),
            ("status in ['done', 'ok']", {"status": "ok"}, True),
            ("a > 1 and b < 10", {"a": 5, "b": 3}, True),
            ("not failed", {"failed": False}, True),
            ("name.startswith('ab')", {"name": "abc"}, True),
            ("score * 2 + 1", {"score": 4}, 9),
            ("int(value) >= 3", {"value": "5", "int": int}, True),
            ("len(items) > 0", {"items": [1, 2, 3], "len": len}, True),
            ("max(a, b)", {"a": 2, "b": 9, "max": max}, 9),
        ],
    )
    def test_evaluates(self, expr, names, expected):
        assert safe_eval(expr, names, allowed_call_names=CALLS) == expected


class TestBlocksRce:
    """Every introspection / code-execution escape must be rejected."""

    @pytest.mark.parametrize(
        "expr,names",
        [
            ("().__class__.__bases__[0].__subclasses__()", {}),
            ("().__class__", {}),
            ("__import__('os')", {}),
            ("__import__('os').system('id')", {}),
            ("(1).__class__.__mro__", {}),
            ("''.__class__.__mro__[1].__subclasses__()", {}),
            ("state.__class__", {"state": {}}),
            ("'{0.__class__}'.format(state)", {"state": {}}),  # format-string attr access
            ("'{0.__class__.__init__.__globals__}'.format(s)", {"s": ""}),
            (
                "().__class__.__bases__[0].__subclasses__()[40]('x', shell=True)",
                {},
            ),
            ("globals()", {}),
            ("eval('1')", {}),
            ("exec('x=1')", {}),
            ("open('/etc/passwd')", {}),
            ("lambda: 1", {}),
            ("[x for x in [1, 2, 3]]", {}),
            ("state.pop('x')", {"state": {"x": 1}}),  # mutating method not allow-listed
            ("state.update({'a': 1})", {"state": {}}),  # not allow-listed
            ("s.format(x=1)", {"s": "{x}"}),  # format not allow-listed
        ],
    )
    def test_blocked(self, expr, names):
        with pytest.raises(
            (UnsafeExpressionError, SyntaxError, NameError, AttributeError, TypeError, KeyError)
        ):
            safe_eval(expr, names, allowed_call_names=CALLS)


class TestCallGating:
    """Only explicitly-approved bare names may be called."""

    def test_unapproved_call_name_rejected(self):
        # 'int' present in names but NOT in allowed_call_names -> rejected.
        with pytest.raises(UnsafeExpressionError):
            safe_eval("int(x)", {"x": "5", "int": int}, allowed_call_names=frozenset())

    def test_dunder_name_rejected(self):
        with pytest.raises(UnsafeExpressionError):
            safe_eval("__import__", {"__import__": __import__})
