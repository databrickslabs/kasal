"""
Safe expression evaluation for user-authored flow expressions.

This module replaces unsafe ``eval()`` of stored flow router conditions and
state-write expressions.  Those expressions are authored by an authenticated
user when they create/edit a Flow and are persisted in the database, so they
are *untrusted input* that is later evaluated server-side inside the flow
execution process (which holds Databricks SPN/OBO credentials).

``eval(expr, {"__builtins__": {}}, ctx)`` is NOT a sandbox: an empty
``__builtins__`` does nothing to stop attribute-introspection escapes such as
``().__class__.__bases__[0].__subclasses__()[N](...)`` which reach ``os`` /
``subprocess`` and give remote code execution.

Instead we parse the expression to an AST and walk it, permitting only a
whitelist of node types: literals, names (resolved against a caller-supplied
context), boolean / comparison / arithmetic operators, indexing, and a small
set of safe calls.  The two rules that close the RCE vectors are:

* **No dunder / private access** — any attribute or name containing a leading
  underscore is rejected, which blocks ``__class__``, ``__subclasses__``,
  ``__globals__``, ``__import__``, ``__builtins__`` and friends.
* **Calls are restricted** — only caller-approved bare names (e.g. ``int``,
  ``len``) and an allow-list of safe, non-mutating methods (e.g. ``dict.get``,
  ``str.startswith``) may be called.  ``str.format``/``format_map`` are *not*
  on the allow-list because their format-mini-language can perform attribute
  access (``"{0.__class__}"``) at runtime, bypassing the AST checks.

Lambdas, comprehensions, walrus assignments, generators, f-strings and
starred/await/yield expressions are all rejected.
"""

import ast
import operator
from typing import Any, Dict, FrozenSet, Optional

__all__ = ["safe_eval", "UnsafeExpressionError"]


class UnsafeExpressionError(ValueError):
    """Raised when an expression uses a construct that is not allowed."""


# Binary arithmetic operators that are safe to evaluate.
_BIN_OPS = {
    ast.Add: operator.add,
    ast.Sub: operator.sub,
    ast.Mult: operator.mul,
    ast.Div: operator.truediv,
    ast.FloorDiv: operator.floordiv,
    ast.Mod: operator.mod,
    ast.Pow: operator.pow,
}

_UNARY_OPS = {
    ast.Not: operator.not_,
    ast.USub: operator.neg,
    ast.UAdd: operator.pos,
}

_CMP_OPS = {
    ast.Eq: operator.eq,
    ast.NotEq: operator.ne,
    ast.Lt: operator.lt,
    ast.LtE: operator.le,
    ast.Gt: operator.gt,
    ast.GtE: operator.ge,
    ast.In: lambda a, b: a in b,
    ast.NotIn: lambda a, b: a not in b,
    ast.Is: operator.is_,
    ast.IsNot: operator.is_not,
}

# Non-mutating, RCE-safe methods that may be invoked on values pulled from the
# evaluation context (dicts/strings/lists produced by parsing crew output).
# Deliberately excludes ``format``/``format_map`` (format-string attribute
# access) and anything dunder/private.
_ALLOWED_METHODS: FrozenSet[str] = frozenset(
    {
        # mapping access
        "get",
        "keys",
        "values",
        "items",
        # string predicates / transforms
        "startswith",
        "endswith",
        "lower",
        "upper",
        "strip",
        "lstrip",
        "rstrip",
        "title",
        "capitalize",
        "split",
        "rsplit",
        "splitlines",
        "replace",
        "join",
        "find",
        "rfind",
        "count",
        "index",
        "isdigit",
        "isnumeric",
        "isalpha",
        "isalnum",
        "zfill",
    }
)

# Cap on AST size to avoid pathological expressions.
_MAX_NODES = 2000


def _ensure_safe_name(name: str) -> None:
    if name.startswith("_"):
        raise UnsafeExpressionError(
            f"Access to private/dunder identifier '{name}' is not allowed"
        )


def _eval_node(
    node: ast.AST, names: Dict[str, Any], allowed_call_names: FrozenSet[str]
) -> Any:
    # Literals -------------------------------------------------------------
    if isinstance(node, ast.Constant):
        return node.value

    # Names ----------------------------------------------------------------
    if isinstance(node, ast.Name):
        _ensure_safe_name(node.id)
        if node.id in names:
            return names[node.id]
        raise UnsafeExpressionError(f"Unknown name '{node.id}'")

    # Boolean operators (short-circuit) ------------------------------------
    if isinstance(node, ast.BoolOp):
        if isinstance(node.op, ast.And):
            result: Any = True
            for value in node.values:
                result = _eval_node(value, names, allowed_call_names)
                if not result:
                    return result
            return result
        if isinstance(node.op, ast.Or):
            result = False
            for value in node.values:
                result = _eval_node(value, names, allowed_call_names)
                if result:
                    return result
            return result
        raise UnsafeExpressionError("Unsupported boolean operator")

    # Unary operators ------------------------------------------------------
    if isinstance(node, ast.UnaryOp):
        op = _UNARY_OPS.get(type(node.op))
        if op is None:
            raise UnsafeExpressionError(
                f"Unsupported unary operator: {type(node.op).__name__}"
            )
        return op(_eval_node(node.operand, names, allowed_call_names))

    # Binary operators -----------------------------------------------------
    if isinstance(node, ast.BinOp):
        op = _BIN_OPS.get(type(node.op))
        if op is None:
            raise UnsafeExpressionError(
                f"Unsupported binary operator: {type(node.op).__name__}"
            )
        left = _eval_node(node.left, names, allowed_call_names)
        right = _eval_node(node.right, names, allowed_call_names)
        return op(left, right)

    # Comparisons ----------------------------------------------------------
    if isinstance(node, ast.Compare):
        left = _eval_node(node.left, names, allowed_call_names)
        for op_node, comparator in zip(node.ops, node.comparators):
            op = _CMP_OPS.get(type(op_node))
            if op is None:
                raise UnsafeExpressionError(
                    f"Unsupported comparison: {type(op_node).__name__}"
                )
            right = _eval_node(comparator, names, allowed_call_names)
            if not op(left, right):
                return False
            left = right
        return True

    # Container literals ---------------------------------------------------
    if isinstance(node, ast.List):
        return [_eval_node(e, names, allowed_call_names) for e in node.elts]
    if isinstance(node, ast.Tuple):
        return tuple(_eval_node(e, names, allowed_call_names) for e in node.elts)
    if isinstance(node, ast.Set):
        return {_eval_node(e, names, allowed_call_names) for e in node.elts}
    if isinstance(node, ast.Dict):
        return {
            _eval_node(k, names, allowed_call_names): _eval_node(
                v, names, allowed_call_names
            )
            for k, v in zip(node.keys, node.values)
        }

    # Indexing / slicing ---------------------------------------------------
    if isinstance(node, ast.Subscript):
        value = _eval_node(node.value, names, allowed_call_names)
        key = _eval_node(node.slice, names, allowed_call_names)
        return value[key]
    if isinstance(node, ast.Slice):
        lower = (
            None
            if node.lower is None
            else _eval_node(node.lower, names, allowed_call_names)
        )
        upper = (
            None
            if node.upper is None
            else _eval_node(node.upper, names, allowed_call_names)
        )
        step = (
            None
            if node.step is None
            else _eval_node(node.step, names, allowed_call_names)
        )
        return slice(lower, upper, step)

    # Attribute reads (non-dunder only) ------------------------------------
    if isinstance(node, ast.Attribute):
        _ensure_safe_name(node.attr)
        value = _eval_node(node.value, names, allowed_call_names)
        return getattr(value, node.attr)

    # Calls (restricted) ---------------------------------------------------
    if isinstance(node, ast.Call):
        if any(isinstance(a, ast.Starred) for a in node.args):
            raise UnsafeExpressionError("Star unpacking is not allowed in calls")

        func = node.func
        if isinstance(func, ast.Name):
            _ensure_safe_name(func.id)
            if func.id not in allowed_call_names:
                raise UnsafeExpressionError(f"Call to '{func.id}' is not allowed")
            callee = names.get(func.id)
            if not callable(callee):
                raise UnsafeExpressionError(f"'{func.id}' is not callable")
        elif isinstance(func, ast.Attribute):
            _ensure_safe_name(func.attr)
            if func.attr not in _ALLOWED_METHODS:
                raise UnsafeExpressionError(f"Method '{func.attr}' is not allowed")
            target = _eval_node(func.value, names, allowed_call_names)
            callee = getattr(target, func.attr)
            if not callable(callee):
                raise UnsafeExpressionError(f"'{func.attr}' is not callable")
        else:
            raise UnsafeExpressionError("Only direct name and method calls are allowed")

        args = [_eval_node(a, names, allowed_call_names) for a in node.args]
        kwargs: Dict[str, Any] = {}
        for kw in node.keywords:
            if kw.arg is None:
                raise UnsafeExpressionError(
                    "Keyword (**) unpacking is not allowed in calls"
                )
            kwargs[kw.arg] = _eval_node(kw.value, names, allowed_call_names)
        return callee(*args, **kwargs)

    # Anything else is rejected -------------------------------------------
    raise UnsafeExpressionError(
        f"Unsupported expression element: {type(node).__name__}"
    )


def safe_eval(
    expression: str,
    names: Optional[Dict[str, Any]] = None,
    allowed_call_names: FrozenSet[str] = frozenset(),
) -> Any:
    """Safely evaluate a restricted Python expression.

    Args:
        expression: The expression string to evaluate.
        names: Mapping of identifier -> value available to the expression.
            Callables intended to be invokable must be referenced here AND
            named in ``allowed_call_names``.
        allowed_call_names: Set of bare names the expression is permitted to
            call (e.g. ``{"int", "float", "len"}``).  Method calls are governed
            separately by the internal method allow-list.

    Returns:
        The evaluated result.

    Raises:
        UnsafeExpressionError: If the expression uses a disallowed construct.
        SyntaxError: If the expression is not valid Python.
        Exception: Any error raised by the underlying operation (e.g.
            ``KeyError`` for a missing dict key) is propagated so callers can
            handle it exactly as they did with ``eval``.
    """
    if names is None:
        names = {}
    if not isinstance(expression, str):
        raise UnsafeExpressionError("Expression must be a string")

    tree = ast.parse(expression, mode="eval")

    node_count = sum(1 for _ in ast.walk(tree))
    if node_count > _MAX_NODES:
        raise UnsafeExpressionError("Expression is too complex")

    return _eval_node(tree.body, names, frozenset(allowed_call_names))
