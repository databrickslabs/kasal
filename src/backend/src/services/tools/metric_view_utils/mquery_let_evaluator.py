"""M ``let`` block evaluator — resolves a narrow, common subset of Power Query M
(string-literal concatenation of PBI parameters and local variables) to a
literal string, without a general M interpreter.

Why this exists: some PBI models build a table's physical source (or its whole
native-query text) at model-open time from parameters rather than a literal —
e.g.::

    Object = "hub_product_" & Table_Version,
    FromClause = Catalog_Name & "." & Database & "." & Object,
    VersionFilter = if Table_Version = "v2" then " AND version = '" & Version_Type & "'" else "",
    NativeQuery = Value.NativeQuery(Data_Mesh, "select ... from "& FromClause &" WHERE ..." & VersionFilter, ...)

The physical table only exists after substituting each parameter's *current*
value (itself a separate named expression in the model — see
``extract_parameter_defaults``). Admin Scanner / Fabric TMDL both expose those
current values, so this is fully resolvable without an LLM — but only when
every step is one of the handful of shapes below. Anything else (a function
call we don't recognise, a condition we can't evaluate, a reference to an
unresolved name) makes the whole table return ``None`` — this never guesses; a
wrong physical table silently wires a bad source.
"""

from __future__ import annotations

import re

# A PBI parameter query: a literal string tagged as its own value, e.g.
#   "dc_datalake_prod_001" meta [IsParameterQuery = true, Type = "Text"]
_PARAMETER_RE = re.compile(
    r'^\s*"((?:[^"]|"")*)"\s*meta\s*\[[^\]]*IsParameterQuery\s*=\s*true',
    re.IGNORECASE | re.DOTALL,
)


def extract_parameter_defaults(expressions: dict[str, str]) -> dict[str, str]:
    """Pull ``{name: current_value}`` for every M parameter query in ``expressions``.

    ``expressions`` is the ``{name: raw_M}`` map of a model's named/shared
    expressions (``generate_config.parse_admin_expressions`` /
    ``parse_tmdl_expressions``) — parameters are simply expressions tagged
    ``IsParameterQuery = true``, indistinguishable from ordinary shared
    queries except by that tag.
    """
    params: dict[str, str] = {}
    for name, expr in (expressions or {}).items():
        if not name or not isinstance(expr, str):
            continue
        m = _PARAMETER_RE.match(expr.strip())
        if m:
            params[name] = m.group(1).replace('""', '"')
    return params


def _skip_string(text: str, i: int) -> int:
    """Given ``text[i] == '"'``, return the index just past the closing quote
    (M doubles an embedded quote as ``""``, so ``"a""b"`` is one string)."""
    j = i + 1
    n = len(text)
    while j < n:
        if text[j] == '"':
            if j + 1 < n and text[j + 1] == '"':
                j += 2
                continue
            return j + 1
        j += 1
    return n  # unterminated — caller's depth tracking will simply run out


def _split_top_level(text: str, sep_chars: str) -> list[str]:
    """Split ``text`` on any of ``sep_chars`` that sit at depth 0, outside a
    quoted string. Depth tracks ``()``, ``{}``, ``[]`` — all M's grouping forms."""
    parts: list[str] = []
    depth = 0
    start = 0
    i = 0
    n = len(text)
    while i < n:
        ch = text[i]
        if ch == '"':
            i = _skip_string(text, i)
            continue
        if ch in "([{":
            depth += 1
        elif ch in ")]}":
            depth -= 1
        elif depth == 0 and ch in sep_chars:
            parts.append(text[start:i])
            start = i + 1
        i += 1
    parts.append(text[start:])
    return parts


def _find_top_level_word(text: str, word: str) -> int:
    """Index of the first top-level, whole-word occurrence of ``word`` in
    ``text`` (depth 0, outside a string), or -1."""
    depth = 0
    i = 0
    n = len(text)
    wl = len(word)
    while i < n:
        ch = text[i]
        if ch == '"':
            i = _skip_string(text, i)
            continue
        if ch in "([{":
            depth += 1
        elif ch in ")]}":
            depth -= 1
        elif depth == 0 and text[i : i + wl] == word:
            before_ok = i == 0 or not (text[i - 1].isalnum() or text[i - 1] == "_")
            after_idx = i + wl
            after_ok = after_idx >= n or not (
                text[after_idx].isalnum() or text[after_idx] == "_"
            )
            if before_ok and after_ok:
                return i
        i += 1
    return -1


def _extract_let_block(mquery: str) -> tuple[list[str], str] | None:
    """Split a *single, non-nested* ``let <bindings> in <expr>`` into its raw
    binding strings and the final expression. Returns ``None`` for anything
    else (no let block, or a nested let — deliberately not supported: two
    top-level ``let``s would make the naive first-``in``-wins split wrong)."""
    s = mquery.strip()
    if not re.match(r"^let\b", s, re.IGNORECASE):
        return None
    body = s[3:]  # past "let"
    # A second top-level "let" means nesting — bail rather than mis-split.
    if _find_top_level_word(body, "let") != -1:
        return None
    in_idx = _find_top_level_word(body, "in")
    if in_idx == -1:
        return None
    bindings_text = body[:in_idx]
    final_expr = body[in_idx + 2 :].strip()
    bindings = [b.strip() for b in _split_top_level(bindings_text, ",") if b.strip()]
    return bindings, final_expr


def _parse_binding(binding: str) -> tuple[str, str] | None:
    """Split ``Name = Expression`` on the first top-level bare ``=`` (not
    ``==``, ``<=``, ``>=``, ``<>``)."""
    depth = 0
    i = 0
    n = len(binding)
    while i < n:
        ch = binding[i]
        if ch == '"':
            i = _skip_string(binding, i)
            continue
        if ch in "([{":
            depth += 1
        elif ch in ")]}":
            depth -= 1
        elif depth == 0 and ch == "=":
            prev_ch = binding[i - 1] if i > 0 else ""
            next_ch = binding[i + 1] if i + 1 < n else ""
            if next_ch != "=" and prev_ch not in ("=", "<", ">", "!"):
                name = binding[:i].strip()
                if re.fullmatch(r'#?"?[\w ]+"?', name):
                    return name.strip('#"'), binding[i + 1 :].strip()
                return None
        i += 1
    return None


def _eval_string_literal(expr: str) -> str | None:
    if len(expr) >= 2 and expr[0] == '"' and _skip_string(expr, 0) == len(expr):
        return expr[1:-1].replace('""', '"')
    return None


def _eval_expr(expr: str, env: dict[str, str | None]) -> str | None:
    """Evaluate the narrow expression grammar this module supports. ``None``
    on anything unrecognised or referencing an unresolved name — never guess."""
    e = expr.strip()
    if not e:
        return None

    lit = _eval_string_literal(e)
    if lit is not None:
        return lit

    # if <cond> then <A> else <B> — cond limited to `<expr> = <expr>` so it
    # can be evaluated with the same machinery (see _eval_condition).
    if_idx = _find_top_level_word(e, "if")
    if if_idx == 0:
        then_idx = _find_top_level_word(e, "then")
        else_idx = _find_top_level_word(e, "else")
        if then_idx != -1 and else_idx != -1 and then_idx < else_idx:
            cond = e[2:then_idx].strip()
            branch_a = e[then_idx + 4 : else_idx].strip()
            branch_b = e[else_idx + 4 :].strip()
            verdict = _eval_condition(cond, env)
            if verdict is None:
                return None
            return _eval_expr(branch_a if verdict else branch_b, env)
        return None

    # Text.From(<expr>) — our params/locals are already text; unwrap.
    tf_match = re.match(r"^Text\.From\s*\((.*)\)$", e, re.DOTALL)
    if tf_match:
        return _eval_expr(tf_match.group(1), env)

    # A & B & C ... — string concatenation, only if every operand resolves.
    amp_parts = _split_top_level(e, "&")
    if len(amp_parts) > 1:
        out = []
        for part in amp_parts:
            v = _eval_expr(part, env)
            if v is None:
                return None
            out.append(v)
        return "".join(out)

    # Bare identifier — look up in the environment (parameters + earlier
    # bindings). Unresolved / unknown → fail rather than guess.
    if re.fullmatch(r"[A-Za-z_]\w*", e):
        return env.get(e)

    return None


def _eval_condition(cond: str, env: dict[str, str | None]) -> bool | None:
    """Evaluate ``<expr> = <expr>`` (the only condition shape supported)."""
    eq_idx = _find_top_level_word(cond, "=")
    # '=' isn't a "word" by _find_top_level_word's alnum boundary rule, so
    # locate it directly instead, guarding against '==' / '<=' / '>=' / '!='.
    depth = 0
    i = 0
    n = len(cond)
    eq_idx = -1
    while i < n:
        ch = cond[i]
        if ch == '"':
            i = _skip_string(cond, i)
            continue
        if ch in "([{":
            depth += 1
        elif ch in ")]}":
            depth -= 1
        elif depth == 0 and ch == "=":
            prev_ch = cond[i - 1] if i > 0 else ""
            next_ch = cond[i + 1] if i + 1 < n else ""
            if next_ch != "=" and prev_ch not in ("=", "<", ">", "!"):
                eq_idx = i
                break
        i += 1
    if eq_idx == -1:
        return None
    left = _eval_expr(cond[:eq_idx], env)
    right = _eval_expr(cond[eq_idx + 1 :], env)
    if left is None or right is None:
        return None
    return left == right


def _extract_call_args(text: str, call_prefix_end: int) -> list[str] | None:
    """Given ``text`` and the index just past a call's opening ``(``, return
    the top-level comma-separated argument strings (the call's own closing
    paren excluded), or ``None`` if the parens never balance."""
    depth = 1
    i = call_prefix_end
    n = len(text)
    start = i
    args: list[str] = []
    while i < n:
        ch = text[i]
        if ch == '"':
            i = _skip_string(text, i)
            continue
        if ch in "([{":
            depth += 1
        elif ch in ")]}":
            depth -= 1
            if depth == 0:
                args.append(text[start:i])
                return args
        elif depth == 1 and ch == ",":
            args.append(text[start:i])
            start = i + 1
        i += 1
    return None  # unbalanced


def resolve_via_let_evaluation(mquery: str, known_params: dict[str, str]) -> str | None:
    """Resolve a parameter-driven ``Value.NativeQuery`` M source to literal SQL.

    Evaluates every ``let`` binding in order (seeding the environment with
    ``known_params``), then evaluates the SQL-text argument of whichever
    binding calls ``Value.NativeQuery`` — itself just another expression in
    the same grammar, so the same evaluator resolves it once every parameter
    and local variable it references has resolved. Returns ``None`` if no
    ``Value.NativeQuery`` call is found, or if anything along the way can't be
    confidently evaluated.
    """
    if not mquery or not isinstance(mquery, str) or not known_params:
        return None
    parsed = _extract_let_block(mquery)
    if not parsed:
        return None
    bindings, _final_expr = parsed

    env: dict[str, str | None] = dict(known_params)
    native_query_sql_arg: str | None = None

    for raw in bindings:
        pb = _parse_binding(raw)
        if not pb:
            continue
        name, raw_expr = pb

        nq_idx = _find_top_level_word(raw_expr, "Value.NativeQuery")
        if nq_idx != -1:
            paren_idx = raw_expr.find("(", nq_idx + len("Value.NativeQuery"))
            if paren_idx != -1:
                args = _extract_call_args(raw_expr, paren_idx + 1)
                if args and len(args) >= 2:
                    native_query_sql_arg = args[1]
            # A binding invoking Value.NativeQuery doesn't itself need to
            # resolve to a plain string — skip normal evaluation for it.
            continue

        env[name] = _eval_expr(raw_expr, env)

    if native_query_sql_arg is None:
        return None
    return _eval_expr(native_query_sql_arg, env)
