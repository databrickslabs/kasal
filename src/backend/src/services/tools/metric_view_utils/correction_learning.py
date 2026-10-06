"""Learn reusable domain context from customer-corrected UCMVs.

The flywheel: Kasal generates UC Metric View YAML → the customer validates, fixes
and deploys it → we diff their corrected YAML against our original output for the
same views → an LLM distils the RECURRING corrections (not one-offs) into a
reusable, human-reviewable domain-context README — the same free text the
generator's ``domain_context`` field consumes. The next generation then starts
closer to what the customer actually wants.

This module is the pure core: parsing, measure-level diffing, and prompt
assembly. The LLM call is injected (``llm_fn``) so the distillation is unit-tested
with a fake and the module never imports an orchestrator or hits a network.
"""

from __future__ import annotations

import difflib
from typing import Any, Callable, Dict, List, Optional, Tuple

import yaml


def _load_view(yaml_text: str) -> Dict[str, Any]:
    """Parse one UC Metric View YAML into the fields we diff on.

    Returns ``{source, filter, measures: {name -> expr}, dimensions: {name ->
    expr}}``. Tolerant: malformed YAML or a non-mapping document yields empty
    structures rather than raising, so one bad file never sinks the batch.
    """
    out: Dict[str, Any] = {
        "source": "",
        "filter": "",
        "measures": {},
        "dimensions": {},
    }
    if not yaml_text or not isinstance(yaml_text, str):
        return out
    try:
        doc = yaml.safe_load(yaml_text)
    except yaml.YAMLError:
        return out
    if not isinstance(doc, dict):
        return out
    out["source"] = str(doc.get("source") or "")
    out["filter"] = str(doc.get("filter") or "")
    for key, bucket in (("measures", "measures"), ("dimensions", "dimensions")):
        items = doc.get(key)
        if isinstance(items, list):
            for it in items:
                if isinstance(it, dict) and it.get("name"):
                    out[bucket][str(it["name"])] = str(it.get("expr") or "")
    return out


def _norm(expr: str) -> str:
    """Collapse whitespace so cosmetic reformatting isn't flagged as a change."""
    return " ".join((expr or "").split())


def measure_names(yaml_text: str) -> set:
    """The set of measure names declared in a UC Metric View YAML (empty on junk)."""
    return {n.strip().lower() for n in _load_view(yaml_text).get("measures", {})}


def _name_key(view: str) -> str:
    """Normalise a view/file name for fuzzy matching: lower-case, drop the common
    `_uc_metric_view` suffix and separators so `FT_PE005` ≈ `ft-pe005`."""
    s = (view or "").strip().lower()
    s = s.replace("_uc_metric_view", "")
    for ch in ("_", "-", ".", " "):
        s = s.replace(ch, "")
    return s


def resolve_original(
    view: str, corrected_yaml: str, originals: Dict[str, str]
) -> Tuple[Optional[str], str]:
    """Find our original view for an uploaded one. Returns (original_key, how).

    ``how`` is one of ``exact`` / ``fuzzy_name`` / ``fuzzy_measures`` / ``none``.
    Tries, in order: exact name, then high name-similarity, then highest
    measure-set overlap — so a renamed-but-same-content deployed YAML still pairs
    with what we generated (and we learn from the correction) instead of being
    dropped as unmatched.
    """
    if not originals:
        return None, "none"
    if view in originals:
        return view, "exact"

    # 1) Name similarity (normalised), ≥ 0.80 ratio.
    target = _name_key(view)
    best_name, best_ratio = None, 0.0
    for k in originals:
        r = difflib.SequenceMatcher(None, target, _name_key(k)).ratio()
        if r > best_ratio:
            best_name, best_ratio = k, r
    if best_name is not None and best_ratio >= 0.80:
        return best_name, "fuzzy_name"

    # 2) Measure-set overlap (Jaccard), ≥ 0.50 — catches heavy renames.
    corr = measure_names(corrected_yaml)
    if corr:
        best_m, best_j = None, 0.0
        for k, oy in originals.items():
            om = measure_names(oy)
            if not om:
                continue
            j = len(corr & om) / len(corr | om)
            if j > best_j:
                best_m, best_j = k, j
        if best_m is not None and best_j >= 0.50:
            return best_m, "fuzzy_measures"

    return None, "none"


def diff_view(original_yaml: str, corrected_yaml: str) -> Dict[str, Any]:
    """Measure-level semantic diff of one view: our output vs the customer's.

    Returns changed / added / removed measures (by name), plus source and filter
    changes. ``changed`` only lists measures whose expression actually differs
    after whitespace normalisation.
    """
    orig = _load_view(original_yaml)
    corr = _load_view(corrected_yaml)

    o_m, c_m = orig["measures"], corr["measures"]
    changed: List[Dict[str, str]] = []
    for name, o_expr in o_m.items():
        if name in c_m and _norm(o_expr) != _norm(c_m[name]):
            changed.append({"name": name, "from_expr": o_expr, "to_expr": c_m[name]})
    added = sorted(set(c_m) - set(o_m))
    removed = sorted(set(o_m) - set(c_m))

    return {
        "changed_measures": changed,
        "added_measures": [{"name": n, "expr": c_m[n]} for n in added],
        "removed_measures": removed,
        "source_from": orig["source"],
        "source_to": corr["source"],
        "filter_from": orig["filter"],
        "filter_to": corr["filter"],
    }


def diff_all(
    pairs: List[Dict[str, str]],
) -> Dict[str, Dict[str, Any]]:
    """Diff a list of ``{view, original_yaml, corrected_yaml}`` → ``{view -> diff}``.

    Views with no material change are dropped (nothing to learn from them).
    """
    result: Dict[str, Dict[str, Any]] = {}
    for p in pairs:
        view = str(p.get("view") or "").strip()
        if not view:
            continue
        d = diff_view(p.get("original_yaml", ""), p.get("corrected_yaml", ""))
        has_change = (
            d["changed_measures"]
            or d["added_measures"]
            or d["removed_measures"]
            or _norm(d["source_from"]) != _norm(d["source_to"])
            or _norm(d["filter_from"]) != _norm(d["filter_to"])
        )
        if has_change:
            result[view] = d
    return result


_SYSTEM_PROMPT = """You are analysing how a customer CORRECTED machine-generated \
Unity Catalog Metric View SQL for their own Power BI model.

You are given, per view, the measures whose SQL the customer changed (our original \
expression vs their corrected one), plus added/removed measures and any source/\
filter changes.

Your job: distil the RECURRING, GENERALISABLE lessons — patterns that show up \
across MULTIPLE measures or views — into a domain-context README that will be fed \
back into future translations of THIS customer's model, so we stop repeating the \
same mistakes. Produce TWO clearly-headed sections:

## DAX translation corrections (self-healing — most errors happen here)
The highest-value section. From the `ours:` vs `corrected:` SQL pairs, extract the
RECURRING DAX→SQL translation mistakes and state the rule that fixes each, so the
next translation gets it right the first time. For each pattern give: the DAX
shape / measure kind, what we did WRONG, and the CORRECT SQL form. A short, generic
SQL snippet IS welcome here (it is a translation instruction, not customer data) —
but generalise the pattern, don't just echo one measure.

## Model domain context
Vocabulary / column mappings they consistently apply, recurring business filters
(e.g. a scenario/version code), and naming / unit / scenario / fiscal-calendar
conventions (e.g. prior-year uses a calendar date_py join).

Rules:
- Only report a pattern you see MORE THAN ONCE, or that is clearly a model-wide \
convention. Ignore one-off, measure-specific tweaks — those do not generalise.
- Output GitHub-flavoured Markdown with those two headings and bullet lists, \
suitable for pasting into a "Domain context" field (which is fed into the DAX→SQL \
translation prompt). No preamble, no code fences around the whole document."""


def build_distillation_prompt(
    diffs_by_view: Dict[str, Dict[str, Any]], model_hint: str = ""
) -> str:
    """Assemble the user prompt from the per-view diffs."""
    lines: List[str] = []
    if model_hint:
        lines.append(f"Model / report: {model_hint}\n")
    for view, d in diffs_by_view.items():
        lines.append(f"## View: {view}")
        if _norm(d["source_from"]) != _norm(d["source_to"]):
            lines.append(f"- source: {d['source_from']!r} -> {d['source_to']!r}")
        if _norm(d["filter_from"]) != _norm(d["filter_to"]):
            lines.append(f"- filter: {d['filter_from']!r} -> {d['filter_to']!r}")
        for c in d["changed_measures"]:
            lines.append(
                f"- measure `{c['name']}` changed:\n"
                f"    ours:      {c['from_expr']}\n"
                f"    corrected: {c['to_expr']}"
            )
        if d["added_measures"]:
            lines.append(
                "- customer ADDED measures: "
                + ", ".join(m["name"] for m in d["added_measures"])
            )
        if d["removed_measures"]:
            lines.append(
                "- customer REMOVED measures: " + ", ".join(d["removed_measures"])
            )
        lines.append("")
    return "\n".join(lines).strip()


def distill_corrections_to_readme(
    diffs_by_view: Dict[str, Dict[str, Any]],
    llm_fn: Callable[[str, str], str],
    model_hint: str = "",
) -> Optional[str]:
    """Turn per-view diffs into a reusable domain-context README via ``llm_fn``.

    ``llm_fn(system_prompt, user_prompt) -> markdown``. Returns None when there is
    nothing material to learn from (no changed views), so the caller can tell
    "no corrections" from "the LLM produced guidance".
    """
    if not diffs_by_view:
        return None
    user_prompt = build_distillation_prompt(diffs_by_view, model_hint)
    if not user_prompt:
        return None
    return llm_fn(_SYSTEM_PROMPT, user_prompt)


def distillation_messages(
    diffs_by_view: Dict[str, Dict[str, Any]], model_hint: str = ""
) -> List[Dict[str, str]]:
    """Build the system+user message list for an async LLM distillation call.

    Lets an async caller (the learning service) run the LLM itself —
    ``LLMManager.completion(messages=distillation_messages(...))`` — while the
    prompt text stays owned here. Returns ``[]`` when there's nothing to distil.
    """
    if not diffs_by_view:
        return []
    user_prompt = build_distillation_prompt(diffs_by_view, model_hint)
    if not user_prompt:
        return []
    return [
        {"role": "system", "content": _SYSTEM_PROMPT},
        {"role": "user", "content": user_prompt},
    ]
