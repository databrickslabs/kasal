"""Build ``TranslationResult``s from ``config['implicit_column_measures']``.

Split out of ``table_processor.py`` (over the file-size ceiling) as its own
seam — this is Step 6d of that module's per-table pipeline: raw columns with
PBI's own default aggregation that are drawn/filtered directly in a visual,
with no named DAX measure behind them
(``services.powerbi.implicit_column_measures.derive_implicit_column_measures``
computed the entries; this module's only job is turning them into the same
``TranslationResult`` shape every other measure source produces).
"""

from __future__ import annotations

from .data_classes import TranslationResult


def build_implicit_measures(
    defs: list[dict], base_names: set[str]
) -> list[TranslationResult]:
    """One ``TranslationResult`` per entry in ``defs`` whose name doesn't
    already collide with a name in ``base_names`` (a real measure or base
    column already owns it) — mutates ``base_names`` to register each one it
    builds, exactly like the SWITCH-decomposition step above it does.
    """
    out: list[TranslationResult] = []
    for defn in defs:
        if defn["name"] in base_names:
            continue
        out.append(
            TranslationResult(
                measure_name=defn["name"],
                original_name=defn["original_name"],
                sql_expr=defn["raw_expr"],
                is_translatable=True,
                skip_reason=defn.get("comment", ""),
                dax_expression="",
                confidence="high",
                category="implicit_visual_column",
                used_in_visuals=defn.get("used_in_visuals", []),
                pbi_kind=defn.get("pbi_kind", "raw_column"),
                pbi_sources=defn.get("pbi_sources", []),
            )
        )
        base_names.add(defn["name"])
    return out
