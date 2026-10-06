#!/usr/bin/env python3
"""A/B test harness for UC Metric View Generator domain_context impact.

Measures the impact of domain_context on translation quality by:
1. Running the UCMV tool twice: once WITHOUT domain_context, once WITH
2. Parsing outputs and computing quality metrics
3. Printing a side-by-side comparison

Usage:
    python scripts/ab_domain_context.py \\
        --measures-json /path/to/measures.json \\
        --mquery-json /path/to/mquery.json \\
        --domain-context /path/to/context.md \\
        [--config-json /path/to/config.json] \\
        [--relationships-json /path/to/relationships.json] \\
        [--model databricks-claude-sonnet-4-5]

Or with a bundle:
    python scripts/ab_domain_context.py \\
        --bundle /path/to/bundle.json \\
        --domain-context /path/to/context.md
"""

import argparse
import json
import logging
import sys
from pathlib import Path
from typing import Any, Dict, Optional

# Setup logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def load_file(path: str | Path) -> str:
    """Load a file's contents."""
    try:
        with open(path, "r") as f:
            return f.read()
    except FileNotFoundError:
        raise FileNotFoundError(f"File not found: {path}")
    except Exception as e:
        raise RuntimeError(f"Error loading file {path}: {e}")


def load_json_file(path: str | Path) -> dict | list:
    """Load and parse a JSON file."""
    content = load_file(path)
    try:
        return json.loads(content)
    except json.JSONDecodeError as e:
        raise ValueError(f"Invalid JSON in {path}: {e}")


def run_ucmv_tool(
    measures_json: str,
    mquery_json: str,
    domain_context: str = "",
    config_json: Optional[str] = None,
    relationships_json: Optional[str] = None,
    model: Optional[str] = None,
    catalog: str = "main",
    schema_name: str = "default",
) -> Dict[str, Any]:
    """Run the UC Metric View Generator tool once.

    Args:
        measures_json: JSON string of measures
        mquery_json: JSON string of MQuery transpilation
        domain_context: Free text domain knowledge (optional)
        config_json: JSON string of pipeline config (optional)
        relationships_json: JSON string of relationships (optional)
        model: LLM model to use (optional)
        catalog: Target catalog name
        schema_name: Target schema name

    Returns:
        Parsed JSON output from the tool
    """
    try:
        from src.services.tools.uc_metric_view_generator_tool import (
            UCMetricViewGeneratorTool,
        )
    except ImportError as e:
        raise RuntimeError(
            f"Could not import UCMV tool. Are you running from the backend root? Error: {e}"
        )

    try:
        tool = UCMetricViewGeneratorTool()

        kwargs = {
            "measures_json": measures_json,
            "mquery_json": mquery_json,
            "domain_context": domain_context,
            "catalog": catalog,
            "schema_name": schema_name,
            "use_llm_fallback": True,
        }

        if config_json:
            kwargs["config_json"] = config_json
        if relationships_json:
            kwargs["relationships_json"] = relationships_json
        if model:
            kwargs["llm_model"] = model

        logger.info(f"Running UCMV tool with domain_context={bool(domain_context)}")
        result = tool._run(**kwargs)

        try:
            return json.loads(result)
        except json.JSONDecodeError as e:
            raise ValueError(f"Tool returned invalid JSON: {e}. Output: {result[:200]}")

    except Exception as e:
        logger.error(f"UCMV tool execution failed: {e}")
        return {"error": str(e)}


def extract_metrics(output: Dict[str, Any]) -> Dict[str, Any]:
    """Extract quality metrics from UCMV output.

    Args:
        output: The JSON output from the tool

    Returns:
        Dict with extracted metrics
    """
    metrics = {
        "total_measures": 0,
        "translated_count": 0,
        "untranslatable_count": 0,
        "views_count": 0,
        "dax_class_breakdown": {},
        "error": None,
    }

    if "error" in output:
        metrics["error"] = output["error"]
        return metrics

    try:
        stats = output.get("stats", {})
        if isinstance(stats, dict):
            metrics["total_measures"] = stats.get("total_measures", 0)
            metrics["translated_count"] = stats.get("translated_count", 0)
            metrics["untranslatable_count"] = stats.get("untranslatable_count", 0)

        # Extract dax_class breakdown if present
        dax_breakdown = stats.get("dax_class_breakdown", {})
        if isinstance(dax_breakdown, dict):
            metrics["dax_class_breakdown"] = {
                str(k): v for k, v in dax_breakdown.items()
            }

        # Count views from specs
        specs = output.get("specs", {})
        if isinstance(specs, dict):
            metrics["views_count"] = len(specs)

    except Exception as e:
        logger.warning(f"Error extracting metrics: {e}")

    return metrics


def compute_diff_metrics(
    output_a: Dict[str, Any], output_b: Dict[str, Any]
) -> Dict[str, Any]:
    """Compute difference metrics between two runs.

    Args:
        output_a: Output from run without domain_context
        output_b: Output from run with domain_context

    Returns:
        Dict with diff metrics
    """
    diff = {
        "measures_improved": 0,
        "expressions_changed": 0,
        "newly_translated": 0,
        "details": [],
    }

    try:
        specs_a = output_a.get("specs", {})
        specs_b = output_b.get("specs", {})

        if not isinstance(specs_a, dict) or not isinstance(specs_b, dict):
            return diff

        # Build measure maps for both runs
        measures_a = {}
        measures_b = {}

        for table_key, spec_a in specs_a.items():
            if "measures" in spec_a and isinstance(spec_a["measures"], list):
                for m in spec_a["measures"]:
                    measures_a[m.get("name", "")] = m

        for table_key, spec_b in specs_b.items():
            if "measures" in spec_b and isinstance(spec_b["measures"], list):
                for m in spec_b["measures"]:
                    measures_b[m.get("name", "")] = m

        # Compare measures
        all_measure_names = set(measures_a.keys()) | set(measures_b.keys())

        for measure_name in all_measure_names:
            m_a = measures_a.get(measure_name, {})
            m_b = measures_b.get(measure_name, {})

            sql_a = m_a.get("sql_expr", "")
            sql_b = m_b.get("sql_expr", "")

            # Check if expression improved
            if sql_a != sql_b:
                diff["expressions_changed"] += 1

                # Check if newly translated (was blank/error, now has SQL)
                if not sql_a and sql_b:
                    diff["newly_translated"] += 1
                    diff["details"].append(
                        f"  ✓ {measure_name}: newly translated with domain_context"
                    )

                # Check if confidence improved
                conf_a = m_a.get("confidence", 0)
                conf_b = m_b.get("confidence", 0)
                if conf_b > conf_a:
                    diff["measures_improved"] += 1
                    diff["details"].append(
                        f"  ↑ {measure_name}: confidence {conf_a} → {conf_b}"
                    )

    except Exception as e:
        logger.warning(f"Error computing diff metrics: {e}")

    return diff


def format_comparison_table(
    metrics_a: Dict[str, Any], metrics_b: Dict[str, Any], diff: Dict[str, Any]
) -> str:
    """Format metrics as a comparison table.

    Args:
        metrics_a: Metrics from run A (no domain_context)
        metrics_b: Metrics from run B (with domain_context)
        diff: Difference metrics

    Returns:
        Formatted table string
    """
    lines = []
    lines.append("\n" + "=" * 80)
    lines.append(
        "A/B TEST: domain_context Impact on UC Metric View Translation Quality"
    )
    lines.append("=" * 80)

    # Error handling
    if metrics_a.get("error"):
        lines.append(f"\n❌ Run A (no domain_context) FAILED: {metrics_a['error']}")
        return "\n".join(lines)

    if metrics_b.get("error"):
        lines.append(f"\n❌ Run B (with domain_context) FAILED: {metrics_b['error']}")
        return "\n".join(lines)

    # Comparison table
    lines.append(
        "\n{:<30} {:<20} {:<20} {:<15}".format(
            "Metric", "Run A (no context)", "Run B (+ context)", "Change"
        )
    )
    lines.append("-" * 85)

    # Total measures
    total_a = metrics_a.get("total_measures", 0)
    total_b = metrics_b.get("total_measures", 0)
    delta = total_b - total_a
    lines.append(
        "{:<30} {:<20} {:<20} {:<15}".format(
            "Total Measures",
            str(total_a),
            str(total_b),
            f"{delta:+d}" if delta != 0 else "—",
        )
    )

    # Translated count
    trans_a = metrics_a.get("translated_count", 0)
    trans_b = metrics_b.get("translated_count", 0)
    delta_trans = trans_b - trans_a
    trans_pct_a = (trans_a / total_a * 100) if total_a > 0 else 0
    trans_pct_b = (trans_b / total_b * 100) if total_b > 0 else 0
    lines.append(
        "{:<30} {:<20} {:<20} {:<15}".format(
            "Translated Measures",
            f"{trans_a} ({trans_pct_a:.1f}%)",
            f"{trans_b} ({trans_pct_b:.1f}%)",
            f"{delta_trans:+d}" if delta_trans != 0 else "—",
        )
    )

    # Untranslatable count
    untrans_a = metrics_a.get("untranslatable_count", 0)
    untrans_b = metrics_b.get("untranslatable_count", 0)
    delta_untrans = untrans_b - untrans_a
    lines.append(
        "{:<30} {:<20} {:<20} {:<15}".format(
            "Untranslatable Measures",
            str(untrans_a),
            str(untrans_b),
            f"{delta_untrans:+d}" if delta_untrans != 0 else "—",
        )
    )

    # Views count
    views_a = metrics_a.get("views_count", 0)
    views_b = metrics_b.get("views_count", 0)
    delta_views = views_b - views_a
    lines.append(
        "{:<30} {:<20} {:<20} {:<15}".format(
            "Generated Views",
            str(views_a),
            str(views_b),
            f"{delta_views:+d}" if delta_views != 0 else "—",
        )
    )

    lines.append("-" * 85)

    # Verdict
    lines.append("\n📊 VERDICT:")
    newly_translated = diff.get("newly_translated", 0)
    expressions_changed = diff.get("expressions_changed", 0)
    measures_improved = diff.get("measures_improved", 0)

    if newly_translated > 0:
        lines.append(
            f"  ✓ Domain context translated {newly_translated} previously untranslatable measure(s)"
        )

    if measures_improved > 0:
        lines.append(
            f"  ✓ Improved translation quality for {measures_improved} measure(s)"
        )

    if expressions_changed > 0:
        lines.append(
            f"  ✓ Changed SQL expressions for {expressions_changed} measure(s)"
        )

    if newly_translated == 0 and measures_improved == 0 and expressions_changed == 0:
        lines.append("  — No measurable improvement from domain_context in this run")

    # Show details if any
    details = diff.get("details", [])
    if details and len(details) <= 10:
        lines.append("\n  Details:")
        for detail in details[:10]:
            lines.append(detail)
        if len(details) > 10:
            lines.append(f"  ... and {len(details) - 10} more")

    # DAX class breakdown
    breakdown_a = metrics_a.get("dax_class_breakdown", {})
    breakdown_b = metrics_b.get("dax_class_breakdown", {})
    if breakdown_a or breakdown_b:
        lines.append("\n📈 DAX CLASS BREAKDOWN:")
        lines.append("  {:<30} {:<15} {:<15}".format("Class", "Run A", "Run B"))
        lines.append("  " + "-" * 60)
        all_classes = set(breakdown_a.keys()) | set(breakdown_b.keys())
        for cls in sorted(all_classes):
            count_a = breakdown_a.get(cls, 0)
            count_b = breakdown_b.get(cls, 0)
            lines.append(
                "  {:<30} {:<15} {:<15}".format(str(cls), str(count_a), str(count_b))
            )

    lines.append("\n" + "=" * 80)

    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser(
        description="A/B test harness for UCMV domain_context impact",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )

    # Input options
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        "--bundle",
        type=str,
        help="Path to a JSON bundle containing measures_json, mquery_json, config_json, relationships_json",
    )
    group.add_argument(
        "--measures-json",
        type=str,
        help="Path to measures_json file",
    )

    parser.add_argument(
        "--mquery-json",
        type=str,
        help="Path to mquery_json file (required if not using --bundle)",
    )
    parser.add_argument(
        "--config-json", type=str, help="Path to config_json file (optional)"
    )
    parser.add_argument(
        "--relationships-json",
        type=str,
        help="Path to relationships_json file (optional)",
    )

    # Domain context
    parser.add_argument(
        "--domain-context",
        type=str,
        required=True,
        help="Path to domain context README file (.md)",
    )

    # LLM config
    parser.add_argument(
        "--model",
        type=str,
        default=None,
        help="LLM model to use (default: databricks-claude-sonnet-4-5)",
    )
    parser.add_argument(
        "--catalog",
        type=str,
        default="main",
        help="Target catalog name (default: main)",
    )
    parser.add_argument(
        "--schema-name",
        type=str,
        default="default",
        help="Target schema name (default: default)",
    )

    args = parser.parse_args()

    # Load inputs
    logger.info("Loading input files...")

    try:
        # Handle bundle vs individual files
        if args.bundle:
            logger.info(f"Loading bundle from {args.bundle}")
            bundle = load_json_file(args.bundle)
            measures_json = json.dumps(bundle.get("measures_json", []))
            mquery_json = json.dumps(bundle.get("mquery_json", []))
            config_json = (
                json.dumps(bundle.get("config_json", {}))
                if bundle.get("config_json")
                else None
            )
            relationships_json = (
                json.dumps(bundle.get("relationships_json"))
                if bundle.get("relationships_json")
                else None
            )
        else:
            if not args.mquery_json:
                parser.error("--mquery-json is required when not using --bundle")

            measures_json = json.dumps(load_json_file(args.measures_json))
            mquery_json = json.dumps(load_json_file(args.mquery_json))
            config_json = (
                json.dumps(load_json_file(args.config_json))
                if args.config_json
                else None
            )
            relationships_json = (
                json.dumps(load_json_file(args.relationships_json))
                if args.relationships_json
                else None
            )

        # Load domain context
        logger.info(f"Loading domain context from {args.domain_context}")
        domain_context = load_file(args.domain_context)

        logger.info(f"Domain context: {len(domain_context)} characters")

    except (FileNotFoundError, ValueError, json.JSONDecodeError) as e:
        logger.error(f"Failed to load inputs: {e}")
        sys.exit(1)

    # Run A: without domain_context
    logger.info("\n🏃 Running A/B test...")
    logger.info("  Phase 1/2: Running UCMV WITHOUT domain_context...")

    output_a = run_ucmv_tool(
        measures_json=measures_json,
        mquery_json=mquery_json,
        domain_context="",
        config_json=config_json,
        relationships_json=relationships_json,
        model=args.model,
        catalog=args.catalog,
        schema_name=args.schema_name,
    )

    # Run B: with domain_context
    logger.info("  Phase 2/2: Running UCMV WITH domain_context...")

    output_b = run_ucmv_tool(
        measures_json=measures_json,
        mquery_json=mquery_json,
        domain_context=domain_context,
        config_json=config_json,
        relationships_json=relationships_json,
        model=args.model,
        catalog=args.catalog,
        schema_name=args.schema_name,
    )

    # Extract metrics
    logger.info("Computing metrics...")
    metrics_a = extract_metrics(output_a)
    metrics_b = extract_metrics(output_b)

    # Compute diff
    diff = compute_diff_metrics(output_a, output_b)

    # Print results
    comparison = format_comparison_table(metrics_a, metrics_b, diff)
    print(comparison)

    # Save outputs to disk for inspection
    output_dir = Path("/tmp/ab_domain_context_results")
    output_dir.mkdir(exist_ok=True)

    logger.info(f"\nDetailed outputs saved to {output_dir}")
    with open(output_dir / "output_a_no_context.json", "w") as f:
        json.dump(output_a, f, indent=2)

    with open(output_dir / "output_b_with_context.json", "w") as f:
        json.dump(output_b, f, indent=2)

    with open(output_dir / "metrics_comparison.json", "w") as f:
        json.dump(
            {
                "metrics_a": metrics_a,
                "metrics_b": metrics_b,
                "diff": diff,
            },
            f,
            indent=2,
        )

    logger.info("Done!")

    # Return non-zero if there were errors
    if metrics_a.get("error") or metrics_b.get("error"):
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
