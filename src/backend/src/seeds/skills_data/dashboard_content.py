"""Writing content that becomes a strong dashboard."""

SKILL = {
    "name": "building-dashboard-content",
    "description": (
        "Produce the content a dashboard is built from: few metrics that "
        "matter, every number paired with a comparison, each with its grain, "
        "period and source. Use when the deliverable is a dashboard, KPI "
        "overview, scorecard or metrics summary. Trigger when the user asks "
        "for a dashboard, KPIs, metrics, a scorecard, an overview of the "
        "numbers, or monitoring."
    ),
    "body": """# Content that becomes a good dashboard

**You write the numbers and their meaning; Kasal lays out the dashboard.** The
failure mode is not an ugly layout — it is a wall of context-free numbers, or
twenty metrics where five would carry the story.

## When to use this skill

Any task whose output is a dashboard, scorecard or metrics overview.

## 1. Few metrics, chosen for the question

Three to seven. Every metric must answer "what would someone DO differently if
this moved?" — a number nobody acts on is decoration. If the request names no
metrics, derive them from what the audience decides.

## 2. A number without a comparison is not information

"Revenue: 4.2M" says nothing. Pair every figure with at least one of: target,
prior period, same period last year, peer segment. The comparison is what
turns a value into a signal — include it in the content explicitly.

## 3. State grain, period and source per metric

"Monthly active users, calendar month, from prod.analytics.usage" — without
this, two metrics that look comparable silently are not, and the dashboard
misleads precisely when someone relies on it.

## 4. Trends need enough points to be trends

A line through three points is noise with confidence. Give at least eight to
twelve periods for a trend, and flag a partial current period — it always
reads as a collapse.

## 5. One line of insight per metric, not a paragraph

Dashboard text is a caption: "up 8% QoQ, driven by EMEA renewals" — the fact
and its driver. Analysis belongs in a report; a dashboard that needs reading
has failed at being a dashboard.

## 6. Segment only where the difference is the story

A breakdown by region earns its place when regions diverge. Splitting every
metric every way multiplies tiles and divides attention.

## Verify before it ships

Dashboard numbers get glanced at and believed. Run the checks from
validating-query-results on every figure — a wrong number on a dashboard does
more damage than a wrong number anywhere else.
""",
}
