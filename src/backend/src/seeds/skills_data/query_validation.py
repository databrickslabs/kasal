"""Checking a result before trusting it."""

SKILL = {
    "name": "validating-query-results",
    "description": (
        "Check a query result or dataset before drawing conclusions from it: "
        "row counts, nulls, duplicate grain, date coverage and outliers, and "
        "how to tell a data problem from a real finding. Use before reporting "
        "any numbers, when a result is surprising, when a total looks too "
        "large, or when two sources disagree. Trigger when the user mentions "
        "verifying data, sanity checking, reconciling numbers, unexpected "
        "results, or numbers that do not match."
    ),
    "body": """# Checking a result before you trust it

A surprising number is usually a data problem, not a discovery. Check first.

## When to use this skill

Before reporting any figure, and immediately whenever a result is surprising.

## The five checks, in order

1. **Row count.** Roughly what you expected? An order of magnitude off means a
   join or a filter did something you did not intend.
2. **Nulls.** Compare `COUNT(*)` with `COUNT(column)`. Aggregates skip nulls
   silently, so an average over a half-null column averages the other half.
3. **Grain.** Is it one row per what you think?
   `SELECT key, COUNT(*) ... GROUP BY key HAVING COUNT(*) > 1`.
   A fan-out join is the most common cause of "revenue doubled".
4. **Date coverage.** `MIN`, `MAX`, and rows per period. A partial final month
   reads as a collapse in the trend.
5. **Outliers.** A single row can dominate a sum. Check the top few
   contributors before attributing a movement to a trend.

## When the number is surprising, prefer the boring explanation

In this order:

1. A filter you forgot — test data, cancelled orders, internal accounts.
2. A join that changed the grain.
3. A timezone or date-boundary difference.
4. A definition change upstream.
5. Only then: something real happened.

## Say what you checked

"Verified: 1.2M rows, no duplicate order ids, dates span 2024-01 to 2024-09,
largest account is 4% of total."

That one line is what makes the rest of the analysis credible.

## Never silently repair data

If rows had to be excluded, say which and why. A cleaned dataset with no note is
indistinguishable from a wrong one.
""",
}
