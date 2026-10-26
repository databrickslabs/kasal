"""Researching on the live web with checkable sources."""

SKILL = {
    "name": "researching-with-sources",
    "description": (
        "Always load this skill before any task that involves searching the "
        "web and reporting what was found: it defines how to pick sources, "
        "the mandatory citation format, and what to do when a claim cannot "
        "be verified. Use for market or competitor research, fact-finding, "
        "background on a company, product, technology or event, and any "
        "answer that will cite the internet. Trigger when the user mentions "
        "researching, looking something up, finding sources, checking a "
        "claim, or asks a question whose answer lives outside the workspace."
    ),
    "body": """# Researching with checkable sources

An unsourced claim from the web is worthless: nobody can check it, and much of
what circulates is wrong. The job is to come back with few, verified things —
not many plausible ones.

## When to use this skill

Any task that searches the web and reports what it found.

## 1. Fix the question before searching

Pin the period, the scope and the audience in one line at the top of the
answer. If the request does not say, pick the most likely reading and state
it — do not open with a clarifying question you can answer yourself.

## 2. Search more than once, differently

One query returns one slice. Rephrase two or three times — the vendor's name,
the product's name, the category term — and compare what comes back.

## 3. Go to the primary source

For anything worth reporting, open the origin: the announcement, the filing,
the changelog, the paper, the documentation. A summary of a summary is where
errors compound. An article ABOUT a thing is a lead, not a source.

## 4. Cite with the exact URL — this is not optional

- **Reproduce URLs exactly as your tools returned them.** Never shorten,
  never describe ("the docs page"), never write a URL you did not receive —
  a plausible invented link is worse than none, because it will be believed
  and then fail.
- One link per claim, the PRIMARY one.
- If a tool gave an answer with no URL, say so: "reported by search, no
  primary source returned."
- Close with a Sources list: `[n] Publisher — Title — URL`, one per line.

## 5. Date every claim

"Latest", "recently" and "now" rot silently. Write the date the fact was true
or published. If a page is undated, say so.

## 6. Say what you could not check

Paywalls, dead links, claims with a single low-quality source, numbers with no
methodology. An answer that lists its own gaps is one the reader can act on;
one that hides them is one they will stop trusting.

## When sources disagree

Report the disagreement — both figures, both links — and say which you would
weight and why. Do not average, and do not silently pick one.
""",
}
