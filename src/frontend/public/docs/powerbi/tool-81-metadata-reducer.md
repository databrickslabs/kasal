# Tool 81 - Power BI Metadata Reducer

**What it is:** Takes the full model context JSON (from Tool 79) and a user question, then uses fuzzy matching and LLM-assisted selection to return only the tables, measures, and relationships relevant to that specific question.

---

## Why It Exists

A large Power BI model can have 300+ measures and 50+ tables. Sending all of that to an LLM for DAX generation creates two problems: token limits (the context exceeds what the LLM can process) and accuracy problems (the LLM gets distracted by irrelevant measures and generates incorrect queries). Tool 81 solves this by filtering the model to only what matters for the question.

## What Problem It Solves

- **Token limit:** Large models can't be passed wholesale to an LLM; reduction makes it feasible
- **Accuracy:** Focused context produces more accurate DAX - fewer irrelevant measures means fewer hallucinated references
- **Speed:** Smaller prompt = faster LLM response

---

## How It Works

```
Receive full model_context_json (from Tool 79) + user_question
    ↓
Fuzzy matching: find measures/tables whose names overlap with question keywords
    ↓
Dependency resolution: include all measures referenced by matched measures
    ↓
LLM-assisted selection: LLM picks the most relevant tables/measures (optional)
    ↓
Return reduced model_context_json (10-20 measures vs 300+)
```

---

## Configuration

| Parameter | Required | Description |
|-----------|----------|-------------|
| `model_context_json` | Yes | Full model JSON from Tool 79 |
| `user_question` | Yes | The business question to filter for |
| `max_measures` | No | Max measures to include (default: 20) |
| `max_tables` | No | Max tables to include (default: 10) |
| `use_llm_selection` | No | LLM-assisted relevance ranking (default: `true`) |
| `llm_workspace_url` | No | Databricks workspace for LLM |
| `llm_token` | No | Databricks PAT for LLM |

---

## Example Crew Position

Always placed between Tool 79 and Tool 80:

```
Tool 79 (fetch full model)
    ↓
Tool 81 (reduce to question-relevant subset)   ← this tool
    ↓
Tool 80 (generate DAX from reduced context)
```

---

## Notes

- Optional but strongly recommended for models with more than 50 measures
- Without LLM selection, uses fuzzy matching only - faster but slightly less accurate
- The reduced output is a valid `model_context_json` that Tool 80 accepts directly
- Does not make any PBI API calls - works entirely on the JSON from Tool 79
