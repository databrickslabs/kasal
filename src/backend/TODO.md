# Development TODOs — Priority Engineering Gaps

These are the two remaining structural gaps vs the Databricks Engineering PowerBI migration tool.
Both are implementable without architectural rethinking.

---

## TODO 1: DAX Strategy Classification (Pre-LLM Dispatch)

**Priority**: High
**Effort**: Days
**Why it matters**: The Engineering tool dispatches to 22 named strategy rule files before any
LLM call. For exotic DAX patterns (EVENTS_IN_PROGRESS, RECURSIVE_HIERARCHY, BASKET_ANALYSIS,
OPENING_CLOSING_BALANCE, TRANSITION_MATRIX, etc.), this guarantees correct handling. Kasal's
LLM-only approach produces plausible SQL for these but with no semantic guarantee. In a
150-measure production model, this is the difference between correct output and output that
looks right but isn't.

**The 22 known strategy types from Engineering:**
```
SIMPLE, LOD_INCLUDE, LOD_INCLUDE+carryforward, LOD_EXCLUDE, LOD_EXCLUDE_INCLUDE,
COMPOSITE, COMPOSITE_LOD_INCLUDE, NON_CUMULATIVE, TIME_INTEL, RATIO_TO_TOTAL,
FIXED_LOD, WINDOW, OPENING_CLOSING_BALANCE, EVENTS_IN_PROGRESS, BASKET_ANALYSIS,
RECURSIVE_HIERARCHY, DYNAMIC_SEGMENTATION, NEW_RETURNING, PERIOD_COMPARISON,
LIKE_FOR_LIKE, TRANSITION_MATRIX, MULTI_GRAIN_COMPOSITE, AGGR_VIRTUAL
```

**Proposed implementation**:
1. Create `src/converters/services/powerbi/dax_strategy_classifier.py`
   - Fast pre-LLM pass: regex/AST scan of DAX expression
   - Map each measure to one of the 22 named types (or UNKNOWN)
   - Can be a lightweight LLM classification call if regex is insufficient
2. Update `dax_to_sql.py` to accept the classified strategy type
3. Add strategy-specific prompt templates for the high-risk types
   (EVENTS_IN_PROGRESS, RECURSIVE_HIERARCHY, BASKET_ANALYSIS are the hardest)
4. Add tests: one test per strategy type with a representative DAX expression

**Where to integrate**: `measure_conversion_pipeline_tool.py` — add classification step
before the LLM translation call.

---

## TODO 2: Measure Batching for Large Models (Context Window Protection)

**Priority**: Medium-High
**Effort**: ~1 week
**Why it matters**: The Engineering tool's Pass 4 explicitly batches measures into groups,
removes large DAX from parent context, and runs each batch as a separate crew/prompt.
This prevents context window collapse on models with 300+ measures and complex DAX.
Kasal's current approach passes all measures to CrewAI in one shot — on large enterprise
models this is a latent failure mode.

**Proposed implementation**:
1. Create `src/engines/crewai/tools/custom/measure_batcher.py`
   - Input: list of measures with DAX expressions + dependency graph
   - Output: ordered list of batches (each ≤ N measures, configurable)
   - Respect dependencies — measures that reference other measures go in later batches
   - Configurable batch size (default: 20 measures per batch)

2. Add batch orchestration to `measure_conversion_pipeline_tool.py`:
   ```
   measures → MeasureBatcher → [batch_1, batch_2, ..., batch_N]
     → for each batch: run_crew(batch) → collect results
     → merge_results(all_batch_results)
     → validate merged output
   ```

3. State persistence between batches:
   - Already have execution history — use it to store intermediate batch results
   - Resume from last completed batch on failure (flow checkpoint system already exists)

4. Merge logic:
   - Collect all translated measures from each batch
   - Re-resolve cross-batch measure references
   - Run ExpressionValidator on the merged set

**Where to integrate**: `measure_conversion_pipeline_tool.py` — add batching layer before
the current CrewAI execution call. The flow checkpoint system (`CheckpointResumeDialog` in
frontend) already handles resumption — just needs the batch state to be persisted per batch.

**Trigger condition**: Only activate batching when measure count > threshold (e.g., 50).
Below threshold, current single-crew approach is faster and simpler.

---

## Reference: Engineering's Approach (for context)

Engineering's `pass4_planning.py` + `pass4_translation.py` do the following:
- Build a batch plan based on measure dependency graph
- Each batch is a separate Claude prompt with only that batch's DAX in context
- `compact_state.json` holds a compressed version of prior batch results for reference
- Adaptive splitting: if a batch causes context overflow, it's automatically split smaller
- Final synthesis: `pass4_synthesis.py` merges all batch results into a coherent whole

This is the architecture to approximate, not replicate exactly.
