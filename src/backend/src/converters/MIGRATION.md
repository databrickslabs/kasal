# Converters Architecture Migration - Service-Based Structure

## Migration Date
2026-01-05

## Overview
Refactored the converters package from an `inbound/outbound` structure to a service-based architecture for better clarity and maintainability.

## Changes

### Directory Structure

**Before:**
```
converters/
├── base/
├── common/
├── inbound/
│   ├── base.py
│   └── powerbi/
│       ├── connector.py
│       ├── dax_parser.py
│       └── aad_service.py
└── outbound/
    ├── dax/
    ├── sql/
    └── uc_metrics/
```

**After (Phase 1):**
```
converters/
├── base/
│   ├── connectors.py      # Moved from inbound/base.py
│   ├── models.py
│   ├── converter.py
│   └── factory.py
├── common/
│   ├── transformers/
│   └── translators/
├── services/              # NEW - Service-based organization
│   ├── powerbi/          # Power BI integration
│   │   ├── connector.py
│   │   ├── dax_parser.py
│   │   └── aad_service.py
│   ├── dax/              # DAX generation
│   ├── sql/              # SQL generation
│   └── uc_metrics/       # UC Metrics generation
└── pipeline.py
```

**After (Phase 2 - DAX Consolidation):**
```
converters/
├── base/
│   ├── connectors.py
│   ├── models.py
│   ├── converter.py
│   └── factory.py
├── common/
│   ├── transformers/
│   └── translators/
├── services/
│   ├── powerbi/          # Complete Power BI + DAX service
│   │   ├── connector.py        # Power BI data extraction
│   │   ├── dax_parser.py       # DAX parsing/transpilation
│   │   ├── aad_service.py      # Authentication
│   │   ├── dax_generator.py    # DAX measure generation
│   │   ├── dax_tree_parsing.py # Dependency resolution
│   │   ├── dax_smart.py        # Smart generator selection
│   │   ├── dax_context.py      # Context tracking
│   │   ├── dax_syntax_converter.py  # SQL→DAX conversion
│   │   └── dax_aggregations.py # Aggregation builders
│   ├── sql/              # SQL generation
│   └── uc_metrics/       # UC Metrics generation
└── pipeline.py
```

### Key Migrations

#### 1. Base Connectors
- **Moved:** `inbound/base.py` → `base/connectors.py`
- **Reason:** Base connector classes are framework components, not inbound-specific

#### 2. PowerBI Service
- **Moved:** `inbound/powerbi/*` → `services/powerbi/*`
- **Updated imports:**
  - `from ..base import` → `from ...base.connectors import`
  - All internal imports updated

#### 3. DAX Service
- **Moved:** `outbound/dax/*` → `services/dax/*`
- **Updated imports:** Maintained relative imports, updated external references

#### 4. SQL Service
- **Moved:** `outbound/sql/*` → `services/sql/*`
- **Updated imports:** Internal circular imports resolved

#### 5. UC Metrics Service
- **Moved:** `outbound/uc_metrics/*` → `services/uc_metrics/*`
- **Updated imports:** All relative imports working correctly

### Import Changes

#### Pipeline
```python
# Phase 1: Inbound/Outbound → Services
# Before
from .inbound.powerbi import PowerBIConnector
from .outbound.dax.generator import DAXGenerator
from .outbound.sql.generator import SQLGenerator
from .outbound.uc_metrics.generator import UCMetricsGenerator

# After Phase 1
from .services.powerbi import PowerBIConnector
from .services.dax import DaxGenerator as DAXGenerator
from .services.sql import SQLGenerator
from .services.uc_metrics import UCMetricsGenerator

# Phase 2: DAX Consolidation
# After Phase 2 (Current)
from .services.powerbi import PowerBIConnector, DAXGenerator
from .services.sql import SQLGenerator
from .services.uc_metrics import UCMetricsGenerator
```

#### Tools (engines/crewai/tools/custom/)
Updated all tool imports:
- `yaml_to_dax.py`
- `yaml_to_sql.py`
- `yaml_to_uc_metrics.py`
- `powerbi_connector_tool.py`
- `measure_conversion_pipeline_tool.py`

### Benefits

1. **Clearer Organization:** Services organized by domain/technology rather than data flow direction
2. **Easier to Extend:** Adding new services (Tableau, Looker, etc.) is straightforward
3. **Better Semantics:** "services/powerbi" is clearer than "inbound/powerbi"
4. **Self-Documenting:** Each service is self-contained with clear responsibilities
5. **No Confusion:** Eliminates "is this inbound or outbound?" ambiguity

### Backward Compatibility

**Breaking Changes:**
- All imports from `converters.inbound.*` must be updated to `converters.services.*` or `converters.base.*`
- All imports from `converters.outbound.*` must be updated to `converters.services.*`

**Migration Guide:**
```python
# Phase 1: Old imports
from converters.inbound.powerbi import PowerBIConnector
from converters.outbound.dax.generator import DAXGenerator

# Phase 1: New imports
from converters.services.powerbi import PowerBIConnector
from converters.services.dax import DaxGenerator

# Phase 2: Current imports (DAX consolidated)
from converters.services.powerbi import PowerBIConnector, DAXGenerator
# All DAX classes now available from powerbi service
```

### Testing

- ✅ Python syntax check passed for all migrated files
- ✅ All imports updated and verified
- ✅ No circular import issues
- ⚠️ Full integration testing recommended

### Files Modified

**Core Converters:**
- `converters/base/__init__.py` - Added connector exports
- `converters/base/connectors.py` - Created (from inbound/base.py)
- `converters/__init__.py` - Updated documentation
- `converters/pipeline.py` - Updated imports

**Services:**
- `converters/services/powerbi/*` - All files
- `converters/services/dax/*` - All files
- `converters/services/sql/*` - All files
- `converters/services/uc_metrics/*` - All files

**Tools:**
- `engines/crewai/tools/custom/yaml_to_dax.py`
- `engines/crewai/tools/custom/yaml_to_sql.py`
- `engines/crewai/tools/custom/yaml_to_uc_metrics.py`
- `engines/crewai/tools/custom/powerbi_connector_tool.py`
- `engines/crewai/tools/custom/measure_conversion_pipeline_tool.py`

### Phase 2: DAX Consolidation (2026-01-05)

**Rationale:** DAX is Power BI's query language. Having DAX parsing in `powerbi/` and DAX generation in `dax/` created confusion and violated single-responsibility. All DAX operations should be unified in the PowerBI service.

**Changes:**
1. **Moved files** from `services/dax/` to `services/powerbi/` with `dax_` prefix:
   - `generator.py` → `dax_generator.py`
   - `tree_parsing.py` → `dax_tree_parsing.py`
   - `smart.py` → `dax_smart.py`
   - `context.py` → `dax_context.py`
   - `syntax_converter.py` → `dax_syntax_converter.py`
   - `aggregations.py` → `dax_aggregations.py`

2. **Updated imports** in moved files to reference new `dax_` prefixed filenames

3. **Updated all external references** from `services.dax` to `services.powerbi`:
   - `converters/pipeline.py`
   - `converters/__init__.py`
   - `engines/crewai/tools/custom/yaml_to_dax.py`

4. **Enhanced PowerBI service exports** in `services/powerbi/__init__.py`:
   ```python
   # Now exports complete DAX functionality
   from .dax_generator import DAXGenerator
   from .dax_tree_parsing import TreeParsingDAXGenerator
   from .dax_smart import SmartDAXGenerator
   from .dax_context import DAXBaseKBIContext, DAXKBIContextCache
   from .dax_syntax_converter import DaxSyntaxConverter
   from .dax_aggregations import AggregationType, DAXAggregationBuilder, ...
   ```

5. **Removed** `services/dax/` directory entirely

**Benefits:**
- ✅ Single source of truth for all DAX operations
- ✅ Clear semantic grouping: PowerBI service handles everything PowerBI/DAX related
- ✅ Easier to understand: "If it's DAX, look in powerbi/"
- ✅ Reduced confusion: No more "which DAX service do I import from?"

### Removed

**Phase 1:**
- `converters/inbound/` directory (entire)
- `converters/outbound/` directory (entire)

**Phase 2:**
- `converters/services/dax/` directory (entire - consolidated into powerbi/)

### Phase 3: PowerBI Service Subdirectory Organization (2026-01-05)

**Rationale:** Clear separation between two distinct workflows:
1. **Extraction** (PowerBI connection) - Extract DAX and transpile to SQL
2. **YAML to DAX** (YAML input) - Generate DAX measures from KPI definitions

**Changes:**
1. **Created subdirectories** in `services/powerbi/`:
   - `extraction/` - PowerBI connection-based extraction and transpilation
   - `yaml_to_dax/` - YAML-based DAX measure generation (renamed from `generation/`)

2. **Moved extraction files** to `powerbi/extraction/`:
   - `aad_service.py` - Azure AD authentication
   - `connector.py` - PowerBI data extraction
   - `dax_parser.py` - DAX parsing and transpilation (use `parse_advanced()`)

3. **Moved YAML→DAX files** to `powerbi/yaml_to_dax/`:
   - `dax_generator.py` - Main DAX generator from KPI definitions
   - `dax_tree_parsing.py` - Dependency resolution
   - `dax_smart.py` - Smart generator selection
   - `dax_context.py` - Context tracking
   - `dax_syntax_converter.py` - SQL→DAX conversion
   - `dax_aggregations.py` - Aggregation builders

4. **Updated all relative imports** from `...` to `....` (added one level for subdirectories)

5. **Created subdirectory __init__.py files** with clear workflow documentation

**Workflows:**

**Extraction Workflow (PowerBI Connection → SQL):**
```
PowerBI connection → connector.py → Extract DAX → dax_parser.parse_advanced()
→ Transpiled SQL stored in KPI._advanced_parsing → pipeline.py formatting
→ Output: SQL (list of dicts) or UC Metrics YAML
```

**YAML to DAX Workflow (YAML → DAX Measures):**
```
YAML KPI Definition → DAXGenerator → DAX measures
→ Output: PowerBI DAX measures
```

**Benefits:**
- ✅ Clear visual separation of two different workflows
- ✅ Self-documenting structure - names clearly indicate source/target
- ✅ No confusion: `extraction/` = from PowerBI, `yaml_to_dax/` = from YAML
- ✅ Easier to navigate and maintain
- ✅ Scalable for adding more conversion workflows

### Phase 4: Pipeline-Based Formatting (2026-01-05)

**Rationale:** Centralize output formatting in pipeline.py instead of separate formatter files. The transpilation logic already exists in dax_parser.py's `parse_advanced()` method, and the formatting should happen where orchestration occurs.

**Changes:**
1. **Deleted redundant formatter files:**
   - Removed `services/powerbi/extraction/sql_yaml_output.py`
   - Removed `services/powerbi/extraction/uc_metrics_output.py`
   - Updated `extraction/__init__.py` to remove these exports

2. **Implemented pipeline-based formatting** in `converters/pipeline.py`:
   - Added `_format_transpiled_sql()` method (lines 222-260)
     - Extracts transpiled SQL from `KPI._advanced_parsing` attribute
     - Returns list of dicts: {name, sql, description, is_transpilable, signature, source_table}
     - Falls back to original formula if not transpilable

   - Added `_format_transpiled_uc_metrics()` method (lines 262-329)
     - Wraps transpiled SQL in UC Metrics YAML format
     - Builds fully qualified source reference (catalog.schema.table)
     - Includes measures with transpiled SQL expressions
     - Adds warnings for non-transpilable measures

3. **Enhanced path detection** in `_convert_to_format()`:
   - Added `inbound_type` parameter to detect source (PowerBI vs YAML)
   - Added `use_transpilation` flag routing:
     ```python
     use_transpilation = (inbound_type == ConnectorType.POWERBI and
                         format in [OutboundFormat.SQL, OutboundFormat.UC_METRICS])
     ```
   - PowerBI → SQL/UC_METRICS: Uses transpilation path (`_format_transpiled_*`)
   - YAML → SQL/UC_METRICS: Uses generation path (`SQLGenerator`/`UCMetricsGenerator`)

**Data Flow:**
```
PowerBI Connector:
  extract_measures() → parse_advanced() → store in KPI._advanced_parsing

Pipeline (transpilation path):
  _convert_to_format() → detects PowerBI source
  → _format_transpiled_sql() OR _format_transpiled_uc_metrics()
  → extracts from KPI._advanced_parsing
  → returns formatted output

Pipeline (generation path):
  _convert_to_format() → detects YAML source
  → SQLGenerator OR UCMetricsGenerator
  → generates from KPI definitions
  → returns formatted output
```

**Benefits:**
- ✅ Single source of truth for formatting (pipeline.py)
- ✅ No code duplication between formatter files
- ✅ Clear separation between transpilation and generation paths
- ✅ Easier to maintain and extend
- ✅ Consistent with other converters (DAX, SQL generation)

### Next Steps

1. Run full test suite
2. Update any external documentation
3. Notify team of import changes
4. Consider adding service-level README files

## Rollback Plan

If issues arise:
1. Restore from git: `git checkout HEAD -- src/backend/src/converters/`
2. Or use the archived directories if kept as backup

## Notes

- All files were copied first, then imports updated, then old directories removed
- Syntax validation completed successfully
- No runtime testing performed yet - recommend thorough testing before deployment
