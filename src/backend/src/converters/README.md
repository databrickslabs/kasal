# Converters Package

This package contains all measure conversion logic for transforming business measures between different formats.

## Architecture

The converters package follows a **clean architecture pattern** with clear separation of concerns:

```
converters/
├── base/              # Base classes and factory pattern
├── models/            # Pydantic data models
├── measure/           # Core measure conversion logic (to be implemented)
├── formats/           # Format-specific handlers (to be implemented)
├── rules/             # Conversion rules and mappings (to be implemented)
└── utils/             # Helper utilities (to be implemented)
```

## Supported Conversions

| Source Format | Target Format | Status |
|--------------|---------------|---------|
| YAML | DAX | 🔜 Pending |
| YAML | SQL | 🔜 Pending |
| YAML | UC Metrics | 🔜 Pending |
| Power BI | YAML | 🔜 Pending |

## Usage

### API Endpoints

The measure conversion service is exposed via REST API:

**Base URL**: `/api/measure-conversion`

#### Get Available Formats
```http
GET /api/measure-conversion/formats
```

#### Convert Measures
```http
POST /api/measure-conversion/convert
Content-Type: application/json

{
  "source_format": "yaml",
  "target_format": "dax",
  "input_data": {
    "description": "Sales Metrics",
    "technical_name": "SALES_METRICS",
    "kbis": [
      {
        "description": "Total Revenue",
        "formula": "SUM(Sales[Amount])"
      }
    ]
  },
  "config": {
    "optimize": true,
    "validate": true
  }
}
```

#### Validate Measures
```http
POST /api/measure-conversion/validate
Content-Type: application/json

{
  "format": "yaml",
  "input_data": {
    "description": "Sales Metrics",
    "technical_name": "SALES_METRICS",
    "kbis": []
  }
}
```

#### Batch Convert
```http
POST /api/measure-conversion/batch-convert
Content-Type: application/json

[
  {
    "source_format": "yaml",
    "target_format": "dax",
    "input_data": {...}
  },
  {
    "source_format": "yaml",
    "target_format": "sql",
    "input_data": {...}
  }
]
```

### Programmatic Usage

#### Creating a New Converter

1. **Extend BaseConverter**:

```python
from converters.base.base_converter import BaseConverter, ConversionFormat

class YAMLToDAXConverter(BaseConverter):
    def __init__(self, config=None):
        super().__init__(config)

    @property
    def source_format(self) -> ConversionFormat:
        return ConversionFormat.YAML

    @property
    def target_format(self) -> ConversionFormat:
        return ConversionFormat.DAX

    def validate_input(self, input_data) -> bool:
        # Validate YAML structure
        return True

    def convert(self, input_data, **kwargs):
        # Implement conversion logic
        return converted_data
```

2. **Register with Factory**:

```python
from converters.base.converter_factory import ConverterFactory

ConverterFactory.register(
    source_format=ConversionFormat.YAML,
    target_format=ConversionFormat.DAX,
    converter_class=YAMLToDAXConverter
)
```

3. **Use via Service**:

```python
from src.services.measure_conversion_service import MeasureConversionService

service = MeasureConversionService()
result = await service.convert(
    source_format=ConversionFormat.YAML,
    target_format=ConversionFormat.DAX,
    input_data=yaml_data
)
```

## Data Models

### KBI (Key Business Indicator)

The core data model representing a business measure:

```python
from converters.models.kbi import KBI, KBIDefinition

kbi = KBI(
    description="Total Revenue",
    formula="SUM(Sales[Amount])",
    filters=[],
    technical_name="TOTAL_REVENUE"
)
```

### KBIDefinition

Complete definition with metadata, filters, and structures:

```python
definition = KBIDefinition(
    description="Sales Metrics",
    technical_name="SALES_METRICS",
    kbis=[kbi1, kbi2],
    structures={"YTD": ytd_structure},
    filters={"date_filter": {...}}
)
```

## Development Roadmap

### Phase 1: Core Infrastructure ✅
- [x] Base converter classes
- [x] Factory pattern
- [x] Data models (KBI, DAXMeasure, SQLMeasure, UCMetric)
- [x] API router and service layer
- [x] Pydantic schemas

### Phase 2: YAML → DAX Conversion 🔜
- [ ] YAML parser
- [ ] DAX formula generator
- [ ] Aggregation rules
- [ ] Filter transformation
- [ ] Dependency resolution

### Phase 3: YAML → SQL Conversion 🔜
- [ ] SQL query generator
- [ ] SQL aggregation rules
- [ ] Table/column mapping

### Phase 4: YAML → UC Metrics 🔜
- [ ] UC Metrics processor
- [ ] Unity Catalog integration

### Phase 5: Power BI Integration 🔜
- [ ] PBI measure parser
- [ ] XMLA connector
- [ ] Measure extraction

## Testing

### Unit Tests
Test individual converters in isolation:

```python
# tests/unit/converters/test_yaml_to_dax.py
async def test_yaml_to_dax_conversion():
    converter = YAMLToDAXConverter()
    result = converter.convert(yaml_input)
    assert result.success
```

### Integration Tests
Test full conversion flow via API:

```python
# tests/integration/api/test_measure_conversion.py
async def test_convert_endpoint(client):
    response = await client.post(
        "/api/measure-conversion/convert",
        json={
            "source_format": "yaml",
            "target_format": "dax",
            "input_data": {...}
        }
    )
    assert response.status_code == 200
```

## Migration from yaml2dax

The existing code at `/Users/david.schwarzenbacher/Downloads/yaml2dax_clean/api/src/yaml2dax`
will be migrated into this structure:

| yaml2dax Module | New Location |
|----------------|--------------|
| `parsers/` | `converters/formats/` |
| `generators/` | `converters/formats/` |
| `models/kbi.py` | `converters/models/kbi.py` ✅ |
| `processors/` | `converters/measure/` |
| `translators/` | `converters/rules/` |
| `resolvers/` | `converters/utils/` |
| `aggregators/` | `converters/rules/` |

## Contributing

When adding new conversion logic:

1. Create converter class extending `BaseConverter`
2. Register with `ConverterFactory`
3. Add comprehensive tests
4. Update this README with supported conversions
5. Follow clean architecture patterns

## Notes

- All database operations must be async
- Use factory pattern for converter instantiation
- Maintain separation between API, service, and domain layers
- Follow existing kasal patterns and conventions
