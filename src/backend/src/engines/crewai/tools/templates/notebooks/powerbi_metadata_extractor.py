# Databricks notebook source
"""
Power BI Metadata Extractor

This notebook extracts schema metadata from Power BI semantic models/datasets
and formats it for use with PowerBITool in Kasal.

Uses INFO.TABLES and INFO.COLUMNS DMV functions (compatible with REST API executeQueries endpoint).

The output is a JSON structure containing tables, columns, data types, and relationships
that can be directly used as the 'metadata' parameter in PowerBITool.

Required Parameters (via job_params):
- workspace_id: Power BI workspace ID
- semantic_model_id: Power BI semantic model/dataset ID
- table_names: List of table names to extract metadata for (e.g., ["Sales", "Products"])
- auth_method: "device_code" or "service_principal" (default: "device_code")

For Service Principal auth, also provide:
- client_id: Azure AD application client ID
- tenant_id: Azure AD tenant ID
- client_secret: Service principal secret

Optional Parameters:
- include_hidden: Include hidden tables/columns (default: false)
- include_relationships: Include table relationships (default: true)
- output_format: "json" or "python_dict" (default: "json")
"""

# COMMAND ----------

# MAGIC %pip install azure-identity requests pandas

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# Import required libraries
import json
import requests
import pandas as pd
from datetime import datetime
from typing import Dict, List, Any, Optional
from azure.identity import DeviceCodeCredential, ClientSecretCredential

# COMMAND ----------

# DBTITLE 1,Configuration - Get Job Parameters
# Default configuration
DEFAULT_TENANT_ID = "9f37a392-f0ae-4280-9796-f1864a10effc"
DEFAULT_CLIENT_ID = "1950a258-227b-4e31-a9cf-717495945fc2"

try:
    job_params = json.loads(dbutils.widgets.get("job_params"))

    # Required parameters
    WORKSPACE_ID = job_params.get("workspace_id")
    SEMANTIC_MODEL_ID = job_params.get("semantic_model_id")

    # Authentication configuration
    AUTH_METHOD = job_params.get("auth_method", "device_code")
    TENANT_ID = job_params.get("tenant_id", DEFAULT_TENANT_ID)
    CLIENT_ID = job_params.get("client_id", DEFAULT_CLIENT_ID)
    CLIENT_SECRET = job_params.get("client_secret")

    # Optional parameters
    INCLUDE_HIDDEN = job_params.get("include_hidden", False)
    INCLUDE_RELATIONSHIPS = job_params.get("include_relationships", True)
    OUTPUT_FORMAT = job_params.get("output_format", "json")  # "json" or "python_dict"

    # Table names (REQUIRED for REST API - DMVs don't work)
    TABLE_NAMES = job_params.get("table_names", [])

    print("=" * 80)
    print("Power BI Metadata Extractor")
    print("=" * 80)
    print(f"Execution Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Workspace ID: {WORKSPACE_ID}")
    print(f"Semantic Model ID: {SEMANTIC_MODEL_ID}")
    print(f"Authentication Method: {AUTH_METHOD}")
    print(f"Table Names Provided: {len(TABLE_NAMES) if TABLE_NAMES else 0}")
    print(f"Include Relationships: {INCLUDE_RELATIONSHIPS}")
    print(f"Output Format: {OUTPUT_FORMAT}")
    print("=" * 80)

    if not TABLE_NAMES:
        print("\n⚠️  WARNING: No table_names provided!")
        print("   The REST API executeQueries endpoint does not support automatic")
        print("   schema discovery via DMV functions (INFO.*, TMSCHEMA_*, etc.)")
        print("\n   Please provide table_names in job_params:")
        print('   "table_names": ["Table1", "Table2", "Table3"]')
        print("\n   Exiting...")
        raise ValueError("table_names parameter is required for metadata extraction")

except Exception as e:
    print(f"❌ Error getting parameters: {str(e)}")
    print("\nRequired parameters in job_params:")
    print("- workspace_id: Power BI workspace ID")
    print("- semantic_model_id: Power BI dataset/semantic model ID")
    print("\nOptional parameters:")
    print("- auth_method: 'device_code' or 'service_principal' (default: 'device_code')")
    print("- include_hidden: true/false (default: false)")
    print("- include_relationships: true/false (default: true)")
    print("- output_format: 'json' or 'python_dict' (default: 'json')")
    raise

# COMMAND ----------

# DBTITLE 1,Authentication Functions
def generate_token_device_code(tenant_id: str, client_id: str) -> str:
    """Generate token using device code flow (DCF)."""
    try:
        credential = DeviceCodeCredential(
            client_id=client_id,
            tenant_id=tenant_id,
        )

        print("\n🔄 Initiating Device Code Flow authentication...")
        print("⚠️  Follow the instructions above to authenticate")

        token = credential.get_token("https://analysis.windows.net/powerbi/api/.default")

        print("✅ Token generated successfully")
        return token.token

    except Exception as e:
        print(f"❌ Token generation failed: {str(e)}")
        raise


def generate_token_service_principal(tenant_id: str, client_id: str, client_secret: str) -> str:
    """Generate token using Service Principal."""
    try:
        print("\n🔄 Authenticating with Service Principal...")

        credential = ClientSecretCredential(
            tenant_id=tenant_id,
            client_id=client_id,
            client_secret=client_secret
        )

        token = credential.get_token("https://analysis.windows.net/powerbi/api/.default")

        print("✅ Token generated successfully")
        return token.token

    except Exception as e:
        print(f"❌ Service Principal authentication failed: {str(e)}")
        raise

# COMMAND ----------

# DBTITLE 1,Generate Access Token
try:
    if AUTH_METHOD == "service_principal":
        if not CLIENT_SECRET:
            raise ValueError("client_secret is required for service_principal authentication")

        access_token = generate_token_service_principal(
            tenant_id=TENANT_ID,
            client_id=CLIENT_ID,
            client_secret=CLIENT_SECRET
        )
    else:
        access_token = generate_token_device_code(
            tenant_id=TENANT_ID,
            client_id=CLIENT_ID
        )

    print(f"\n✅ Authentication successful!")

except Exception as e:
    print(f"\n❌ Authentication failed: {str(e)}")
    raise

# COMMAND ----------

# DBTITLE 1,Power BI Metadata API Functions
def get_dataset_info(token: str, dataset_id: str) -> dict:
    """Get basic dataset information."""
    url = f"https://api.powerbi.com/v1.0/myorg/datasets/{dataset_id}"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }

    print(f"🔄 Fetching dataset information...")
    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        print("✅ Dataset information retrieved")
        return response.json()
    else:
        print(f"❌ Failed to get dataset info: {response.text}")
        return {}


def execute_dax_for_metadata(token: str, dataset_id: str, dax_query: str) -> pd.DataFrame:
    """Execute a DAX query to retrieve metadata."""
    url = f"https://api.powerbi.com/v1.0/myorg/datasets/{dataset_id}/executeQueries"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }

    body = {
        "queries": [{"query": dax_query}],
        "serializerSettings": {"includeNulls": True}
    }

    print(f"   Executing query: {dax_query[:50]}...")
    response = requests.post(url, headers=headers, json=body, timeout=60)
    print(f"   Response status: {response.status_code}")

    if response.status_code == 200:
        results = response.json().get("results", [])
        if results and results[0].get("tables"):
            rows = results[0]["tables"][0].get("rows", [])
            if rows:
                return pd.DataFrame(rows)
    else:
        print(f"   ❌ Query failed: {response.text}")

    return pd.DataFrame()


def map_dax_type_to_metadata_type(dax_type: str) -> str:
    """
    Map DAX/Power BI data types to simplified metadata types.

    Args:
        dax_type: Power BI data type (e.g., "Int64", "String", "Decimal", "DateTime")

    Returns:
        str: Simplified type ("string", "int", "decimal", "datetime", "boolean")
    """
    type_mapping = {
        # String types
        "String": "string",
        "Text": "string",

        # Integer types
        "Int64": "int",
        "Integer": "int",
        "Whole Number": "int",

        # Decimal/Float types
        "Decimal": "decimal",
        "Double": "decimal",
        "Currency": "decimal",
        "Fixed Decimal Number": "decimal",
        "Decimal Number": "decimal",

        # DateTime types
        "DateTime": "datetime",
        "Date": "datetime",
        "Time": "datetime",

        # Boolean types
        "Boolean": "boolean",
        "True/False": "boolean"
    }

    # Return mapped type or default to string
    return type_mapping.get(dax_type, "string")


def extract_table_metadata_manual(token: str, dataset_id: str, table_names: List[str]) -> List[Dict[str, Any]]:
    """
    Extract column metadata by querying each table directly.

    This is a fallback when DMV functions don't work with the REST API.
    Requires user to provide table names upfront.
    """
    print(f"\n🔄 Extracting metadata for {len(table_names)} tables manually...")

    tables_metadata = []

    for table_name in table_names:
        print(f"   Processing table: {table_name}")

        # Query first row to get column names and types
        query = f"EVALUATE TOPN(1, '{table_name}')"
        df = execute_dax_for_metadata(token, dataset_id, query)

        if df.empty:
            print(f"   ⚠️  Could not query table: {table_name}")
            continue

        # Extract column information from DataFrame
        columns = []
        for col_name in df.columns:
            # Remove the square brackets from column names
            clean_name = col_name.strip('[]')

            # Infer data type from pandas dtype
            dtype = df[col_name].dtype
            if dtype == 'object':
                data_type = 'string'
            elif dtype == 'int64':
                data_type = 'int'
            elif dtype == 'float64':
                data_type = 'decimal'
            elif dtype == 'datetime64[ns]':
                data_type = 'datetime'
            elif dtype == 'bool':
                data_type = 'boolean'
            else:
                data_type = 'string'

            columns.append({
                "name": clean_name,
                "data_type": data_type
            })

        tables_metadata.append({
            "name": table_name,
            "columns": columns
        })

        print(f"   ✅ Found {len(columns)} columns in {table_name}")

    return tables_metadata


def extract_table_metadata(token: str, dataset_id: str, include_hidden: bool = False) -> List[Dict[str, Any]]:
    """
    Extract table and column metadata from Power BI semantic model.

    Note: DMV functions (INFO.*, TMSCHEMA_*, SYSTEMRESTRICTSCHEMA) don't work
    with the REST API executeQueries endpoint.

    This function returns empty and the user should provide table names manually,
    or use the Admin Scanner API for full schema discovery.
    """
    print("\n🔄 Attempting to extract table metadata...")
    print("⚠️  Note: Automatic schema discovery via DMVs is not supported with REST API")
    print("⚠️  You need to either:")
    print("   1. Provide table names manually (see extract_table_metadata_manual)")
    print("   2. Use Power BI Admin Scanner API (requires admin permissions)")
    print("   3. Use XMLA endpoint with TMSCHEMA views (requires different connection)")

    return []

    # Process tables
    tables_dict = {}

    # INFO.TABLES columns: [TableName], [IsHidden], [Description] (if available)
    for _, table_row in tables_df.iterrows():
        table_name = table_row.get("[TableName]")
        if not table_name:
            continue

        table_hidden = table_row.get("[IsHidden]", False)

        # Skip hidden tables if requested
        if table_hidden and not include_hidden:
            continue

        tables_dict[table_name] = {
            "name": table_name,
            "columns": []
        }

    # Process columns
    # INFO.COLUMNS columns: [TableName], [ColumnName], [DataType], [IsHidden]
    for _, col_row in columns_df.iterrows():
        table_name = col_row.get("[TableName]")
        column_name = col_row.get("[ColumnName]")

        if not table_name or not column_name:
            continue

        # Skip if table not in our dict (was hidden)
        if table_name not in tables_dict:
            continue

        column_hidden = col_row.get("[IsHidden]", False)

        # Skip hidden columns if requested
        if column_hidden and not include_hidden:
            continue

        # Get data type - INFO.COLUMNS uses [DataType]
        column_type = col_row.get("[DataType]", "String")

        # Add column
        column_info = {
            "name": column_name,
            "data_type": map_dax_type_to_metadata_type(column_type)
        }

        tables_dict[table_name]["columns"].append(column_info)

    # Convert to list
    tables_list = list(tables_dict.values())

    # Remove tables with no columns (all were hidden)
    tables_list = [t for t in tables_list if len(t["columns"]) > 0]

    print(f"✅ Processed {len(tables_list)} tables")

    return tables_list


def extract_relationship_metadata(token: str, dataset_id: str) -> List[Dict[str, Any]]:
    """
    Extract relationship metadata from Power BI semantic model.

    Note: INFO.RELATIONSHIPS may not be available in all Power BI deployments.
    If this fails, relationships will be omitted from the output.
    """
    print("\n🔄 Extracting relationship metadata...")

    try:
        # Try to get relationships using INFO.RELATIONSHIPS
        # Note: This may not work in all environments
        dax_query = "EVALUATE INFO.RELATIONSHIPS"
        df = execute_dax_for_metadata(token, dataset_id, dax_query)
    except Exception as e:
        print(f"⚠️  INFO.RELATIONSHIPS not available: {str(e)}")
        return []

    if df.empty:
        print("⚠️  No relationships found")
        return []

    print(f"✅ Retrieved {len(df)} relationships")

    relationships = []
    # INFO.RELATIONSHIPS columns may vary, but typically include:
    # [FromTableName], [FromColumnName], [ToTableName], [ToColumnName]
    for _, row in df.iterrows():
        rel = {
            "fromTable": row.get("[FromTableName]", row.get("[FromTable]", "")),
            "fromColumn": row.get("[FromColumnName]", row.get("[FromColumn]", "")),
            "toTable": row.get("[ToTableName]", row.get("[ToTable]", "")),
            "toColumn": row.get("[ToColumnName]", row.get("[ToColumn]", "")),
        }

        # Only add if we have valid data
        if rel["fromTable"] and rel["toTable"]:
            relationships.append(rel)

    return relationships

# COMMAND ----------

# DBTITLE 1,Get Dataset Information
try:
    dataset_info = get_dataset_info(access_token, SEMANTIC_MODEL_ID)

    if dataset_info:
        print("\n" + "=" * 80)
        print("Dataset Information")
        print("=" * 80)
        print(f"Dataset Name: {dataset_info.get('name', 'N/A')}")
        print(f"Dataset ID: {dataset_info.get('id', 'N/A')}")
        print(f"Configured By: {dataset_info.get('configuredBy', 'N/A')}")
        print("=" * 80)

except Exception as e:
    print(f"⚠️  Could not retrieve dataset info: {str(e)}")
    # Continue anyway

# COMMAND ----------

# DBTITLE 1,Extract Metadata
try:
    # Extract tables and columns using manual approach (table names required)
    tables_metadata = extract_table_metadata_manual(
        token=access_token,
        dataset_id=SEMANTIC_MODEL_ID,
        table_names=TABLE_NAMES
    )

    # Build metadata structure
    metadata = {"tables": tables_metadata}

    # Extract relationships if requested
    if INCLUDE_RELATIONSHIPS:
        relationships = extract_relationship_metadata(
            token=access_token,
            dataset_id=SEMANTIC_MODEL_ID
        )

        if relationships:
            metadata["relationships"] = relationships

    print("\n" + "=" * 80)
    print("Metadata Extraction Summary")
    print("=" * 80)
    print(f"✅ Tables extracted: {len(tables_metadata)}")

    total_columns = sum(len(table["columns"]) for table in tables_metadata)
    print(f"✅ Total columns: {total_columns}")

    if INCLUDE_RELATIONSHIPS and "relationships" in metadata:
        print(f"✅ Relationships extracted: {len(metadata['relationships'])}")

    print("=" * 80)

except Exception as e:
    print(f"❌ Error extracting metadata: {str(e)}")
    raise

# COMMAND ----------

# DBTITLE 1,Display Metadata Structure
print("\n" + "=" * 80)
print("Extracted Metadata Structure")
print("=" * 80)

# Display each table with sample columns
for table in tables_metadata[:5]:  # Show first 5 tables
    print(f"\n📊 Table: {table['name']}")
    if table.get('description'):
        print(f"   Description: {table['description']}")
    print(f"   Columns ({len(table['columns'])}):")

    for col in table['columns'][:10]:  # Show first 10 columns per table
        col_str = f"      - {col['name']} ({col['data_type']})"
        if col.get('description'):
            col_str += f" - {col['description']}"
        print(col_str)

    if len(table['columns']) > 10:
        print(f"      ... and {len(table['columns']) - 10} more columns")

if len(tables_metadata) > 5:
    print(f"\n... and {len(tables_metadata) - 5} more tables")

# COMMAND ----------

# DBTITLE 1,Format Output
if OUTPUT_FORMAT == "python_dict":
    # Python dictionary format (easier to copy-paste into Python code)
    output_str = repr(metadata)
    print("\n" + "=" * 80)
    print("Metadata in Python Dictionary Format")
    print("=" * 80)
    print("Copy this into your Python code:\n")
    print(output_str)

elif OUTPUT_FORMAT == "json":
    # JSON format (pretty-printed)
    output_str = json.dumps(metadata, indent=2)
    print("\n" + "=" * 80)
    print("Metadata in JSON Format")
    print("=" * 80)
    print("Copy this into your PowerBITool configuration:\n")
    print(output_str)

# COMMAND ----------

# DBTITLE 1,Save to File (Optional)
# Uncomment to save metadata to a file
# output_filename = f"powerbi_metadata_{SEMANTIC_MODEL_ID}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
# with open(f"/dbfs/tmp/{output_filename}", "w") as f:
#     json.dump(metadata, f, indent=2)
# print(f"✅ Metadata saved to: /dbfs/tmp/{output_filename}")

# COMMAND ----------

# DBTITLE 1,Display for PowerBITool Usage
print("\n" + "=" * 80)
print("💡 Using This Metadata with PowerBITool")
print("=" * 80)
print("\nWhen running your crew, provide this as the 'metadata' parameter:\n")
print("Dataset Name:", dataset_info.get('name', 'your_dataset'))
print("\nMetadata (compact format for crew input):")
print("-" * 80)

# Compact format without descriptions for easier copying
compact_metadata = {
    "tables": [
        {
            "name": table["name"],
            "columns": [
                {"name": col["name"], "data_type": col["data_type"]}
                for col in table["columns"]
            ]
        }
        for table in tables_metadata
    ]
}

print(json.dumps(compact_metadata, separators=(',', ':')))
print("-" * 80)

print("\n📋 Example crew input parameters:")
print(f"""
dataset_name: {dataset_info.get('name', 'your_dataset')}

metadata: {json.dumps(compact_metadata, separators=(',', ':'))}
""")

# COMMAND ----------

# DBTITLE 1,Execution Summary
print("\n" + "=" * 80)
print("Execution Summary")
print("=" * 80)
print(f"✅ Metadata Extraction Completed Successfully")
print(f"   Execution Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print(f"   Semantic Model ID: {SEMANTIC_MODEL_ID}")
print(f"   Tables Extracted: {len(tables_metadata)}")
print(f"   Total Columns: {total_columns}")
if INCLUDE_RELATIONSHIPS and "relationships" in metadata:
    print(f"   Relationships: {len(metadata['relationships'])}")
print("=" * 80)

# COMMAND ----------

# DBTITLE 1,Return Results
result_summary = {
    "status": "success",
    "execution_time": datetime.now().isoformat(),
    "semantic_model_id": SEMANTIC_MODEL_ID,
    "dataset_name": dataset_info.get('name', 'unknown'),
    "tables_count": len(tables_metadata),
    "columns_count": total_columns,
    "metadata": metadata,
    "compact_metadata": compact_metadata
}

dbutils.notebook.exit(json.dumps(result_summary))
