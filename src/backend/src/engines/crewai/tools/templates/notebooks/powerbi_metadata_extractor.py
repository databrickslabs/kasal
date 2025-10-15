# Databricks notebook source
"""
Power BI Metadata Extractor

This notebook extracts schema metadata from Power BI semantic models/datasets
and formats it for use with PowerBITool in Kasal.

APPROACH:
1. Discovers tables using INFO.VIEW.TABLES() DMV function
2. Queries actual table data using TOPN to get sample rows
3. Infers column names and data types from the response

The output is a JSON structure containing tables, columns, and data types
that can be directly used as the 'metadata' parameter in PowerBITool.

Required Parameters (via job_params):
- workspace_id: Power BI workspace ID
- semantic_model_id: Power BI semantic model/dataset ID
- auth_method: "device_code" or "service_principal" (default: "device_code")

For Service Principal auth, also provide:
- client_id: Azure AD application client ID
- tenant_id: Azure AD tenant ID
- client_secret: Service principal secret

Optional Parameters:
- sample_size: Number of rows to sample per table for type inference (default: 100)
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
    # Get job parameters
    job_params = json.loads(dbutils.widgets.get("job_params"))

    # Extract required parameters
    WORKSPACE_ID = job_params.get("workspace_id")
    SEMANTIC_MODEL_ID = job_params.get("semantic_model_id")

    # Authentication configuration
    AUTH_METHOD = job_params.get("auth_method", "device_code")
    TENANT_ID = job_params.get("tenant_id", DEFAULT_TENANT_ID)
    CLIENT_ID = job_params.get("client_id", DEFAULT_CLIENT_ID)
    CLIENT_SECRET = job_params.get("client_secret")

    # Optional parameters
    SAMPLE_SIZE = job_params.get("sample_size", 100)
    OUTPUT_FORMAT = job_params.get("output_format", "json")

    print("=" * 80)
    print("Power BI Metadata Extractor")
    print("=" * 80)
    print(f"Execution Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Workspace ID: {WORKSPACE_ID}")
    print(f"Semantic Model ID: {SEMANTIC_MODEL_ID}")
    print(f"Authentication Method: {AUTH_METHOD}")
    print(f"Sample Size: {SAMPLE_SIZE} rows per table")
    print(f"Output Format: {OUTPUT_FORMAT}")
    print("=" * 80)

except Exception as e:
    print(f"❌ Error getting parameters: {str(e)}")
    print("\nRequired parameters in job_params:")
    print("- workspace_id: Power BI workspace ID")
    print("- semantic_model_id: Power BI dataset/semantic model ID")
    print("\nOptional parameters:")
    print("- auth_method: 'device_code' or 'service_principal' (default: 'device_code')")
    print("- sample_size: Number of rows to sample (default: 100)")
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
    else:  # device_code (default)
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


def extract_table_metadata_from_data(token: str, dataset_id: str, table_names: List[str], sample_size: int = 100) -> List[Dict[str, Any]]:
    """
    Extract column metadata by querying actual data from each table.

    Uses Power BI REST API executeQueries endpoint with TOPN to get sample data,
    then infers column names and data types from the response.

    Args:
        token: Access token for Power BI API
        dataset_id: Power BI dataset/semantic model ID
        table_names: List of table names to extract metadata for
        sample_size: Number of rows to sample (default: 100)

    Returns:
        List of table metadata dictionaries with columns and data types
    """
    print(f"\n🔄 Extracting metadata for {len(table_names)} tables...")
    print(f"   Using sample size: {sample_size} rows per table\n")

    tables_metadata = []

    for table_name in table_names:
        print(f"   📊 Processing table: {table_name}")

        # Query sample data to get column names and types
        # Using TOPN to get first N rows from the table
        query = f"EVALUATE TOPN({sample_size}, '{table_name}')"
        df = execute_dax_for_metadata(token, dataset_id, query)

        if df.empty:
            print(f"   ⚠️  Could not query table: {table_name} (may be empty or not exist)")
            continue

        # Extract column information from DataFrame
        columns = []
        for col_name in df.columns:
            # Remove the table name prefix and square brackets from column names
            # Power BI returns columns as [TableName[ColumnName]] or [ColumnName]
            clean_name = col_name.strip(table_name).strip('[').strip(']')

            # Infer data type from pandas dtype based on actual data
            dtype = str(df[col_name].dtype)

            if 'object' in dtype or 'string' in dtype:
                data_type = 'string'
            elif 'int' in dtype:
                data_type = 'int'
            elif 'float' in dtype or 'decimal' in dtype:
                data_type = 'decimal'
            elif 'datetime' in dtype:
                data_type = 'datetime'
            elif 'bool' in dtype:
                data_type = 'boolean'
            else:
                # Default to string for unknown types
                data_type = 'string'

            columns.append({
                "name": clean_name,
                "data_type": data_type
            })

        tables_metadata.append({
            "name": table_name,
            "columns": columns
        })

        print(f"   ✅ Found {len(columns)} columns in '{table_name}'")

    print(f"\n✅ Successfully processed {len(tables_metadata)}/{len(table_names)} tables")

    return tables_metadata

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

# DBTITLE 1,Discover Tables
try:
    print("\n🔄 Discovering tables in dataset...")
    tables_df = execute_dax_for_metadata(access_token, SEMANTIC_MODEL_ID, "EVALUATE INFO.VIEW.TABLES()")

    if tables_df.empty:
        print("⚠️  Could not discover tables automatically")
        print("   The notebook will exit. Please provide table_names manually if needed.")
        raise ValueError("No tables found in dataset")

    # Extract unique table names from the [Name] column
    table_names = list(set(tables_df['[Name]']))

    print("\n" + "=" * 80)
    print("Tables Discovered")
    print("=" * 80)
    print(f"Found {len(table_names)} tables:")
    for table_name in table_names:
        print(f"  - {table_name}")
    print("=" * 80)

except Exception as e:
    print(f"❌ Error discovering tables: {str(e)}")
    raise

# COMMAND ----------

# DBTITLE 1,Extract Metadata
try:
    # Extract tables and columns by querying actual data
    # This approach works reliably with the REST API executeQueries endpoint
    tables_metadata = extract_table_metadata_from_data(
        token=access_token,
        dataset_id=SEMANTIC_MODEL_ID,
        table_names=table_names,
        sample_size=SAMPLE_SIZE
    )

    # Build metadata structure
    metadata = {"tables": tables_metadata}

    print("\n" + "=" * 80)
    print("Metadata Extraction Summary")
    print("=" * 80)
    print(f"✅ Tables extracted: {len(tables_metadata)}")

    total_columns = sum(len(table["columns"]) for table in tables_metadata)
    print(f"✅ Total columns: {total_columns}")
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
    print(f"   Columns ({len(table['columns'])}):")

    for col in table['columns'][:10]:  # Show first 10 columns per table
        print(f"      - {col['name']} ({col['data_type']})")

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

else:  # json (default)
    # JSON format (pretty-printed)
    output_str = json.dumps(metadata, indent=2)
    print("\n" + "=" * 80)
    print("Metadata in JSON Format")
    print("=" * 80)
    print("Copy this into your PowerBITool configuration:\n")
    print(output_str)

# COMMAND ----------

# DBTITLE 1,Display for PowerBITool Usage
print("\n" + "=" * 80)
print("💡 Using This Metadata with PowerBITool")
print("=" * 80)
print("\nWhen running your crew, provide this as the 'metadata' parameter:\n")
print("Dataset Name:", dataset_info.get('name', 'your_dataset'))
print("\nMetadata (compact format for crew input):")
print("-" * 80)

# Compact format without extra whitespace
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
