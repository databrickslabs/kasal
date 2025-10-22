# Databricks notebook source
"""
Power BI Full Pipeline - Metadata Extraction, DAX Generation, and Execution

This notebook provides an end-to-end Power BI integration in one place:
1. Extract metadata from Power BI semantic model
2. Generate DAX query from natural language question using LLM
3. Execute the generated DAX query
4. Return all results

Required Parameters (via job_params):
- workspace_id: Power BI workspace ID
- semantic_model_id: Power BI semantic model/dataset ID
- question: Natural language question (e.g., "What is the total NSR per product?")
- auth_method: "device_code" or "service_principal" (default: "device_code")

For Service Principal auth, also provide:
- client_id: Azure AD application client ID
- tenant_id: Azure AD tenant ID
- client_secret: Service principal secret

For DAX Generation:
- databricks_host: Databricks workspace URL (e.g., "https://example.databricks.com")
- databricks_token: Databricks personal access token for LLM API
- model_name: LLM model to use (default: "databricks-meta-llama-3-1-405b-instruct")
- temperature: LLM temperature (default: 0.1)

Optional Parameters:
- sample_size: Number of rows to sample per table for type inference (default: 100)
- skip_metadata: Skip metadata extraction if metadata is provided directly (default: False)
- metadata: Pre-extracted metadata (JSON string) - use with skip_metadata=True
"""

# COMMAND ----------

# MAGIC %pip install azure-identity requests pandas

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# Import required libraries
import json
import re
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
DEFAULT_MODEL_NAME = "databricks-meta-llama-3-1-405b-instruct"
DEFAULT_TEMPERATURE = 0.1

try:
    # Get job parameters
    job_params = json.loads(dbutils.widgets.get("job_params"))

    # Extract required parameters
    WORKSPACE_ID = job_params.get("workspace_id")
    SEMANTIC_MODEL_ID = job_params.get("semantic_model_id")
    QUESTION = job_params.get("question")

    # Authentication configuration
    AUTH_METHOD = job_params.get("auth_method", "device_code")
    TENANT_ID = job_params.get("tenant_id", DEFAULT_TENANT_ID)
    CLIENT_ID = job_params.get("client_id", DEFAULT_CLIENT_ID)
    CLIENT_SECRET = job_params.get("client_secret")

    # Databricks API configuration for LLM
    DATABRICKS_HOST = job_params.get("databricks_host")
    DATABRICKS_TOKEN = job_params.get("databricks_token")
    MODEL_NAME = job_params.get("model_name", DEFAULT_MODEL_NAME)
    TEMPERATURE = job_params.get("temperature", DEFAULT_TEMPERATURE)

    # Optional parameters
    SAMPLE_SIZE = job_params.get("sample_size", 100)
    SKIP_METADATA = job_params.get("skip_metadata", False)
    METADATA_JSON = job_params.get("metadata")

    print("=" * 80)
    print("Power BI Full Pipeline - Metadata → DAX Generation → Execution")
    print("=" * 80)
    print(f"Execution Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Workspace ID: {WORKSPACE_ID}")
    print(f"Semantic Model ID: {SEMANTIC_MODEL_ID}")
    print(f"Question: {QUESTION}")
    print(f"Authentication Method: {AUTH_METHOD}")
    print(f"LLM Model: {MODEL_NAME}")
    print(f"Temperature: {TEMPERATURE}")
    print(f"Skip Metadata Extraction: {SKIP_METADATA}")
    print("=" * 80)

except Exception as e:
    print(f"❌ Error getting parameters: {str(e)}")
    print("\nRequired parameters in job_params:")
    print("- workspace_id: Power BI workspace ID")
    print("- semantic_model_id: Power BI dataset/semantic model ID")
    print("- question: Natural language question")
    print("- databricks_host: Databricks workspace URL")
    print("- databricks_token: Databricks personal access token")
    print("\nOptional parameters:")
    print("- auth_method: 'device_code' or 'service_principal' (default: 'device_code')")
    print("- model_name: LLM model (default: 'databricks-meta-llama-3-1-405b-instruct')")
    print("- temperature: LLM temperature (default: 0.1)")
    print("- sample_size: Rows to sample (default: 100)")
    print("- skip_metadata: Skip metadata extraction (default: False)")
    print("- metadata: Pre-extracted metadata JSON (use with skip_metadata=True)")
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

# DBTITLE 1,Power BI API Functions
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
    """Extract column metadata by querying actual data from each table."""
    print(f"\n🔄 Extracting metadata for {len(table_names)} tables...")
    print(f"   Using sample size: {sample_size} rows per table\n")

    tables_metadata = []

    for table_name in table_names:
        print(f"   📊 Processing table: {table_name}")

        # Query sample data to get column names and types
        query = f"EVALUATE TOPN({sample_size}, '{table_name}')"
        df = execute_dax_for_metadata(token, dataset_id, query)

        if df.empty:
            print(f"   ⚠️  Could not query table: {table_name} (may be empty or not exist)")
            continue

        # Extract column information from DataFrame
        columns = []
        for col_name in df.columns:
            # Remove the table name prefix and square brackets from column names
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

# DBTITLE 1,STEP 1: Extract Metadata (if not skipped)
if SKIP_METADATA and METADATA_JSON:
    print("\n" + "=" * 80)
    print("STEP 1: Using Pre-Extracted Metadata")
    print("=" * 80)
    metadata = json.loads(METADATA_JSON)
    tables_metadata = metadata.get("tables", [])
    print(f"✅ Loaded metadata with {len(tables_metadata)} tables")

    # Create compact metadata
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

    dataset_info = {"name": "pre-loaded"}
    total_columns = sum(len(table["columns"]) for table in tables_metadata)

else:
    print("\n" + "=" * 80)
    print("STEP 1: Extracting Metadata from Power BI")
    print("=" * 80)

    try:
        # Get dataset info
        dataset_info = get_dataset_info(access_token, SEMANTIC_MODEL_ID)

        if dataset_info:
            print(f"Dataset Name: {dataset_info.get('name', 'N/A')}")

        # Discover tables
        print("\n🔄 Discovering tables in dataset...")
        tables_df = execute_dax_for_metadata(access_token, SEMANTIC_MODEL_ID, "EVALUATE INFO.VIEW.TABLES()")

        if tables_df.empty:
            raise ValueError("No tables found in dataset")

        # Extract unique table names
        table_names = list(set(tables_df['[Name]']))
        print(f"Found {len(table_names)} tables: {', '.join(table_names[:5])}")

        # Extract metadata
        tables_metadata = extract_table_metadata_from_data(
            token=access_token,
            dataset_id=SEMANTIC_MODEL_ID,
            table_names=table_names,
            sample_size=SAMPLE_SIZE
        )

        # Build metadata structure
        metadata = {"tables": tables_metadata}

        # Create compact metadata
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

        total_columns = sum(len(table["columns"]) for table in tables_metadata)

        print(f"\n✅ STEP 1 Complete: Extracted {len(tables_metadata)} tables, {total_columns} columns")

    except Exception as e:
        print(f"❌ Error in metadata extraction: {str(e)}")
        raise

# COMMAND ----------

# DBTITLE 1,Metadata Formatting for LLM
def format_metadata_for_llm(metadata: Dict[str, Any]) -> str:
    """Format metadata as a readable string for LLM context."""
    tables = metadata.get('tables', [])
    if not tables:
        return "No metadata available"

    output = "Power BI Dataset Structure:\n\n"

    for table in tables:
        table_name = table.get('name', 'Unknown')
        columns = table.get('columns', [])

        output += f"Table: {table_name}\n"

        if columns:
            output += "Columns:\n"
            for col in columns:
                col_name = col.get('name', 'Unknown')
                col_type = col.get('data_type', 'Unknown')
                output += f"  - {col_name} ({col_type})\n"

        output += "\n"

    return output

# COMMAND ----------

# DBTITLE 1,DAX Query Cleaning Utility
def clean_dax_query(dax_query: str) -> str:
    """Remove HTML/XML tags and other artifacts from DAX queries."""
    # Remove HTML/XML tags like <oii>, </oii>, etc.
    cleaned = re.sub(r"<[^>]+>", "", dax_query)
    # Collapse extra whitespace
    cleaned = " ".join(cleaned.split())
    return cleaned

# COMMAND ----------

# DBTITLE 1,STEP 2: Generate DAX Query from Question
print("\n" + "=" * 80)
print("STEP 2: Generating DAX Query from Natural Language Question")
print("=" * 80)
print(f"Question: {QUESTION}")

try:
    # Format metadata for LLM
    metadata_str = format_metadata_for_llm(metadata)

    # Build prompt for DAX generation
    prompt = f"""You are a Power BI DAX expert. Generate a DAX query to answer the following question.

Available dataset structure:
{metadata_str}

User question: {QUESTION}

IMPORTANT RULES:
1. Generate only the DAX query without any explanation or markdown
2. Do NOT use any HTML or XML tags in the query
3. Do NOT use angle brackets < or > except for DAX operators
4. Use only valid DAX syntax
5. Reference only columns and measures that exist in the schema
6. The query should be executable as-is
7. Use proper DAX functions like EVALUATE, SUMMARIZE, FILTER, CALCULATE, etc.
8. Start the query with EVALUATE

Example format:
EVALUATE SUMMARIZE(Sales, Product[Category], "Total Revenue", SUM(Sales[Amount]))

Now generate the DAX query for the user's question:"""

    print(f"\n🔄 Calling LLM: {MODEL_NAME}")
    print(f"   Temperature: {TEMPERATURE}")

    # Call Databricks LLM API
    llm_url = f"{DATABRICKS_HOST}/serving-endpoints/{MODEL_NAME}/invocations"
    llm_headers = {
        "Authorization": f"Bearer {DATABRICKS_TOKEN}",
        "Content-Type": "application/json"
    }

    llm_body = {
        "messages": [
            {"role": "user", "content": prompt}
        ],
        "temperature": TEMPERATURE,
        "max_tokens": 2000
    }

    print(f"   Endpoint: {llm_url}")
    llm_response = requests.post(llm_url, headers=llm_headers, json=llm_body, timeout=120)

    if llm_response.status_code != 200:
        raise Exception(f"LLM API call failed: {llm_response.text}")

    llm_result = llm_response.json()

    # Extract content from response
    if "choices" in llm_result and len(llm_result["choices"]) > 0:
        raw_dax = llm_result["choices"][0]["message"]["content"]
    else:
        raise Exception(f"Unexpected LLM response format: {llm_result}")

    print("✅ LLM response received")

    # Clean the response
    cleaned_dax = clean_dax_query(raw_dax)

    # Remove markdown code blocks if present
    if "```" in cleaned_dax:
        parts = cleaned_dax.split("```")
        for part in parts:
            if "EVALUATE" in part.upper():
                cleaned_dax = part.strip()
                # Remove language identifier if present
                if cleaned_dax.startswith("dax\n") or cleaned_dax.startswith("DAX\n"):
                    cleaned_dax = cleaned_dax[4:].strip()
                break

    # Ensure query starts with EVALUATE
    if not cleaned_dax.strip().upper().startswith("EVALUATE"):
        lines = cleaned_dax.split("\n")
        for i, line in enumerate(lines):
            if "EVALUATE" in line.upper():
                cleaned_dax = "\n".join(lines[i:])
                break

    DAX_QUERY = cleaned_dax.strip()

    print("\n" + "-" * 80)
    print("Generated DAX Query:")
    print("-" * 80)
    print(DAX_QUERY)
    print("-" * 80)

    print(f"\n✅ STEP 2 Complete: DAX query generated successfully")

except Exception as e:
    print(f"❌ Error in DAX generation: {str(e)}")
    raise

# COMMAND ----------

# DBTITLE 1,STEP 3: Execute DAX Query
print("\n" + "=" * 80)
print("STEP 3: Executing DAX Query")
print("=" * 80)

def execute_dax_query(token: str, dataset_id: str, dax_query: str) -> pd.DataFrame:
    """Execute a DAX query against the Power BI dataset using REST API."""
    url = f"https://api.powerbi.com/v1.0/myorg/datasets/{dataset_id}/executeQueries"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }

    body = {
        "queries": [
            {
                "query": dax_query
            }
        ],
        "serializerSettings": {
            "includeNulls": True
        }
    }

    print(f"\n🔄 Executing DAX query...")
    print(f"   Endpoint: {url}")
    print(f"   Query preview: {dax_query[:100]}...")

    response = requests.post(url, headers=headers, json=body, timeout=60)
    print(f"   Response status: {response.status_code}")

    if response.status_code == 200:
        results = response.json().get("results", [])

        if results and results[0].get("tables"):
            rows = results[0]["tables"][0].get("rows", [])

            if rows:
                df = pd.DataFrame(rows)
                print(f"✅ Query successful: {len(df)} rows returned")
                print(f"   Columns: {list(df.columns)}")
                return df
            else:
                print("⚠️  Query returned no rows")
                return pd.DataFrame()
        else:
            print("⚠️  No tables in response")
            return pd.DataFrame()
    else:
        print(f"❌ Query failed: {response.text}")
        return pd.DataFrame()

try:
    df_result = execute_dax_query(access_token, SEMANTIC_MODEL_ID, DAX_QUERY)

    if not df_result.empty:
        print("\n" + "=" * 80)
        print("Query Results")
        print("=" * 80)
        print(f"Total rows: {len(df_result)}")
        print(f"Columns: {list(df_result.columns)}")
        print("=" * 80)

        # Display first few rows
        print("\nSample Results (First 10 rows):")
        print("-" * 80)
        display(df_result.head(10))

        if len(df_result) > 10:
            print(f"\n... and {len(df_result) - 10} more rows")

        # Convert to Spark DataFrame
        print("\n🔄 Converting results to Spark DataFrame...")
        spark_df = spark.createDataFrame(df_result)
        print(f"✅ Spark DataFrame created successfully")

        print(f"\n✅ STEP 3 Complete: Query executed, {len(df_result)} rows returned")

    else:
        print("⚠️  Query returned empty DataFrame")
        spark_df = None
        print(f"\n⚠️  STEP 3 Complete: No results returned")

except Exception as e:
    print(f"❌ Error executing DAX query: {str(e)}")
    df_result = pd.DataFrame()
    spark_df = None
    raise

# COMMAND ----------

# DBTITLE 1,Execution Summary
print("\n" + "=" * 80)
print("FULL PIPELINE EXECUTION SUMMARY")
print("=" * 80)
print(f"✅ Pipeline Completed Successfully")
print(f"   Execution Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print(f"   Workspace ID: {WORKSPACE_ID}")
print(f"   Semantic Model ID: {SEMANTIC_MODEL_ID}")
print(f"   Dataset Name: {dataset_info.get('name', 'N/A')}")
print("")
print(f"STEP 1 - Metadata Extraction:")
print(f"   Tables: {len(tables_metadata)}")
print(f"   Total Columns: {total_columns}")
print("")
print(f"STEP 2 - DAX Generation:")
print(f"   Question: {QUESTION}")
print(f"   Model: {MODEL_NAME}")
print(f"   Generated Query Length: {len(DAX_QUERY)} characters")
print("")
print(f"STEP 3 - DAX Execution:")
print(f"   Authentication: {AUTH_METHOD}")
print(f"   Rows Returned: {len(df_result) if not df_result.empty else 0}")
print(f"   Columns: {list(df_result.columns) if not df_result.empty else 'N/A'}")
print("=" * 80)

# COMMAND ----------

# DBTITLE 1,Return Results as JSON
# Convert DataFrame to JSON for output
if not df_result.empty:
    # Convert to list of dictionaries
    result_data = df_result.to_dict(orient='records')
else:
    result_data = []

# Build complete result summary
result_summary = {
    "status": "success",
    "execution_time": datetime.now().isoformat(),
    "pipeline_steps": {
        "step_1_metadata": {
            "tables_count": len(tables_metadata),
            "columns_count": total_columns,
            "metadata": metadata,
            "compact_metadata": compact_metadata
        },
        "step_2_dax_generation": {
            "question": QUESTION,
            "model_name": MODEL_NAME,
            "temperature": TEMPERATURE,
            "generated_dax": DAX_QUERY
        },
        "step_3_execution": {
            "workspace_id": WORKSPACE_ID,
            "semantic_model_id": SEMANTIC_MODEL_ID,
            "auth_method": AUTH_METHOD,
            "rows_returned": len(df_result) if not df_result.empty else 0,
            "columns": list(df_result.columns) if not df_result.empty else [],
            "result_data": result_data[:1000]  # Limit to first 1000 rows for JSON output
        }
    },
    "dataset_name": dataset_info.get('name', 'unknown')
}

# Exit with complete results
dbutils.notebook.exit(json.dumps(result_summary))
