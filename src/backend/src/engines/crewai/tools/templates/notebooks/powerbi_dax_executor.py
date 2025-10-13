# Databricks notebook source
"""
Power BI DAX Query Executor

This notebook executes DAX queries against Power BI datasets using the Power BI REST API.
Supports both Device Code Flow (DCF) and Service Principal authentication.

Required Parameters (via job_params):
- dax_statement: DAX query to execute
- workspace_id: Power BI workspace ID
- semantic_model_id: Power BI semantic model/dataset ID
- auth_method: "device_code" or "service_principal" (default: "device_code")

For Service Principal auth, also provide:
- client_id: Azure AD application client ID
- tenant_id: Azure AD tenant ID
- client_secret: Service principal secret

Configuration (constants):
- TENANT_ID: Your Azure AD tenant ID (can be overridden by job_params)
- CLIENT_ID: Power BI public client ID for DCF (can be overridden by job_params)
"""

# COMMAND ----------

# MAGIC %pip install azure-identity requests pandas

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# Import required libraries
import json
import time
import requests
import sys
import pandas as pd
from datetime import datetime
from typing import Dict, List, Any, Optional, Tuple
from azure.identity import InteractiveBrowserCredential, DeviceCodeCredential, ClientSecretCredential

# COMMAND ----------

# DBTITLE 1,Configuration - Get Job Parameters
# Default configuration (can be overridden by job parameters)
DEFAULT_TENANT_ID = "9f37a392-f0ae-4280-9796-f1864a10effc"  # Your tenant ID
DEFAULT_CLIENT_ID = "1950a258-227b-4e31-a9cf-717495945fc2"  # Power BI public client for DCF

try:
    # Get job parameters
    job_params = json.loads(dbutils.widgets.get("job_params"))

    # Extract DAX query and IDs
    DAX_QUERY = job_params.get("dax_statement")
    WORKSPACE_ID = job_params.get("workspace_id")
    SEMANTIC_MODEL_ID = job_params.get("semantic_model_id")

    # Authentication configuration
    AUTH_METHOD = job_params.get("auth_method", "device_code")  # "device_code" or "service_principal"
    TENANT_ID = job_params.get("tenant_id", DEFAULT_TENANT_ID)
    CLIENT_ID = job_params.get("client_id", DEFAULT_CLIENT_ID)

    # Service Principal credentials (if using service_principal auth)
    CLIENT_SECRET = job_params.get("client_secret")

    print("=" * 80)
    print("Power BI DAX Query Executor")
    print("=" * 80)
    print(f"Execution Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Workspace ID: {WORKSPACE_ID}")
    print(f"Semantic Model ID: {SEMANTIC_MODEL_ID}")
    print(f"Authentication Method: {AUTH_METHOD}")
    print(f"Tenant ID: {TENANT_ID}")
    print(f"Client ID: {CLIENT_ID[:8]}..." if CLIENT_ID and len(CLIENT_ID) > 8 else "Client ID: ***")
    print("=" * 80)

except Exception as e:
    print(f"❌ Error getting parameters: {str(e)}")
    print("\nRequired parameters in job_params:")
    print("- dax_statement: DAX query to execute")
    print("- workspace_id: Power BI workspace ID")
    print("- semantic_model_id: Power BI dataset/semantic model ID")
    print("- auth_method: 'device_code' or 'service_principal' (optional, default: 'device_code')")
    print("\nFor Service Principal auth, also provide:")
    print("- client_id: Azure AD application client ID")
    print("- tenant_id: Azure AD tenant ID")
    print("- client_secret: Service principal secret")
    raise

# COMMAND ----------

# DBTITLE 1,Display DAX Query
print("DAX Query to Execute:")
print("-" * 80)
print(DAX_QUERY)
print("-" * 80)

# COMMAND ----------

# DBTITLE 1,Authentication Functions
def generate_token_device_code(tenant_id: str, client_id: str) -> str:
    """
    Generate token using device code flow (DCF).

    This is an interactive authentication method where the user needs to:
    1. Navigate to microsoft.com/devicelogin
    2. Enter the code displayed in the output
    3. Sign in with their Azure AD credentials

    ⚠️  Not recommended for production - use Service Principal instead

    Args:
        tenant_id: Azure AD tenant ID
        client_id: Power BI public client ID

    Returns:
        str: Access token for Power BI API
    """
    try:
        credential = DeviceCodeCredential(
            client_id=client_id,
            tenant_id=tenant_id,
        )

        print("\n🔄 Initiating Device Code Flow authentication...")
        print("⚠️  Follow the instructions above to authenticate")

        # Get token for Power BI API
        token = credential.get_token("https://analysis.windows.net/powerbi/api/.default")

        print("✅ Token generated successfully")
        print(f"   Token length: {len(token.token)} characters")
        print(f"   Token expires at: {datetime.fromtimestamp(token.expires_on).strftime('%Y-%m-%d %H:%M:%S')}")

        return token.token

    except Exception as e:
        print(f"❌ Token generation failed: {str(e)}")
        raise


def generate_token_service_principal(tenant_id: str, client_id: str, client_secret: str) -> str:
    """
    Generate token using Service Principal (non-interactive).

    This is the recommended method for production as it doesn't require user interaction.

    Args:
        tenant_id: Azure AD tenant ID
        client_id: Service Principal application ID
        client_secret: Service Principal secret

    Returns:
        str: Access token for Power BI API
    """
    try:
        print("\n🔄 Authenticating with Service Principal...")

        credential = ClientSecretCredential(
            tenant_id=tenant_id,
            client_id=client_id,
            client_secret=client_secret
        )

        # Get token for Power BI API
        token = credential.get_token("https://analysis.windows.net/powerbi/api/.default")

        print("✅ Token generated successfully")
        print(f"   Token length: {len(token.token)} characters")
        print(f"   Token expires at: {datetime.fromtimestamp(token.expires_on).strftime('%Y-%m-%d %H:%M:%S')}")

        return token.token

    except Exception as e:
        print(f"❌ Service Principal authentication failed: {str(e)}")
        print("\nTroubleshooting:")
        print("1. Verify Service Principal has 'Read' permissions in Power BI workspace")
        print("2. Check that client_id, tenant_id, and client_secret are correct")
        print("3. Ensure 'Service principals can access Power BI APIs' is enabled in tenant settings")
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
    """
    Get metadata about the dataset to verify connection.

    Args:
        token: Access token for Power BI API
        dataset_id: Power BI dataset/semantic model ID

    Returns:
        dict: Dataset metadata
    """
    url = f"https://api.powerbi.com/v1.0/myorg/datasets/{dataset_id}"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }

    print(f"🔄 Fetching dataset metadata...")
    response = requests.get(url, headers=headers)
    print(f"   Response status: {response.status_code}")

    if response.status_code == 200:
        print("✅ Successfully connected to dataset")
        return response.json()
    else:
        print(f"❌ Failed to connect: {response.text}")
        return {}


def execute_dax_query(token: str, dataset_id: str, dax_query: str) -> pd.DataFrame:
    """
    Execute a DAX query against the Power BI dataset using REST API.

    Args:
        token: Access token for Power BI API
        dataset_id: Power BI dataset/semantic model ID
        dax_query: DAX query to execute

    Returns:
        pd.DataFrame: Query results as pandas DataFrame
    """
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
        print("\nTroubleshooting:")
        print("1. Verify DAX query syntax is correct")
        print("2. Check table and column names exist in the dataset")
        print("3. Test the query in Power BI Desktop first")
        print("4. Verify token has not expired")
        return pd.DataFrame()

# COMMAND ----------

# DBTITLE 1,Verify Dataset Connection
try:
    dataset_info = get_dataset_info(access_token, SEMANTIC_MODEL_ID)

    if dataset_info:
        print("\n" + "=" * 80)
        print("Dataset Information")
        print("=" * 80)
        print(f"Dataset Name: {dataset_info.get('name', 'N/A')}")
        print(f"Dataset ID: {dataset_info.get('id', 'N/A')}")
        print(f"Is Refreshable: {dataset_info.get('isRefreshable', 'N/A')}")
        print(f"Configured By: {dataset_info.get('configuredBy', 'N/A')}")
        print("=" * 80)
    else:
        print("⚠️  Could not retrieve dataset information")

except Exception as e:
    print(f"❌ Error getting dataset info: {str(e)}")
    # Continue anyway - dataset info is optional

# COMMAND ----------

# DBTITLE 1,Execute DAX Query
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
    else:
        print("⚠️  Query returned empty DataFrame")

except Exception as e:
    print(f"❌ Error executing DAX query: {str(e)}")
    raise

# COMMAND ----------

# DBTITLE 1,Convert to Spark DataFrame
try:
    if not df_result.empty:
        print("\n🔄 Converting results to Spark DataFrame...")

        # Convert pandas DataFrame to Spark DataFrame
        spark_df = spark.createDataFrame(df_result)

        print(f"✅ Spark DataFrame created successfully")
        print(f"\nSchema:")
        spark_df.printSchema()

        # Display Spark DataFrame
        print("\nSpark DataFrame Preview:")
        display(spark_df)

    else:
        print("⚠️  No data to convert to Spark DataFrame")
        spark_df = None

except Exception as e:
    print(f"❌ Error converting to Spark DataFrame: {str(e)}")
    print("   Displaying pandas DataFrame instead")
    spark_df = None

# COMMAND ----------

# DBTITLE 1,Save Results (Optional)
# Uncomment to save results to a Delta table
# if spark_df is not None:
#     table_name = f"powerbi_dax_results_{SEMANTIC_MODEL_ID}"
#     spark_df.write.mode("overwrite").saveAsTable(table_name)
#     print(f"✅ Results saved to table: {table_name}")

# COMMAND ----------

# DBTITLE 1,Execution Summary
print("\n" + "=" * 80)
print("Execution Summary")
print("=" * 80)
print(f"✅ DAX Query Executed Successfully")
print(f"   Execution Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print(f"   Workspace ID: {WORKSPACE_ID}")
print(f"   Semantic Model ID: {SEMANTIC_MODEL_ID}")
print(f"   Authentication: {AUTH_METHOD}")
print(f"   Rows Returned: {len(df_result) if not df_result.empty else 0}")
print(f"   Columns: {list(df_result.columns) if not df_result.empty else 'N/A'}")
print("=" * 80)

# COMMAND ----------

# DBTITLE 1,Return Results as JSON
# Return execution summary for downstream processing
result_summary = {
    "status": "success",
    "execution_time": datetime.now().isoformat(),
    "workspace_id": WORKSPACE_ID,
    "semantic_model_id": SEMANTIC_MODEL_ID,
    "auth_method": AUTH_METHOD,
    "rows_returned": len(df_result) if not df_result.empty else 0,
    "columns": list(df_result.columns) if not df_result.empty else [],
    "dax_query": DAX_QUERY
}

dbutils.notebook.exit(json.dumps(result_summary))
