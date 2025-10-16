# Databricks notebook source
"""
Test Notebook - Job Output Retrieval

This notebook demonstrates how to retrieve the output from a completed Databricks job
using the run_id. This is useful for fetching results from jobs that exit with
dbutils.notebook.exit(json.dumps(result_data)).

Two approaches are shown:
1. Direct Databricks SDK approach (what JobMetadataFetcherTool uses internally)
2. How to parse and extract specific fields from the output

Required Parameters (via job_params):
- run_id: The Databricks job run ID to fetch output from
- task_key: (Optional) For multi-task jobs, specify which task's output to retrieve
- extract_key: (Optional) Specific key to extract from the result (e.g., 'metadata', 'compact_metadata')

Example usage:
{
  "run_id": "720546100105818",
  "task_key": "execute_notebook",
  "extract_key": "compact_metadata"
}
"""

# COMMAND ----------

# MAGIC %pip install databricks-sdk

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# Import required libraries
import json
from datetime import datetime
from typing import Optional
from databricks.sdk import WorkspaceClient

# COMMAND ----------

# DBTITLE 1,Configuration - Get Job Parameters
try:
    # Get job parameters
    job_params = json.loads(dbutils.widgets.get("job_params"))

    # Extract parameters
    RUN_ID = job_params.get("run_id")
    TASK_KEY = job_params.get("task_key")  # Optional for multi-task jobs
    EXTRACT_KEY = job_params.get("extract_key")  # Optional

    if not RUN_ID:
        raise ValueError("run_id is required")

    print("=" * 80)
    print("Job Output Retrieval Test")
    print("=" * 80)
    print(f"Execution Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Run ID: {RUN_ID}")
    print(f"Task Key: {TASK_KEY if TASK_KEY else '(auto-detect for multi-task jobs)'}")
    print(f"Extract Key: {EXTRACT_KEY if EXTRACT_KEY else '(none - will return full output)'}")
    print("=" * 80)

except Exception as e:
    print(f"❌ Error getting parameters: {str(e)}")
    print("\nRequired parameters in job_params:")
    print("- run_id: Databricks job run ID")
    print("\nOptional parameters:")
    print("- task_key: Task key for multi-task jobs (e.g., 'execute_notebook')")
    print("- extract_key: Specific key to extract (e.g., 'metadata', 'compact_metadata')")
    raise

# COMMAND ----------

# DBTITLE 1,Initialize Databricks Workspace Client
print("\n🔄 Initializing Databricks Workspace Client...")

try:
    # Initialize workspace client (automatically uses current authentication)
    workspace_client = WorkspaceClient()

    # Get current user info to verify authentication
    current_user = workspace_client.current_user.me()
    print(f"✅ Authenticated as: {current_user.user_name}")

except Exception as e:
    print(f"❌ Failed to initialize workspace client: {str(e)}")
    raise

# COMMAND ----------

# DBTITLE 1,Approach 1: Direct SDK Method (What JobMetadataFetcherTool Uses)
print("\n" + "=" * 80)
print("APPROACH 1: Direct Databricks SDK Method")
print("=" * 80)
print("This is what JobMetadataFetcherTool uses internally.\n")

try:
    print(f"🔄 Fetching output from run_id: {RUN_ID}")

    # First, check if this is a multi-task job
    run_info = workspace_client.jobs.get_run(int(RUN_ID))

    # Check if job has multiple tasks
    if run_info.tasks and len(run_info.tasks) > 1:
        print(f"📊 Multi-task job detected with {len(run_info.tasks)} tasks:")
        for task in run_info.tasks:
            print(f"   - {task.task_key}: {task.state.life_cycle_state}")

        # If no task_key specified, use the first task
        if not TASK_KEY:
            TASK_KEY = run_info.tasks[0].task_key
            print(f"\n⚠️  No task_key specified - auto-detecting first task: '{TASK_KEY}'")

        # Find the task run
        task_run = None
        for task in run_info.tasks:
            if task.task_key == TASK_KEY:
                task_run = task
                break

        if not task_run:
            available_tasks = [t.task_key for t in run_info.tasks]
            raise ValueError(f"Task '{TASK_KEY}' not found. Available tasks: {', '.join(available_tasks)}")

        # Get output from the specific task run
        print(f"🔄 Fetching output from task '{TASK_KEY}' (task_run_id: {task_run.run_id})")
        output = workspace_client.jobs.get_run_output(task_run.run_id)

    elif run_info.tasks and len(run_info.tasks) == 1:
        # Single task job - get output from the task run
        task_run = run_info.tasks[0]
        TASK_KEY = task_run.task_key
        print(f"📊 Single task job detected: '{TASK_KEY}'")
        print(f"🔄 Fetching output from task run_id: {task_run.run_id}")
        output = workspace_client.jobs.get_run_output(task_run.run_id)

    else:
        # Legacy single-task job without tasks array
        print(f"📊 Legacy job format detected (no tasks array)")
        print(f"🔄 Fetching output directly from run_id: {RUN_ID}")
        output = workspace_client.jobs.get_run_output(int(RUN_ID))

    print(f"✅ Successfully retrieved job run output")

    # Check if notebook output exists
    if not output.notebook_output:
        print("⚠️  No notebook output found")
        print("   This job may not have used dbutils.notebook.exit()")
        result_data = None
    elif not output.notebook_output.result:
        print("⚠️  Notebook output exists but result is empty")
        print("   The notebook may have exited without data")
        result_data = None
    else:
        print(f"✅ Notebook output result found")

        # Parse the result (it's a JSON string)
        result_data = json.loads(output.notebook_output.result)

        print(f"\n📊 Result structure:")
        print(f"   Type: {type(result_data)}")
        if isinstance(result_data, dict):
            print(f"   Keys: {list(result_data.keys())}")

        print(f"\n📝 Full result:")
        print("-" * 80)
        print(json.dumps(result_data, indent=2))
        print("-" * 80)

except ValueError as e:
    print(f"❌ Error: {str(e)}")
    result_data = None
    raise

except Exception as e:
    print(f"❌ Error fetching job output: {str(e)}")
    print("\nPossible reasons:")
    print("1. Run ID does not exist")
    print("2. Job has not completed yet")
    print("3. You don't have permission to access this run")
    print("4. The job failed before calling dbutils.notebook.exit()")
    print("5. For multi-task jobs, wrong task_key specified")
    result_data = None
    raise

# COMMAND ----------

# DBTITLE 1,Approach 2: Extract Specific Key (If Requested)
if result_data and EXTRACT_KEY:
    print("\n" + "=" * 80)
    print("APPROACH 2: Extract Specific Key")
    print("=" * 80)
    print(f"Attempting to extract key: '{EXTRACT_KEY}'\n")

    try:
        if not isinstance(result_data, dict):
            print(f"⚠️  Cannot extract key - result is not a dictionary")
            print(f"   Result type: {type(result_data)}")
            extracted_data = None
        elif EXTRACT_KEY not in result_data:
            print(f"❌ Key '{EXTRACT_KEY}' not found in result")
            print(f"   Available keys: {list(result_data.keys())}")
            extracted_data = None
        else:
            extracted_data = result_data[EXTRACT_KEY]
            print(f"✅ Successfully extracted '{EXTRACT_KEY}'")

            print(f"\n📊 Extracted data:")
            print(f"   Type: {type(extracted_data)}")
            if isinstance(extracted_data, dict):
                print(f"   Keys: {list(extracted_data.keys())}")
            elif isinstance(extracted_data, list):
                print(f"   Length: {len(extracted_data)}")

            print(f"\n📝 Extracted result:")
            print("-" * 80)
            print(json.dumps(extracted_data, indent=2))
            print("-" * 80)

    except Exception as e:
        print(f"❌ Error extracting key: {str(e)}")
        extracted_data = None
        raise
else:
    extracted_data = None
    if result_data:
        print("\n💡 No extract_key specified - showing full result above")

# COMMAND ----------

# DBTITLE 1,Example: How JobMetadataFetcherTool Formats the Response
print("\n" + "=" * 80)
print("How JobMetadataFetcherTool Formats the Response")
print("=" * 80)
print("This shows what you would get from JobMetadataFetcherTool with the same inputs.\n")

if result_data:
    if EXTRACT_KEY and extracted_data is not None:
        # Format like JobMetadataFetcherTool with extract_key
        tool_response = {
            "success": True,
            "run_id": RUN_ID,
            "task_key": TASK_KEY,
            "extract_key": EXTRACT_KEY,
            "data": extracted_data,
            "message": f"Successfully extracted '{EXTRACT_KEY}' from job output"
        }
    else:
        # Format like JobMetadataFetcherTool without extract_key
        tool_response = {
            "success": True,
            "run_id": RUN_ID,
            "task_key": TASK_KEY,
            "data": result_data,
            "available_keys": list(result_data.keys()) if isinstance(result_data, dict) else None,
            "message": "Successfully fetched complete job output"
        }

    print("📦 JobMetadataFetcherTool Response Format:")
    print("-" * 80)
    print(json.dumps(tool_response, indent=2))
    print("-" * 80)

else:
    # Error response format
    tool_response = {
        "error": "No output found from job run",
        "run_id": RUN_ID,
        "details": "The notebook may not have called dbutils.notebook.exit() or the job failed."
    }

    print("❌ JobMetadataFetcherTool Error Response Format:")
    print("-" * 80)
    print(json.dumps(tool_response, indent=2))
    print("-" * 80)

# COMMAND ----------

# DBTITLE 1,Additional Job Run Information
print("\n" + "=" * 80)
print("Additional Job Run Information")
print("=" * 80)
print("You can also get metadata about the job run itself:\n")

try:
    # Get full run information
    run_info = workspace_client.jobs.get_run(int(RUN_ID))

    print(f"📊 Job Run Details:")
    print(f"   Run ID: {run_info.run_id}")
    print(f"   Run Name: {run_info.run_name}")
    print(f"   State: {run_info.state.life_cycle_state}")
    print(f"   Result State: {run_info.state.result_state}")

    if run_info.start_time:
        start_time = datetime.fromtimestamp(run_info.start_time / 1000)
        print(f"   Start Time: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")

    if run_info.end_time:
        end_time = datetime.fromtimestamp(run_info.end_time / 1000)
        print(f"   End Time: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")

        if run_info.start_time:
            duration_seconds = (run_info.end_time - run_info.start_time) / 1000
            print(f"   Duration: {duration_seconds:.2f} seconds")

    if run_info.tasks and len(run_info.tasks) > 0:
        print(f"\n   Tasks ({len(run_info.tasks)}):")
        for task in run_info.tasks:
            print(f"      - {task.task_key}: {task.state.life_cycle_state}")

    print("\n✅ Job run information retrieved successfully")

except Exception as e:
    print(f"⚠️  Could not retrieve job run information: {str(e)}")

# COMMAND ----------

# DBTITLE 1,Example Use Cases
print("\n" + "=" * 80)
print("Example Use Cases")
print("=" * 80)

print("""
1. **Retrieve Full PowerBI Pipeline Output (single-task job)**:
   {
     "run_id": "295929103033812"
   }
   → Returns: Complete result with all pipeline steps

2. **Retrieve Output from Multi-Task Job**:
   {
     "run_id": "720546100105818",
     "task_key": "execute_notebook"
   }
   → Returns: Output from specific task in multi-task job

3. **Extract Only Metadata**:
   {
     "run_id": "720546100105818",
     "task_key": "execute_notebook",
     "extract_key": "metadata"
   }
   → Returns: Just the metadata field

4. **Extract Compact Metadata**:
   {
     "run_id": "295929103033812",
     "extract_key": "compact_metadata"
   }
   → Returns: Just the compact_metadata field

5. **Extract Execution Results**:
   {
     "run_id": "295929103033812",
     "extract_key": "pipeline_steps"
   }
   → Returns: All pipeline step outputs

6. **Auto-Detect Task in Multi-Task Job**:
   {
     "run_id": "720546100105818"
   }
   → Automatically uses first task if only one task, or first task in list
""")

print("\n💡 Pro Tips:")
print("   - Always wait for job to complete before fetching output")
print("   - Check result_state == 'SUCCESS' before fetching")
print("   - For multi-task jobs, specify task_key or it will auto-detect")
print("   - Use extract_key to get specific fields and reduce data transfer")
print("   - JobMetadataFetcherTool is registered as tool ID 72 in Kasal")
print("   - If you see 'multiple tasks' error, provide task_key parameter")

# COMMAND ----------

# DBTITLE 1,Execution Summary
print("\n" + "=" * 80)
print("Execution Summary")
print("=" * 80)
print(f"✅ Test Completed Successfully")
print(f"   Execution Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print(f"   Run ID: {RUN_ID}")
print(f"   Task Key: {TASK_KEY if TASK_KEY else '(none)'}")
print(f"   Extract Key: {EXTRACT_KEY if EXTRACT_KEY else '(none)'}")
print(f"   Output Found: {'Yes' if result_data else 'No'}")
print(f"   Extraction Successful: {'Yes' if extracted_data is not None else 'No' if EXTRACT_KEY else 'N/A'}")
print("=" * 80)

# COMMAND ----------

# DBTITLE 1,Return Results
# Build result summary
summary = {
    "status": "success",
    "execution_time": datetime.now().isoformat(),
    "run_id": RUN_ID,
    "task_key": TASK_KEY,
    "extract_key": EXTRACT_KEY,
    "output_found": result_data is not None,
    "full_result": result_data,
    "extracted_result": extracted_data if EXTRACT_KEY else None,
    "tool_response_example": tool_response
}

# Exit with results
dbutils.notebook.exit(json.dumps(summary))
