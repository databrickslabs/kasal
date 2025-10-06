DAX Analysis System - Simplified Implementation Guide
This guide helps you create a simplified DAX analysis system based on the lead discovery repository structure, focusing on dashboard analysis with crew orchestration.

Overview
Your simplified system will have: - Manager Agent: Orchestrates Databricks job execution - DAX Developer Agent: Generates optimized DAX statements based on dashboard requirements - Single Workflow: Dashboard ID + Questions → DAX Query → Databricks Execution → Results

Core Components You'll Need (Minimal Setup)
1. Crew Configuration (crew_dax_analysis.json)
{
  "id": "dax-analysis-crew-id",
  "name": "dax_analysis",
  "agent_ids": ["job-manager-id", "dax-developer-id"],
  "task_ids": ["dax-analysis-task-id"],
  "nodes": [
    {
      "id": "agent-job-manager",
      "type": "agentNode",
      "data": {
        "label": "Job Manager",
        "role": "Databricks Job Orchestrator",
        "goal": "Execute DAX analysis jobs with dashboard parameters",
        "backstory": "Experienced in managing Databricks workflows and DAX query execution",
        "tools": ["databricks_job_tool"],
        "llm": "databricks-llama-4-maverick"
      }
    },
    {
      "id": "agent-dax-developer",
      "type": "agentNode",
      "data": {
        "label": "DAX Developer",
        "role": "DAX Query Specialist",
        "goal": "Generate optimized DAX statements based on dashboard requirements",
        "backstory": "Expert in DAX query optimization and Power BI analytics",
        "tools": ["dax_generator_tool"],
        "llm": "databricks-llama-4-maverick"
      }
    },
    {
      "id": "task-dax-analysis",
      "type": "taskNode",
      "data": {
        "label": "Generate and Execute DAX Query",
        "description": "Analyze dashboard ID {dashboard_id} and questions {questions} to create DAX statement, then execute via Databricks job",
        "expected_output": "DAX query results with analysis insights"
      }
    }
  ]
}
2. Backend Components You Need
A. Minimal API Structure:
backend/
├── src/
│   └── dax_analysis/
│       ├── api/
│       │   └── v1/
│       │       └── routers/
│       │           └── dax_analysis.py    # Main API endpoint
│       ├── services/
│       │   ├── dax_crew_service.py        # Kasal crew integration
│       │   └── databricks_job_service.py  # Job execution
│       ├── schemas/
│       │   └── dax_analysis.py            # Request/response models
│       └── settings.py                    # Environment configuration
B. Environment Variables (.env):
# Databricks Configuration
DATABRICKS_HOST=https://your-workspace.databricks.com
DATABRICKS_TOKEN=your-personal-access-token
DATABRICKS_DAX_JOB_ID=your-job-id

# Kasal Integration
KASAL_BASE_URL=http://localhost:8000
KASAL_API_KEY=your-kasal-api-key
KASAL_CREW_NAME=dax_analysis
USE_KASAL_CREW=true
3. Scripts/Notebook You Need
A. Databricks Notebook (scripts/dax_analysis_job.py):
# Databricks notebook source
# MAGIC %md
# MAGIC # DAX Analysis Job
# MAGIC This notebook expects DAX statement as input and executes analysis

# COMMAND ----------

import json
import sys
from datetime import datetime

# Get job parameters
try:
    job_params = json.loads(dbutils.widgets.get("job_params"))
    dax_statement = job_params.get("dax_statement")
    dashboard_id = job_params.get("dashboard_id")
    questions = job_params.get("questions", [])
except:
    print("Error: No job parameters provided")
    sys.exit(1)

print(f"Dashboard ID: {dashboard_id}")
print(f"Questions: {questions}")
print(f"DAX Statement: {dax_statement}")

# COMMAND ----------

# Execute your DAX analysis function
# This could be a preinstalled wheel function or direct implementation
def evaluate_dax(dax_statement, context=None):
    """
    Your DAX evaluation function here
    This could call a preinstalled wheel or implement direct analysis
    """
    # Example implementation:
    # result = your_dax_wheel.evaluate(dax_statement)
    # return result

    # Placeholder - replace with your actual DAX evaluation logic
    return {
        "status": "success",
        "results": f"Analysis results for: {dax_statement}",
        "timestamp": datetime.now().isoformat()
    }

# Execute analysis
try:
    results = evaluate_dax(dax_statement, {
        "dashboard_id": dashboard_id,
        "questions": questions
    })

    print("Analysis Results:")
    print(json.dumps(results, indent=2))

except Exception as e:
    print(f"DAX analysis failed: {str(e)}")
    raise
4. API Implementation
A. Main API Router (api/v1/routers/dax_analysis.py):
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import List, Optional
import uuid
from datetime import datetime

router = APIRouter(prefix="/dax-analysis", tags=["dax-analysis"])

class DaxAnalysisRequest(BaseModel):
    dashboard_id: str
    questions: List[str]

class DaxAnalysisResponse(BaseModel):
    analysis_id: str
    status: str
    results: Optional[dict] = None

@router.post("/analyze", response_model=DaxAnalysisResponse)
async def analyze_dashboard(request: DaxAnalysisRequest):
    """
    Analyze dashboard with DAX queries based on questions
    """
    try:
        analysis_id = str(uuid.uuid4())

        # Prepare crew parameters
        crew_params = {
            "dashboard_id": request.dashboard_id,
            "questions": request.questions,
            "analysis_id": analysis_id,
            "timestamp": datetime.now().isoformat()
        }

        # Execute Kasal crew (similar to current lead discovery implementation)
        from ...services.dax_crew_service import DaxCrewService
        crew_service = DaxCrewService()

        result = await crew_service.execute_dax_analysis(crew_params)

        return DaxAnalysisResponse(
            analysis_id=analysis_id,
            status="completed",
            results=result
        )

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
B. Crew Service (services/dax_crew_service.py):
from ..integrations.kasal_client import KasalClient
from ..settings import get_settings

class DaxCrewService:
    def __init__(self):
        self.settings = get_settings()
        self.kasal_client = KasalClient()

    async def execute_dax_analysis(self, crew_params: dict):
        """Execute DAX analysis crew"""

        # Transform parameters for Kasal crew
        job_params = {
            "dashboard_id": crew_params["dashboard_id"],
            "questions": crew_params["questions"],
            "analysis_id": crew_params["analysis_id"]
        }

        # Execute crew (similar to lead discovery crew execution)
        crew_result = await self.kasal_client.execute_crew(
            crew_name=self.settings.KASAL_CREW_NAME,
            job_params=job_params
        )

        return crew_result
What You DON'T Need (Simplified Setup)
❌ Skip these complex components: - Frontend React application - Database models and repositories - User authentication - Export functionality - Location services - Lead processing logic - Configuration management UI - WebSocket support - Job monitoring dashboard

Deployment Steps
Set up Kasal crew with your 2 agents (Job Manager + DAX Developer)
Create Databricks job that runs your DAX analysis notebook
Configure environment variables for Databricks and Kasal
Deploy minimal FastAPI backend with single endpoint
Test workflow: API → Kasal Crew → Databricks Job → Results
Key Differences from Current Repo
Much simpler: Only 1 API endpoint vs 15+ endpoints
No database: Direct crew execution without persistence
No frontend: API-only interface
Single workflow: Dashboard ID + Questions → DAX → Results
Minimal dependencies: Just FastAPI, Kasal client, Databricks SDK
Required Dependencies
fastapi==0.115.0
uvicorn==0.34.0
pydantic==2.11.0
httpx==0.28.0
databricks-sdk==0.57.0
Usage Example
# Start the API server
uvicorn dax_analysis.api.main:app --reload

# Make a request
curl -X POST "http://localhost:8000/api/v1/dax-analysis/analyze" \
     -H "Content-Type: application/json" \
     -d '{
       "dashboard_id": "dashboard_123",
       "questions": [
         "What are the top 5 performing products?",
         "Show sales trends for the last quarter"
       ]
     }'
This gives you a production-ready but simplified version focusing only on your DAX analysis use case, leveraging the proven patterns from the lead discovery system.

Flow of Logic - LeadDiscovery System (LeDom)
This section shows the exact script-by-script execution flow when a user requests "leads for Zurich restaurants" - demonstrating how the system orchestrates the entire pipeline from frontend to results to get an idea what within IDOR has to be done.

High level flow: 1. Schema Creation: discovery_data = DiscoveryCreate(...) 2. Function Parameter: create_discovery(discovery_data, ...) - schema object passed as parameter to the router(s) 3. Data Extraction: search_criteria = discovery_data.search_criteria - clean data extracted 4. Service Call: Clean data passed to execute_crew_discovery() 5. Final Result: Databricks gets "restaurants" and ["Zurich"] instead of messy input

Flow 0: Schema Validation (Critical First Step)
📁 Frontend Request: {"businessType": "Restaurants", "locations": ["Zurich", "zurich", "ZURICH"]}
   ↓
📄 backend/src/lead_discovery/schemas/discovery.py:40-54
   class DiscoveryCreate(BaseModel):
       search_criteria: Dict[str, Any]
       engine_type: str = "kasal"

   Lines 17-22: Business type normalization
   business_type = "restaurants"  # normalized to lowercase

   Lines 24-37: Location deduplication
   locations = ["Zurich"]  # duplicates removed, case preserved

   Lines 46-53: Validation
   if not business_type: raise ValueError("business_type is required")
   if not locations: raise ValueError("locations must be a non-empty list")

   ↓ Creates validated DiscoveryCreate object with clean data
Flow 1: Frontend → API Router
📁 Frontend Request
   ↓
📄 backend/src/lead_discovery/api/v1/routers/discovery.py:317-347
   @discover_router.post("")
   async def start_discovery(payload: Dict[str, Any])
   ↓ calls
   create_discovery(discovery_data, request, mock_user, discovery_service)
Flow 2: API Router → Discovery Service
📄 discovery.py:55-83
   await discovery_service.execute_crew_discovery(
       discovery_id=discovery.id,
       business_type=business_type,
       locations=locations,
       crew_name="gmap"
   )
   ↓ calls
📄 backend/src/lead_discovery/services/lead_discovery_service.py
   async def execute_crew_discovery()
Flow 3: Discovery Service → Kasal Client
📄 lead_discovery_service.py (execute_crew_discovery method)
   kasal_client = KasalClient()
   await kasal_client.initialize(config)
   ↓ calls
📄 backend/src/lead_discovery/integrations/kasal_client.py:38-120
   async def initialize(self, config: Dict[str, Any])

   Then calls:
📄 kasal_client.py:1094-1154
   async def execute_crew_by_id(crew_id, inputs)
Flow 4: Kasal Client → Crew Details
📄 kasal_client.py:1111
   crew = await self.get_crew_details(crew_id)
   ↓ calls
📄 kasal_client.py:729-782
   async def get_crew_details(self, crew_id: str)

   Reads from:
📁 /crew_lead_discovery.json (lines 1-147)
   {
     "id": "42cb8de8-d77c-4d5a-beca-51ca79a5c88e",
     "name": "lead_discovery",
     "nodes": [...]
   }
Flow 5: Crew Parsing → Execution
📄 kasal_client.py:1116
   agents_yaml, tasks_yaml = await self.parse_crew_nodes(crew)
   ↓ calls
📄 kasal_client.py:1050-1071
   async def parse_crew_nodes(self, crew: Dict[str, Any])

   Then calls:
📄 kasal_client.py:1133
   execution_data = await self.execute_crew(agents_yaml, tasks_yaml, inputs)
   ↓ calls
📄 kasal_client.py:896-951
   async def execute_crew()
Flow 6: Kasal HTTP Request → Kasal Server
📄 kasal_client.py:942
   response = await self.client.post("/api/v1/executions", json=payload)
   ↓ HTTP POST to
🌐 Kasal Server /api/v1/executions

   Payload contains:
   - agents_yaml: Databricks Job Orchestrator config
   - tasks_yaml: "Execute job ID 220164985136197 with {job_params}"
   - inputs: {"business_type": "restaurants", "city": "Zurich", "api_key": "user-key"}
Flow 7: Kasal Agent → Databricks Job Submission
🤖 Kasal Agent (Databricks Job Orchestrator)
   Processes task: "Execute job ID 220164985136197 with {job_params}"
   ↓ creates
   job_params = {
       "search_id": "uuid",
       "search_params": {
           "query": "restaurants",
           "city": "Zurich",
           "api_key": "user-key"
       }
   }
   ↓ calls Databricks API
   databricks.jobs.run_now(job_id=220164985136197, notebook_params={...})
Flow 8: Databricks Job → Notebook Execution
🔷 Databricks Job Runner
   ↓ executes
📄 scripts/databricks_notebooks/gmaps_p_job.py:1-766

   Line 683-694: Get parameters
   job_params_json = dbutils.widgets.get("job_params")
   job_params = json.loads(job_params_json)
Flow 9: Parameter Extraction
📄 gmaps_p_job.py:718-726
   search_id = job_params["search_id"]
   search_timestamp = job_params["search_timestamp"]
   search_params = job_params.get("search_params", {})

   Extracts:
   - query = "restaurants"
   - city = "Zurich"
   - api_key = "user-key"
   - max_results = 100
Flow 10: Location Lookup
📄 gmaps_p_job.py:546
   locations = get_locations_for_city(city, country_code, max_search_points)
   ↓ calls
📄 gmaps_p_job.py:114-176
   def get_locations_for_city(city: str, country_code: str)

   Line 125-159: SQL Query
   locations_query = f"""
   SELECT postal_code, latitude, longitude, city
   FROM {LOCATION_TABLE}
   WHERE LOWER(city) = LOWER('Zurich')
   """

   Line 163: Execute via Spark
   result_df = spark.sql(locations_query)
Flow 11: API Session Setup
📄 gmaps_p_job.py:554
   session = create_session()
   ↓ calls
📄 gmaps_p_job.py:184-198
   def create_session() -> requests.Session:

   Creates HTTP session with:
   - Connection pooling
   - Retry logic
   - Rate limiting configuration
Flow 12: Location Processing Loop
📄 gmaps_p_job.py:566-583
   for loc_idx, location in enumerate(locations):
       postal_code = location['postal_code']    # "8001"
       latitude = location['latitude']          # 47.3769
       longitude = location['longitude']        # 8.5417

       for page in range(1, max_pages_per_location + 1):
           response = fetch_gmaps_page(...)
Flow 13: Google Maps API Call
📄 gmaps_p_job.py:586-589
   response = fetch_gmaps_page(session, api_key, query, latitude, longitude, zoom, page, language, country_code, include_reviews, max_results_per_page)
   ↓ calls
📄 gmaps_p_job.py:200-235
   def fetch_gmaps_page(session, api_key, query, latitude, longitude, zoom, page, language, country, include_reviews, max_results_per_page)

   Line 206-207: API URL
   url = "https://cloud.gmapsextractor.com/api/v2/search"
   location_str = f"@{latitude},{longitude},{zoom}z"  # "@47.3769,8.5417,16z"

   Line 211-212: Headers with user's API key
   headers = {"Authorization": f"Bearer {api_key}"}

   Line 214-222: Request payload
   data = {
       "q": "restaurants",
       "ll": "@47.3769,8.5417,16z",
       "limit": 20
   }

   Line 225: HTTP POST
   response = session.post(url, headers=headers, json=data, timeout=30)
Flow 14: Data Processing
📄 gmaps_p_job.py:601-620
   for place in places:
       result = process_place(place, search_id, search_timestamp, query_params, page-1, postal_code, latitude, longitude, loc_city, country_code)
   ↓ calls
📄 gmaps_p_job.py:243-380
   def process_place(place: Dict, search_id: str, ...)

   Key processing lines:
   Line 255-257: Business type detection
   is_restaurant = check_business_type(categories, 'restaurant|diner|eatery')

   Line 264-266: Performance metrics
   performance_tier = calculate_performance_tier(rating, review_count)

   Line 290-310: CCH relevance scoring
   if is_restaurant or is_bar:
       cch_relevance_score = 9
       cch_channel_type = 'HoReCa'

   Line 321-379: Return structured data
   return {
       'search_id': search_id,
       'place_name': place.get('name'),
       'categories': categories,
       'cch_relevance_score': cch_relevance_score,
       ...
   }
Flow 15: Database Storage
📄 gmaps_p_job.py:648-650
   if len(all_results) >= BATCH_INSERT_SIZE:
       insert_results_batch(all_results)
   ↓ calls
📄 gmaps_p_job.py:388-511
   def insert_results_batch(results: List[Dict]):

   Line 394: Convert to DataFrame
   df = pd.DataFrame(results)

   Line 416: Reorder columns
   df = df[column_order]

   Line 503: Create Spark DataFrame
   spark_df = spark.createDataFrame(df, schema=schema)

   Line 506-508: Write to Delta table
   spark_df.write.mode("append").saveAsTable(RESULTS_TABLE)
   # Writes to: users.nehme_tohme.gmaps_search_table
Flow 16: Job Completion & Results Flow Back
📄 gmaps_p_job.py:749-754
   print("Job completed successfully!")
   (Job execution ends)
   ↓
🔷 Databricks Job Status: COMPLETED
   ↓
🤖 Kasal Agent detects completion
   ↓
📄 kasal_client.py:1147-1152
   result = await self.monitor_execution_with_traces(execution_id)
   ↓
📄 lead_discovery_service.py
   Updates discovery status in database
   ↓
📁 Frontend polls /discover/{id}/results
   ↓
📄 discovery.py:365-384
   async def get_results()
   ↓
🖥️ User sees Zurich restaurant leads
Key Script Relationships:
Configuration Files: - crew_lead_discovery.json → Defines crew structure - backend/src/lead_discovery/settings.py → Environment variables

Core Processing Scripts: - discovery.py → API endpoints - kasal_client.py → Crew orchestration - gmaps_p_job.py → Data extraction & processing

Data Flow: - User Input → API → Kasal → Databricks → GMaps API → Delta Table → Results API → Frontend

Each script has specific responsibilities and clear handoff points, creating a robust pipeline for lead discovery. The user's simple request "leads for Zurich restaurants" triggers a complex multi-step orchestration involving crew agents, job execution, location lookup, API calls, data processing, and storage - all while maintaining the user's API key throughout the pipeline.