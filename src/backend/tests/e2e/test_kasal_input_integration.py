"""
Real integration test for CrewAI input workflow.

This test makes actual API calls to a running backend.
Requires the backend to be running at http://localhost:8000
"""

import json
import time
import sys
from typing import Dict, Any, List, Optional
import requests
from datetime import datetime


class TestCrewAIRealIntegration:
    """Real integration test for the CrewAI input workflow."""
    
    # Test configuration
    BASE_URL = "http://localhost:8000"
    TEST_USER = "alice@acme-corp.com"
    
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            "x-forwarded-email": self.TEST_USER,
            "Accept": "application/json, text/plain, */*",
            "Content-Type": "application/json"
        })
    
    def check_backend_health(self) -> bool:
        """Check if backend is accessible."""
        try:
            response = self.session.get(f"{self.BASE_URL}/health")
            return response.status_code == 200
        except requests.exceptions.ConnectionError:
            return False
    
    def list_crews(self) -> List[Dict[str, Any]]:
        """List all crews for the user."""
        response = self.session.get(f"{self.BASE_URL}/api/v1/crews")
        response.raise_for_status()
        return response.json()
    
    def get_crew(self, crew_id: str) -> Dict[str, Any]:
        """Get a specific crew by ID."""
        response = self.session.get(f"{self.BASE_URL}/api/v1/crews/{crew_id}")
        response.raise_for_status()
        return response.json()
    
    def create_agent(self, agent_data: Dict[str, Any]) -> Dict[str, Any]:
        """Create a new agent."""
        response = self.session.post(
            f"{self.BASE_URL}/api/v1/agents",
            json=agent_data
        )
        response.raise_for_status()
        return response.json()
    
    def create_task(self, task_data: Dict[str, Any]) -> Dict[str, Any]:
        """Create a new task."""
        response = self.session.post(
            f"{self.BASE_URL}/api/v1/tasks",
            json=task_data
        )
        response.raise_for_status()
        return response.json()
    
    def create_crew(self, crew_data: Dict[str, Any]) -> Dict[str, Any]:
        """Create a new crew."""
        response = self.session.post(
            f"{self.BASE_URL}/api/v1/crews",
            json=crew_data
        )
        response.raise_for_status()
        return response.json()
    
    def execute_crew(self, execution_payload: Dict[str, Any]) -> str:
        """Execute a crew and return execution ID."""
        response = self.session.post(
            f"{self.BASE_URL}/api/v1/executions",
            json=execution_payload
        )
        response.raise_for_status()
        result = response.json()
        return result.get("execution_id", result.get("id"))
    
    def get_execution_status(self, execution_id: str) -> Dict[str, Any]:
        """Get execution status."""
        response = self.session.get(
            f"{self.BASE_URL}/api/v1/executions/{execution_id}"
        )
        response.raise_for_status()
        return response.json()
    
    def get_execution_traces(self, execution_id: str) -> List[Dict[str, Any]]:
        """Get execution traces."""
        response = self.session.get(
            f"{self.BASE_URL}/api/v1/traces/job/{execution_id}"
        )
        if response.status_code == 200:
            data = response.json()
            return data.get("traces", [])
        return []
    
    def extract_variables_from_text(self, text: str) -> List[str]:
        """Extract variables in {variable} format from text."""
        import re
        pattern = r'\{([^}]+)\}'
        return list(set(re.findall(pattern, text)))
    
    def extract_variables_from_crew(self, crew_data: Dict[str, Any]) -> List[str]:
        """Extract all variables from crew configuration."""
        variables = set()
        
        for node in crew_data.get("nodes", []):
            node_data = node.get("data", {})
            text_fields = ["role", "goal", "backstory", "description", "expected_output"]
            
            for field in text_fields:
                if field in node_data and isinstance(node_data[field], str):
                    vars_in_field = self.extract_variables_from_text(node_data[field])
                    variables.update(vars_in_field)
        
        return sorted(list(variables))
    
    def build_execution_payload(
        self,
        crew_data: Dict[str, Any],
        inputs: Dict[str, str]
    ) -> Dict[str, Any]:
        """Build execution payload from crew data."""
        agents_yaml = {}
        tasks_yaml = {}
        
        for node in crew_data.get("nodes", []):
            if node["type"] == "agentNode":
                agents_yaml[node["id"]] = node["data"]
            elif node["type"] == "taskNode":
                tasks_yaml[node["id"]] = node["data"]
        
        return {
            "agents_yaml": agents_yaml,
            "tasks_yaml": tasks_yaml,
            "inputs": inputs,
            "model": "databricks-llama-4-maverick",
            "planning": False,
            "reasoning": False,
            "execution_type": "crew",
            "schema_detection_enabled": True
        }
    
    def monitor_execution(self, execution_id: str, timeout: int = 120) -> Dict[str, Any]:
        """Monitor execution until completion."""
        start_time = time.time()
        last_status = None
        last_trace_count = 0
        
        print(f"\nMonitoring execution: {execution_id}")
        
        while time.time() - start_time < timeout:
            # Get status
            try:
                execution = self.get_execution_status(execution_id)
                status = execution.get("status", "UNKNOWN")
                
                if status != last_status:
                    print(f"Status: {status}")
                    last_status = status
                
                # Get traces
                traces = self.get_execution_traces(execution_id)
                if len(traces) > last_trace_count:
                    print("\n--- New Traces ---")
                    for trace in traces[last_trace_count:]:
                        timestamp = trace.get("created_at", trace.get("timestamp", ""))
                        event_type = trace.get("event_type", "TRACE")
                        event_source = trace.get("event_source", "")
                        event_context = trace.get("event_context", "")
                        
                        trace_line = f"[{timestamp}] {event_type}"
                        if event_source:
                            trace_line += f" | {event_source}"
                        if event_context:
                            trace_line += f" | {event_context}"
                        
                        print(f"  {trace_line}")
                    
                    last_trace_count = len(traces)
                
                # Check if completed
                if status in ["COMPLETED", "FAILED", "ERROR"]:
                    return execution
                
                time.sleep(2)
                
            except Exception as e:
                print(f"Error monitoring execution: {e}")
                time.sleep(2)
        
        raise TimeoutError(f"Execution {execution_id} did not complete within {timeout} seconds")
    
    def test_list_and_execute_existing_crew(self):
        """Test listing crews and executing one with input variables."""
        print("\n=== Testing List and Execute Existing Crew ===")
        
        # List crews
        print("\n1. Listing available crews...")
        crews = self.list_crews()
        
        if not crews:
            print("   No crews found. Please create some crews first.")
            return
        
        print(f"\nFound {len(crews)} crews:")
        for i, crew in enumerate(crews, 1):
            print(f"[{i}] {crew['name']}")
            print(f"    ID: {crew['id']}")
        
        # Find a crew with input variables
        crew_with_inputs = None
        print("\n2. Checking crews for input variables...")
        
        for i, crew in enumerate(crews[:3], 1):  # Check first 3 crews for debugging
            # Get full crew details
            try:
                full_crew = self.get_crew(crew['id'])
                variables = self.extract_variables_from_crew(full_crew)
                
                if variables:
                    crew_with_inputs = full_crew
                    print(f"\n   ✓ Found crew with variables: {full_crew['name']}")
                    print(f"   Required variables: {variables}")
                    break
                else:
                    print(f"   - {crew['name']}: No variables found")
                    
            except Exception as e:
                print(f"   - {crew['name']}: Error loading crew - {e}")
        
        if not crew_with_inputs:
            print("\n   No crews with input variables found in the checked crews.")
            print("   Creating a new crew with variables instead...")
            return
        
        # Collect inputs
        print("\n3. Setting input values...")
        inputs = {}
        for var in variables:
            if var == "from":
                inputs[var] = "Zurich"
            elif var == "to":
                inputs[var] = "Montreal"
            elif var == "date":
                inputs[var] = "July 20th, 2025"
            elif var == "topic":
                inputs[var] = "AI and Data Summit"
            else:
                inputs[var] = f"test_{var}"
            
            print(f"   {var}: {inputs[var]}")
        
        # Build and execute
        print("\n4. Executing crew...")
        payload = self.build_execution_payload(crew_with_inputs, inputs)
        
        try:
            execution_id = self.execute_crew(payload)
            print(f"   ✓ Execution started: {execution_id}")
            
            # Monitor execution
            result = self.monitor_execution(execution_id)
            
            print(f"\n5. Execution completed with status: {result['status']}")
            
            if result.get("result"):
                print("\n=== Final Result ===")
                if isinstance(result["result"], dict):
                    print(json.dumps(result["result"], indent=2))
                else:
                    print(result["result"])
            
        except Exception as e:
            print(f"   ✗ Execution failed: {e}")
    
    def test_create_and_execute_new_crew(self):
        """Test creating a new crew with input variables and executing it."""
        print("\n=== Testing Create and Execute New Crew ===")
        
        print("\n1. Creating test agent with input variables...")
        
        # Create agent
        agent_data = {
            "name": f"Test Agent {datetime.now().strftime('%H%M%S')}",
            "role": "Search for information",
            "goal": "Find information about {topic} on {date}",
            "backstory": "Expert researcher with access to various sources",
            "tools": [],
            "llm": "databricks-llama-4-maverick",
            "max_iter": 25,
            "memory": True,
            "verbose": False,
            "allow_delegation": False,
            "cache": True
        }
        
        try:
            agent = self.create_agent(agent_data)
            print(f"   ✓ Created agent: {agent['id']}")
        except Exception as e:
            print(f"   ✗ Failed to create agent: {e}")
            return
        
        print("\n2. Creating test task with input variables...")
        
        # Create task
        task_data = {
            "name": f"Test Task {datetime.now().strftime('%H%M%S')}",
            "description": "Research {topic} and provide a summary for {date}",
            "expected_output": "A comprehensive summary of the topic",
            "tools": [],
            "memory": True,
            "cache_response": False,
            "retry_on_fail": True,
            "max_retries": 3
        }
        
        try:
            task = self.create_task(task_data)
            print(f"   ✓ Created task: {task['id']}")
        except Exception as e:
            print(f"   ✗ Failed to create task: {e}")
            return
        
        print("\n3. Creating crew...")
        
        # Build crew data
        nodes = [
            {
                "id": f"agent-{agent['id']}",
                "type": "agentNode",
                "position": {"x": 100, "y": 100},
                "data": {
                    **agent, 
                    "label": agent['name'],  # Add required label field
                    "agentId": agent['id']
                }
            },
            {
                "id": f"task-{task['id']}",
                "type": "taskNode",
                "position": {"x": 400, "y": 100},
                "data": {
                    **task,
                    "label": task['name'],  # Add required label field
                    "taskId": task['id']
                }
            }
        ]
        
        edges = [
            {
                "id": "edge-1",
                "source": f"agent-{agent['id']}",
                "target": f"task-{task['id']}",
                "type": "smoothstep"
            }
        ]
        
        crew_data = {
            "name": f"Test Crew {datetime.now().strftime('%H%M%S')}",
            "agent_ids": [agent['id']],
            "task_ids": [task['id']],
            "nodes": nodes,
            "edges": edges
        }
        
        try:
            crew = self.create_crew(crew_data)
            print(f"   ✓ Created crew: {crew['id']}")
        except requests.exceptions.HTTPError as e:
            print(f"   ✗ Failed to create crew: {e}")
            if hasattr(e.response, 'text'):
                print(f"   Error details: {e.response.text}")
            return
        except Exception as e:
            print(f"   ✗ Failed to create crew: {e}")
            return
        
        # Extract variables and execute
        variables = self.extract_variables_from_crew(crew)
        print(f"\n4. Required variables: {variables}")
        
        print("\n5. Setting input values...")
        inputs = {
            "topic": "CrewAI Integration Testing",
            "date": "December 2024"
        }
        for var, value in inputs.items():
            print(f"   {var}: {value}")
        
        print("\n6. Executing crew...")
        payload = self.build_execution_payload(crew, inputs)
        
        try:
            execution_id = self.execute_crew(payload)
            print(f"   ✓ Execution started: {execution_id}")
            
            # Monitor execution
            result = self.monitor_execution(execution_id)
            
            print(f"\n7. Execution completed with status: {result['status']}")
            
            if result.get("result"):
                print("\n=== Final Result ===")
                if isinstance(result["result"], dict):
                    print(json.dumps(result["result"], indent=2))
                else:
                    print(result["result"])
            
        except Exception as e:
            print(f"   ✗ Execution failed: {e}")


def main():
    """Run the real integration tests."""
    test = TestCrewAIRealIntegration()
    
    print("=" * 60)
    print("CREWAI INPUT WORKFLOW - REAL INTEGRATION TEST")
    print("=" * 60)
    
    # Check backend
    print("\nChecking backend connectivity...")
    if not test.check_backend_health():
        print("✗ Backend is not accessible at http://localhost:8000")
        print("Please ensure the backend is running: cd src/backend && ./run.sh")
        sys.exit(1)
    
    print("✓ Backend is accessible")
    
    try:
        # Test 1: List and execute existing crew
        test.test_list_and_execute_existing_crew()
        
        # Test 2: Create and execute new crew
        test.test_create_and_execute_new_crew()
        
    except Exception as e:
        print(f"\n✗ Test failed with error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    
    print("\n" + "=" * 60)
    print("ALL TESTS COMPLETED!")
    print("=" * 60)


if __name__ == "__main__":
    main()