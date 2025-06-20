#!/bin/bash

# Kasal CLI - Interactive script to interact with Kasal backend
# This script allows you to list crews and execute them with input parameters

# Configuration
BACKEND_URL="${KASAL_BACKEND_URL:-http://localhost:8000}"
API_KEY="${KASAL_API_KEY:-}"
USER_EMAIL="${KASAL_USER_EMAIL:-alice@acme-corp.com}"
MODEL="${KASAL_MODEL:-databricks-llama-4-maverick}"
PLANNING="${KASAL_PLANNING:-false}"
REASONING="${KASAL_REASONING:-false}"
SCHEMA_DETECTION="${KASAL_SCHEMA_DETECTION:-true}"
SHOW_TRACE="${KASAL_SHOW_TRACE:-false}"

# Colors for better output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

# Function to print colored output
print_color() {
    local color=$1
    local message=$2
    echo -e "${color}${message}${NC}"
}

# Function to check if backend is accessible
check_backend() {
    print_color "$YELLOW" "Checking backend connectivity..."
    
    response=$(curl -s -o /dev/null -w "%{http_code}" "$BACKEND_URL/health" 2>/dev/null)
    
    if [ "$response" = "200" ]; then
        print_color "$GREEN" "✓ Backend is accessible at $BACKEND_URL"
        return 0
    else
        print_color "$RED" "✗ Backend is not accessible at $BACKEND_URL"
        print_color "$YELLOW" "Please ensure the backend is running or set KASAL_BACKEND_URL environment variable"
        return 1
    fi
}


# Function to list all crews
list_crews() {
    print_color "$BLUE" "\n=== Available Crews ===\n"
    
    # Get crews from backend
    response=$(curl -s -X GET "$BACKEND_URL/api/v1/crews" \
        -H "Accept: application/json, text/plain, */*" \
        -H "x-forwarded-email: $USER_EMAIL" \
        $([ -n "$API_KEY" ] && echo "-H \"Authorization: Bearer $API_KEY\"" || echo ""))
    
    # Check if response is valid JSON
    if ! echo "$response" | jq empty 2>/dev/null; then
        print_color "$RED" "Error: Invalid response from backend"
        echo "$response"
        return 1
    fi
    
    # Parse and display crews
    crew_count=$(echo "$response" | jq 'length')
    
    if [ "$crew_count" -eq 0 ]; then
        print_color "$YELLOW" "No crews found"
        return 1
    fi
    
    # Display crews with numbers
    for i in $(seq 0 $((crew_count - 1))); do
        crew=$(echo "$response" | jq -r ".[$i]")
        crew_id=$(echo "$crew" | jq -r '.id')
        crew_name=$(echo "$crew" | jq -r '.name // "Unnamed Crew"')
        crew_desc=$(echo "$crew" | jq -r '.description // "No description"')
        
        print_color "$GREEN" "[$((i + 1))] $crew_name"
        echo "    ID: $crew_id"
        echo "    Description: $crew_desc"
        echo ""
    done
    
    echo "$response" > /tmp/kasal_crews.json
    return 0
}


# Function to execute a crew
execute_crew() {
    local crew_number=$1
    
    # First list crews
    if ! list_crews; then
        return 1
    fi
    
    # Load crews from temp file
    crews=$(cat /tmp/kasal_crews.json)
    crew_count=$(echo "$crews" | jq 'length')
    
    # If crew number not provided, prompt for it
    if [ -z "$crew_number" ]; then
        echo ""
        read -p "Enter crew number to execute: " crew_number
    fi
    
    # Validate crew number
    if [ "$crew_number" -lt 1 ] || [ "$crew_number" -gt "$crew_count" ]; then
        print_color "$RED" "Error: Invalid crew number. Please select between 1 and $crew_count"
        return 1
    fi
    
    # Get selected crew
    crew_index=$((crew_number - 1))
    crew=$(echo "$crews" | jq ".[$crew_index]")
    crew_id=$(echo "$crew" | jq -r '.id')
    crew_name=$(echo "$crew" | jq -r '.name // "Unnamed Crew"')
    
    print_color "$BLUE" "\n=== Executing Crew: $crew_name ===\n"
    
    # Extract nodes and convert to agents_yaml and tasks_yaml format
    nodes=$(echo "$crew" | jq -c '.nodes // []')
    
    # Debug: Show node structure if DEBUG is set
    if [ -n "$DEBUG" ]; then
        print_color "$YELLOW" "\nDebug: Crew structure:"
        echo "$crew" | jq '{id, name, nodes: (.nodes | length), edges: (.edges | length)}'
        
        print_color "$YELLOW" "\nDebug: First few nodes:"
        echo "$nodes" | jq -c '.[:2]'
        
        print_color "$YELLOW" "\nDebug: Node types found:"
        echo "$nodes" | jq -r '.[].type' | sort | uniq -c
    fi
    
    # Build agents_yaml from agent nodes
    agents_yaml=$(echo "$nodes" | jq 'map(select(.type == "agentNode")) | reduce .[] as $agent ({}; .[$agent.id] = $agent.data)')
    
    # Build tasks_yaml from task nodes  
    tasks_yaml=$(echo "$nodes" | jq 'map(select(.type == "taskNode")) | reduce .[] as $task ({}; .[$task.id] = $task.data)')
    
    # Check if we have agents and tasks
    agent_count=$(echo "$agents_yaml" | jq 'length')
    task_count=$(echo "$tasks_yaml" | jq 'length')
    
    if [ "$agent_count" -eq 0 ]; then
        print_color "$RED" "Error: No agents found in this crew"
        
        # Debug: Show what we're looking for
        if [ -n "$DEBUG" ]; then
            print_color "$YELLOW" "\nDebug: Looking for nodes with type='agentNode'"
            print_color "$YELLOW" "Debug: agents_yaml content:"
            echo "$agents_yaml" | jq .
        fi
        
        return 1
    fi
    
    if [ "$task_count" -eq 0 ]; then
        print_color "$RED" "Error: No tasks found in this crew"
        
        # Debug: Show what we're looking for
        if [ -n "$DEBUG" ]; then
            print_color "$YELLOW" "\nDebug: Looking for nodes with type='taskNode'"
            print_color "$YELLOW" "Debug: tasks_yaml content:"
            echo "$tasks_yaml" | jq .
        fi
        
        return 1
    fi
    
    print_color "$GREEN" "Found $agent_count agents and $task_count tasks"
    
    # Extract all text fields that might contain variables from both agents and tasks
    all_text=$(echo "$nodes" | jq -r '
        .[] | 
        .data | 
        to_entries | 
        .[] | 
        select(.key | IN("role", "goal", "backstory", "description", "expected_output", "context")) | 
        .value | 
        select(type == "string")
    ' 2>/dev/null || echo "")
    
    # Find all variables in {variable} format
    variables=()
    while IFS= read -r line; do
        while [[ "$line" =~ \{([^}]+)\} ]]; do
            var="${BASH_REMATCH[1]}"
            # Add to array if not already present
            if [[ ! " ${variables[@]} " =~ " ${var} " ]]; then
                variables+=("$var")
            fi
            # Remove the found variable to continue searching
            line=${line/\{$var\}/}
        done
    done <<< "$all_text"
    
    # Collect input parameters if needed
    inputs="{}"
    if [ ${#variables[@]} -gt 0 ]; then
        print_color "$YELLOW" "\nThis crew requires the following input variables:"
        for var in "${variables[@]}"; do
            echo "  - {$var}"
        done
        echo ""
        
        # Collect values for each variable
        for var in "${variables[@]}"; do
            read -p "Enter value for {$var}: " value
            inputs=$(echo "$inputs" | jq --arg key "$var" --arg val "$value" '. + {($key): $val}')
        done
    else
        print_color "$GREEN" "No input variables required for this crew"
    fi
    
    # Prepare execution payload matching frontend format
    execution_payload=$(jq -n \
        --argjson agents_yaml "$agents_yaml" \
        --argjson tasks_yaml "$tasks_yaml" \
        --argjson inputs "$inputs" \
        --arg model "$MODEL" \
        --argjson planning "$PLANNING" \
        --argjson reasoning "$REASONING" \
        --argjson schema_detection "$SCHEMA_DETECTION" \
        '{
            "agents_yaml": $agents_yaml,
            "tasks_yaml": $tasks_yaml,
            "inputs": $inputs,
            "model": $model,
            "planning": $planning,
            "reasoning": $reasoning,
            "execution_type": "crew",
            "schema_detection_enabled": $schema_detection
        }')
    
    # Debug: Show payload if DEBUG environment variable is set
    if [ -n "$DEBUG" ]; then
        print_color "$YELLOW" "\nDebug: Execution payload:"
        echo "$execution_payload" | jq .
    fi
    
    print_color "$YELLOW" "\nStarting execution..."
    
    # Execute crew
    response=$(curl -s -X POST "$BACKEND_URL/api/v1/executions" \
        -H "Content-Type: application/json" \
        -H "Accept: application/json, text/plain, */*" \
        -H "x-forwarded-email: $USER_EMAIL" \
        $([ -n "$API_KEY" ] && echo "-H \"Authorization: Bearer $API_KEY\"" || echo "") \
        -d "$execution_payload")
    
    # Check response
    if ! echo "$response" | jq empty 2>/dev/null; then
        print_color "$RED" "Error: Invalid response from backend"
        echo "$response"
        return 1
    fi
    
    execution_id=$(echo "$response" | jq -r '.execution_id // .id')
    
    if [ -z "$execution_id" ] || [ "$execution_id" = "null" ]; then
        print_color "$RED" "Error: Failed to start execution"
        echo "$response" | jq .
        return 1
    fi
    
    print_color "$GREEN" "✓ Execution started with ID: $execution_id"
    
    # Monitor execution status
    monitor_execution "$execution_id"
}

# Function to fetch and display execution traces
fetch_traces() {
    local execution_id=$1
    local last_trace_id=${2:-0}
    
    # Fetch traces for this execution using job_id endpoint
    trace_response=$(curl -s -X GET "$BACKEND_URL/api/v1/traces/job/$execution_id" \
        -H "Accept: application/json, text/plain, */*" \
        -H "x-forwarded-email: $USER_EMAIL" \
        $([ -n "$API_KEY" ] && echo "-H \"Authorization: Bearer $API_KEY\"" || echo ""))
    
    if echo "$trace_response" | jq empty 2>/dev/null; then
        # Debug: Show trace structure if DEBUG is set
        if [ -n "$DEBUG" ] && [ "$last_trace_id" -eq 0 ]; then
            echo -e "\nDebug: Trace response structure:" >&2
            echo "$trace_response" | jq -c 'keys' >&2
            echo -e "\nDebug: First trace entry:" >&2
            echo "$trace_response" | jq '.traces[0] // "No traces"' >&2
        fi
        
        # Get traces array from the response
        traces=$(echo "$trace_response" | jq -c '.traces // []')
        trace_count=$(echo "$traces" | jq 'length')
        
        if [ "$trace_count" -gt "$last_trace_id" ]; then
            # Display new traces
            {
                echo ""  # New line before traces
                print_color "$BLUE" "--- Execution Trace ---"
                
                # Show only new traces since last check
                # Format trace entries based on actual schema
                echo "$traces" | jq -r --argjson start "$last_trace_id" '.[$start:] | .[] | 
                    "[\(.created_at // .timestamp // "N/A")] \(.event_type // "TRACE") | \(.event_source // "Unknown") | \(.event_context // "")\(if .output then " → " + (.output | tostring) else "" end)"' | while IFS= read -r trace_line; do
                    # Skip traces with no meaningful content
                    if [[ "$trace_line" == *"| |"* ]] || [[ -z "${trace_line// }" ]]; then
                        continue
                    fi
                    
                    # Color code based on event type or content
                    if echo "$trace_line" | grep -qi "error\|exception\|failed"; then
                        print_color "$RED" "$trace_line"
                    elif echo "$trace_line" | grep -qi "warn\|warning"; then
                        print_color "$YELLOW" "$trace_line"
                    elif echo "$trace_line" | grep -qi "agent.*start\|agent.*end\|agent"; then
                        print_color "$GREEN" "$trace_line"
                    elif echo "$trace_line" | grep -qi "task.*start\|task.*end\|task"; then
                        print_color "$CYAN" "$trace_line"
                    elif echo "$trace_line" | grep -qi "tool\|function.*call"; then
                        print_color "$YELLOW" "$trace_line"
                    elif echo "$trace_line" | grep -qi "crew.*start\|crew.*end"; then
                        print_color "$BLUE" "$trace_line"
                    else
                        echo "$trace_line"
                    fi
                done
            } >&2  # Send trace output to stderr
            
            echo "$trace_count"  # Return new count on stdout
        else
            echo "$last_trace_id"  # No new traces
        fi
    else
        echo "$last_trace_id"  # Failed to fetch
    fi
}

# Function to monitor execution status
monitor_execution() {
    local execution_id=$1
    local status="RUNNING"
    local last_trace_count=0
    
    print_color "$YELLOW" "\nMonitoring execution..."
    
    while [ "$status" = "RUNNING" ] || [ "$status" = "PENDING" ]; do
        # Fetch execution status
        response=$(curl -s -X GET "$BACKEND_URL/api/v1/executions/$execution_id" \
            -H "Accept: application/json, text/plain, */*" \
            -H "x-forwarded-email: $USER_EMAIL" \
            $([ -n "$API_KEY" ] && echo "-H \"Authorization: Bearer $API_KEY\"" || echo ""))
        
        if ! echo "$response" | jq empty 2>/dev/null; then
            print_color "$RED" "Error: Failed to get execution status"
            return 1
        fi
        
        status=$(echo "$response" | jq -r '.status')
        
        # Show traces if enabled
        if [ "$SHOW_TRACE" = "true" ]; then
            new_trace_count=$(fetch_traces "$execution_id" "$last_trace_count")
            
            # Debug output if enabled
            if [ -n "$DEBUG" ]; then
                echo -e "\nDebug: last_trace_count=$last_trace_count, new_trace_count=$new_trace_count" >&2
            fi
            
            # Update count only if we got a valid number
            if [[ "$new_trace_count" =~ ^[0-9]+$ ]]; then
                last_trace_count=$new_trace_count
            fi
            
            echo -ne "\rStatus: $status"
        else
            echo -ne "\rStatus: $status"
        fi
        
        # Wait before next check
        if [ "$status" = "RUNNING" ] || [ "$status" = "PENDING" ]; then
            sleep 2
        fi
    done
    
    echo ""
    
    if [ "$status" = "COMPLETED" ]; then
        print_color "$GREEN" "\n✓ Execution completed successfully!"
        
        # Extract result from the response
        # The result field might be a string, object, or nested structure
        raw_result=$(echo "$response" | jq -r '.result // empty')
        
        if [ -n "$raw_result" ] && [ "$raw_result" != "null" ]; then
            print_color "$BLUE" "\n=== Final Result ==="
            
            # Check if it's a JSON string that needs parsing
            if echo "$raw_result" | grep -q '^{' 2>/dev/null || echo "$raw_result" | grep -q '^\[' 2>/dev/null; then
                # It's already JSON, pretty print it
                echo "$raw_result" | jq . 2>/dev/null || echo "$raw_result"
            else
                # Try to parse as JSON string
                parsed_result=$(echo "$raw_result" | jq -r . 2>/dev/null)
                if [ $? -eq 0 ] && (echo "$parsed_result" | grep -q '^{' 2>/dev/null || echo "$parsed_result" | grep -q '^\[' 2>/dev/null); then
                    # Successfully parsed as JSON
                    echo "$parsed_result" | jq . 2>/dev/null || echo "$parsed_result"
                else
                    # Plain text result
                    echo "$raw_result"
                fi
            fi
            
            echo ""
            
            # Also show execution details
            print_color "$GREEN" "\nExecution Details:"
            echo "  Execution ID: $execution_id"
            echo "  Status: $status"
            
            # Show run name if available
            run_name=$(echo "$response" | jq -r '.run_name // empty')
            if [ -n "$run_name" ] && [ "$run_name" != "null" ]; then
                echo "  Run Name: $run_name"
            fi
            
            # Show timestamps if available
            created_at=$(echo "$response" | jq -r '.created_at // empty')
            if [ -n "$created_at" ] && [ "$created_at" != "null" ]; then
                echo "  Started: $created_at"
            fi
            
            updated_at=$(echo "$response" | jq -r '.updated_at // empty')
            if [ -n "$updated_at" ] && [ "$updated_at" != "null" ]; then
                echo "  Completed: $updated_at"
            fi
        else
            print_color "$YELLOW" "\nExecution completed but no result data available."
            print_color "$YELLOW" "The crew may not have produced any output."
        fi
    else
        print_color "$RED" "\n✗ Execution failed with status: $status"
        error=$(echo "$response" | jq -r '.error // "No error details available"')
        echo "Error: $error"
        
        # Show more details if available
        error_details=$(echo "$response" | jq -r '.error_details // ""')
        if [ -n "$error_details" ] && [ "$error_details" != "null" ]; then
            echo "Details: $error_details"
        fi
        
        # Show traceback if available
        traceback=$(echo "$response" | jq -r '.traceback // ""')
        if [ -n "$traceback" ] && [ "$traceback" != "null" ]; then
            echo "Traceback:"
            echo "$traceback"
        fi
    fi
}

# Function to show main menu
show_menu() {
    print_color "$BLUE" "\n=== Kasal CLI Menu ==="
    echo "1. Execute a crew"
    echo "2. Set backend URL (current: $BACKEND_URL)"
    echo "3. Exit"
    echo ""
}

# Function to show usage
show_usage() {
    echo "Usage: $0 [OPTIONS]"
    echo ""
    echo "Options:"
    echo "  -u, --user EMAIL        User email (default: alice@acme-corp.com)"
    echo "  -b, --backend URL       Backend URL (default: http://localhost:8000)"
    echo "  -k, --api-key KEY       API key for authentication"
    echo "  -m, --model MODEL       LLM model to use (default: databricks-llama-4-maverick)"
    echo "  -p, --planning          Enable planning mode"
    echo "  -r, --reasoning         Enable reasoning mode"
    echo "  --no-schema-detection   Disable schema detection"
    echo "  -t, --trace             Show execution trace during monitoring"
    echo "  -h, --help              Show this help message"
    echo ""
    echo "Environment variables:"
    echo "  KASAL_USER_EMAIL        User email"
    echo "  KASAL_BACKEND_URL       Backend URL"
    echo "  KASAL_API_KEY           API key"
    echo "  KASAL_MODEL             LLM model"
    echo "  KASAL_PLANNING          Enable planning (true/false)"
    echo "  KASAL_REASONING         Enable reasoning (true/false)"
    echo "  KASAL_SCHEMA_DETECTION  Enable schema detection (true/false)"
    echo "  KASAL_SHOW_TRACE        Show execution trace (true/false)"
    echo ""
    echo "Examples:"
    echo "  # Run with default settings"
    echo "  $0"
    echo ""
    echo "  # Use a different user and model"
    echo "  $0 -u admin@admin.com -m databricks-llama-4-maverick"
    echo ""
    echo "  # Enable planning and reasoning"
    echo "  $0 -p -r"
    echo ""
    echo "  # Run with execution trace monitoring"
    echo "  $0 -t"
    echo ""
    echo "  # Connect to a different backend with API key"
    echo "  $0 -b https://api.kasal.io -k your-api-key-here"
    echo ""
    echo "  # Debug mode to see request/response details"
    echo "  DEBUG=1 $0"
    echo ""
    echo "  # Using environment variables"
    echo "  KASAL_MODEL=databricks-llama-4-maverick KASAL_PLANNING=true $0"
}

# Parse command line arguments
parse_args() {
    while [[ $# -gt 0 ]]; do
        case $1 in
            -u|--user)
                USER_EMAIL="$2"
                shift 2
                ;;
            -b|--backend)
                BACKEND_URL="$2"
                shift 2
                ;;
            -k|--api-key)
                API_KEY="$2"
                shift 2
                ;;
            -m|--model)
                MODEL="$2"
                shift 2
                ;;
            -p|--planning)
                PLANNING="true"
                shift
                ;;
            -r|--reasoning)
                REASONING="true"
                shift
                ;;
            --no-schema-detection)
                SCHEMA_DETECTION="false"
                shift
                ;;
            -t|--trace)
                SHOW_TRACE="true"
                shift
                ;;
            -h|--help)
                show_usage
                exit 0
                ;;
            *)
                print_color "$RED" "Unknown option: $1"
                show_usage
                exit 1
                ;;
        esac
    done
}

# Main interactive loop
main() {
    # Parse command line arguments
    parse_args "$@"
    
    print_color "$BLUE" "Welcome to Kasal CLI!"
    print_color "$YELLOW" "User: $USER_EMAIL"
    print_color "$YELLOW" "Backend: $BACKEND_URL"
    print_color "$YELLOW" "Model: $MODEL"
    
    if [ "$PLANNING" = "true" ] || [ "$REASONING" = "true" ]; then
        print_color "$YELLOW" "Features: $([ "$PLANNING" = "true" ] && echo -n "Planning ") $([ "$REASONING" = "true" ] && echo -n "Reasoning")"
    fi
    
    echo ""
    
    # Check backend connectivity
    if ! check_backend; then
        exit 1
    fi
    
    # Check for required tools
    if ! command -v jq &> /dev/null; then
        print_color "$RED" "Error: jq is required but not installed"
        print_color "$YELLOW" "Please install jq: brew install jq (macOS) or apt-get install jq (Linux)"
        exit 1
    fi
    
    while true; do
        show_menu
        read -p "Select an option: " choice
        
        case $choice in
            1)
                execute_crew
                ;;
            2)
                read -p "Enter new backend URL: " new_url
                BACKEND_URL="$new_url"
                check_backend
                ;;
            3)
                print_color "$GREEN" "Goodbye!"
                exit 0
                ;;
            *)
                print_color "$RED" "Invalid option. Please try again."
                ;;
        esac
    done
}

# Run main function with all arguments
main "$@"