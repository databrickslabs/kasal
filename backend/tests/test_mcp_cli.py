#!/usr/bin/env python3
"""
MCP Authentication Test using Databricks CLI

This script tests the MCP authentication using the Databricks CLI token.
It helps us understand how the MCP endpoint behaves with a fresh token.
"""

import sys
import os
import json
import subprocess
import requests
import asyncio
from typing import Optional, Tuple, Dict
from urllib.parse import urljoin

def get_cli_token() -> Tuple[Optional[str], Optional[str]]:
    """
    Get a fresh token from the Databricks CLI.
    
    Returns:
        Tuple[Optional[str], Optional[str]]: (token, error_message)
    """
    try:
        # Run the CLI command to get the token
        result = subprocess.run(
            ["databricks", "auth", "token", "-p", "mcp"],
            capture_output=True,
            text=True,
            check=True
        )
        
        # Parse the JSON output
        token_data = json.loads(result.stdout)
        access_token = token_data.get("access_token")
        
        if not access_token:
            return None, "No access token found in CLI response"
            
        return access_token, None
        
    except subprocess.CalledProcessError as e:
        return None, f"CLI command failed: {e.stderr}"
    except json.JSONDecodeError as e:
        return None, f"Failed to parse CLI output: {e}"
    except Exception as e:
        return None, f"Unexpected error: {str(e)}"

def parse_sse_line(line: str) -> Dict[str, str]:
    """
    Parse an SSE line into event and data.
    
    Args:
        line: The SSE line to parse
        
    Returns:
        Dict with event and data fields
    """
    parts = line.split(":", 1)
    if len(parts) != 2:
        return {}
        
    field = parts[0].strip()
    value = parts[1].strip()
    
    return {field: value}

def test_mcp_endpoint(token: str, base_url: str) -> None:
    """
    Test the MCP endpoint with the given token.
    
    Args:
        token: The access token to use
        base_url: The base MCP server URL
    """
    print("\n" + "="*60)
    print("🔐 TESTING MCP ENDPOINT WITH CLI TOKEN")
    print("="*60)
    
    # Prepare headers
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "Accept": "text/event-stream",
        "Cache-Control": "no-cache",
        "Connection": "keep-alive"
    }
    
    print("\n📋 Headers:")
    for key, value in headers.items():
        if key == "Authorization":
            print(f"   {key}: Bearer {value[7:27]}...")
        else:
            print(f"   {key}: {value}")
    
    # Test 1: Initial SSE Connection
    print("\n🔄 Test 1: Initial SSE Connection")
    messages_endpoint = None
    session_id = None
    
    try:
        # Connect to the SSE endpoint
        response = requests.get(base_url, headers=headers, stream=True, timeout=30)
        print(f"   Status: {response.status_code}")
        print(f"   Content-Type: {response.headers.get('Content-Type', 'Not set')}")
        print(f"   All Headers: {dict(response.headers)}")
        
        # Read the initial SSE messages
        print("   Reading initial SSE messages...")
        try:
            for line in response.iter_lines():
                if not line:
                    continue
                    
                line_str = line.decode('utf-8')
                print(f"   Line: {line_str}")
                
                # Skip ping messages
                if line_str.startswith(": ping"):
                    continue
                
                # Parse the SSE line
                event_data = parse_sse_line(line_str)
                
                if "event" in event_data and event_data["event"] == "endpoint":
                    # Extract the messages endpoint
                    if "data" in event_data:
                        messages_endpoint = event_data["data"]
                        # Extract session_id from the endpoint
                        if "session_id=" in messages_endpoint:
                            session_id = messages_endpoint.split("session_id=")[1].split("&")[0]
                            print(f"   Found session_id: {session_id}")
                
                # Stop after we get the endpoint
                if messages_endpoint:
                    break
        except requests.exceptions.ChunkedEncodingError:
            # This is expected when we close the connection
            print("   Connection closed (expected)")
        finally:
            # Always close the initial connection
            response.close()
                
    except Exception as e:
        print(f"   ❌ Error: {e}")
        print(f"   Error Type: {type(e)}")
        import traceback
        print(f"   Traceback: {traceback.format_exc()}")
        return
        
    if not messages_endpoint:
        print("   ❌ Failed to get messages endpoint")
        return
        
    # Test 2: Initialize Call to Messages Endpoint
    print("\n🔄 Test 2: Initialize Call")
    try:
        # Construct the full messages URL
        messages_url = urljoin(base_url, messages_endpoint)
        print(f"   Messages URL: {messages_url}")
        
        init_data = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "initialize",
            "params": {
                "protocolVersion": "2024-11-05",
                "capabilities": {},
                "clientInfo": {
                    "name": "test-client",
                    "version": "1.0.0"
                }
            }
        }
        
        # Send initialize request
        init_response = requests.post(
            messages_url,
            headers=headers,
            json=init_data,
            timeout=30
        )
        
        print(f"   Status: {init_response.status_code}")
        print(f"   Content-Type: {init_response.headers.get('Content-Type', 'Not set')}")
        print(f"   All Headers: {dict(init_response.headers)}")
        print(f"   Response: {init_response.text[:200]}...")
        
    except Exception as e:
        print(f"   ❌ Error: {e}")
        print(f"   Error Type: {type(e)}")
        import traceback
        print(f"   Traceback: {traceback.format_exc()}")
    
    # Test 3: Listen for Messages
    print("\n🔄 Test 3: Listen for Messages")
    try:
        # Connect to the messages endpoint
        msg_response = requests.get(messages_url, headers=headers, stream=True, timeout=30)
        print(f"   Status: {msg_response.status_code}")
        print(f"   Content-Type: {msg_response.headers.get('Content-Type', 'Not set')}")
        print(f"   All Headers: {dict(msg_response.headers)}")
        
        print("   Listening for messages...")
        message_count = 0
        try:
            for line in msg_response.iter_lines():
                if not line:
                    continue
                    
                line_str = line.decode('utf-8')
                
                # Skip ping messages
                if line_str.startswith(": ping"):
                    print(f"   Ping: {line_str}")
                    continue
                    
                print(f"   Message {message_count + 1}: {line_str[:200]}")
                message_count += 1
                
                # Stop after 3 non-ping messages
                if message_count >= 3:
                    break
        except requests.exceptions.ChunkedEncodingError:
            # This is expected when we close the connection
            print("   Connection closed (expected)")
        finally:
            # Always close the connection
            msg_response.close()
            
    except Exception as e:
        print(f"   ❌ Error: {e}")
        print(f"   Error Type: {type(e)}")
        import traceback
        print(f"   Traceback: {traceback.format_exc()}")

def main():
    """Main test function."""
    
    print("🚀 MCP Authentication Test with CLI Token")
    
    # MCP server URL
    mcp_url = "https://mcpgenie-1444828305810485.aws.databricksapps.com/sse"
    
    # Get token from CLI
    print("\n📋 Getting token from Databricks CLI...")
    token, error = get_cli_token()
    
    if error:
        print(f"❌ Failed to get token: {error}")
        return
        
    print("✅ Successfully got token from CLI")
    
    # Test the endpoint
    test_mcp_endpoint(token, mcp_url)
    
    print("\n💡 Notes:")
    print("   1. This test uses a fresh token from the Databricks CLI")
    print("   2. The token is obtained using 'databricks auth token -p mcp'")
    print("   3. This helps us understand how the MCP endpoint behaves")
    print("   4. Based on the results, we can improve our auth implementation")

if __name__ == "__main__":
    main() 