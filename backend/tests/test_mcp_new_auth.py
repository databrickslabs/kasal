#!/usr/bin/env python3
"""
Test the new MCP authentication that follows the CLI approach
"""

import sys
import os
import json
import requests
import asyncio
from typing import Optional, Tuple

# Add the backend src to Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from src.utils.databricks_auth import get_mcp_auth_headers

async def test_new_mcp_auth():
    """Test the new MCP authentication approach."""
    
    print("\n" + "="*60)
    print("🔐 TESTING NEW MCP AUTHENTICATION")
    print("="*60)
    
    # MCP server URL
    mcp_url = "https://mcpgenie-1444828305810485.aws.databricksapps.com/sse"
    
    print(f"\n📋 Configuration:")
    print(f"   MCP Server: {mcp_url}")
    
    # Test 1: Get authentication headers
    print(f"\n🔄 Test 1: Get MCP Authentication Headers")
    try:
        headers, error = await get_mcp_auth_headers(mcp_url)
        
        if headers:
            print(f"   ✅ Headers obtained:")
            for key, value in headers.items():
                if key == "Authorization":
                    print(f"      {key}: Bearer {value[7:27]}...")
                else:
                    print(f"      {key}: {value}")
        else:
            print(f"   ❌ Failed: {error}")
            return
    except Exception as e:
        print(f"   ❌ Error: {e}")
        return
    
    # Test 2: Test SSE connection
    print(f"\n🔄 Test 2: Test SSE Connection")
    try:
        response = requests.get(mcp_url, headers=headers, stream=True, timeout=10)
        print(f"   Status: {response.status_code}")
        print(f"   Content-Type: {response.headers.get('Content-Type', 'Not set')}")
        
        if response.status_code == 200:
            print(f"   ✅ SSE connection successful")
            if "text/event-stream" in response.headers.get('Content-Type', ''):
                print(f"   ✅ Correct content type for SSE")
                
                # Read a few lines to verify it's working
                line_count = 0
                for line in response.iter_lines():
                    if line:
                        line_str = line.decode('utf-8')
                        print(f"   SSE Line: {line_str[:100]}...")
                        line_count += 1
                        if line_count >= 3:  # Read first 3 lines
                            break
                            
            else:
                print(f"   ⚠️  Unexpected content type")
                print(f"   Response: {response.text[:200]}")
        else:
            print(f"   ❌ SSE connection failed")
            print(f"   Response: {response.text[:200]}")
            
        response.close()
        
    except Exception as e:
        print(f"   ❌ SSE connection error: {e}")
    
    print(f"\n" + "="*60)

def main():
    """Main test function."""
    
    print("🚀 New MCP Authentication Test")
    print("This test uses the Databricks CLI to get JWT tokens for MCP")
    
    # Run the async test
    asyncio.run(test_new_mcp_auth())
    
    print(f"\n💡 Key Changes:")
    print(f"   1. Use Databricks CLI directly: 'databricks auth token -p mcp'")
    print(f"   2. Get JWT tokens (not PAT tokens) for MCP compatibility")
    print(f"   3. Same headers as CLI test")
    print(f"   4. Fully working SSE connection")

if __name__ == "__main__":
    main() 