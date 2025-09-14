#!/usr/bin/env python3
"""
Knowledge Graph Enforcement Hook for Kasal Project
Ensures knowledge graph consultation and updates per CLAUDE.md requirements
"""

import json
import sys
from pathlib import Path
from datetime import datetime

def log_enforcement_action(action, details):
    """Log enforcement actions for debugging and compliance"""
    log_entry = {
        "timestamp": datetime.now().isoformat(),
        "action": action,
        "details": details
    }

    log_file = Path(".claude/logs/knowledge-graph-enforcement.log")
    log_file.parent.mkdir(exist_ok=True)

    with open(log_file, "a") as f:
        f.write(json.dumps(log_entry) + "\n")

def check_knowledge_graph_consultation(context):
    """
    Check if knowledge graph has been consulted in recent tool usage
    """
    recent_tools = context.get("recentTools", [])

    # Check for knowledge graph tools in recent usage
    kg_tools = [
        "mcp__knowledge-graph__aim_search_nodes",
        "mcp__knowledge-graph__aim_read_graph",
        "mcp__knowledge-graph__aim_open_nodes"
    ]

    has_consulted = any(tool in recent_tools for tool in kg_tools)

    log_enforcement_action("consultation_check", {
        "recent_tools": recent_tools,
        "has_consulted": has_consulted
    })

    return has_consulted

def enforce_knowledge_graph_workflow(tool_name, args, context):
    """
    Enforce knowledge graph workflow requirements
    """
    # Skip enforcement for knowledge graph tools themselves
    if tool_name.startswith("mcp__knowledge-graph__"):
        return {"allow": True, "message": "Knowledge graph tool - allowing"}

    # File operation tools that require prior knowledge graph consultation
    file_operation_tools = [
        "Read", "Edit", "MultiEdit", "Write", "Bash", "Glob", "Grep"
    ]

    if tool_name in file_operation_tools:
        if not check_knowledge_graph_consultation(context):
            return {
                "allow": False,
                "message": "🚫 BLOCKED: Must consult knowledge graph first!\n" +
                          "Required: Use aim_search_nodes() or aim_read_graph() before file operations.\n" +
                          "This enforces CLAUDE.md mandatory workflow requirements."
            }

    return {"allow": True, "message": "Tool allowed"}

def suggest_knowledge_graph_update(tool_name, args, result):
    """
    Suggest knowledge graph updates after file modifications
    """
    if tool_name in ["Write", "Edit", "MultiEdit"] and result.get("success"):
        file_path = args.get("file_path") or "modified files"

        log_enforcement_action("update_suggestion", {
            "tool": tool_name,
            "file_path": file_path,
            "result_success": result.get("success")
        })

        return {
            "type": "reminder",
            "message": f"📚 MANDATORY: Update knowledge graph with changes to {file_path}\n" +
                      "Use: aim_add_observations() to document modifications per CLAUDE.md requirements."
        }

    return None

def main():
    """Main enforcement entry point"""
    if len(sys.argv) < 2:
        print("Usage: knowledge-graph-enforcement.py <action> [data]")
        sys.exit(1)

    action = sys.argv[1]
    data = json.loads(sys.argv[2]) if len(sys.argv) > 2 else {}

    if action == "pre_tool_use":
        tool_name = data.get("tool_name")
        args = data.get("args", {})
        context = data.get("context", {})

        result = enforce_knowledge_graph_workflow(tool_name, args, context)
        print(json.dumps(result))

        if not result["allow"]:
            sys.exit(1)

    elif action == "post_tool_use":
        tool_name = data.get("tool_name")
        args = data.get("args", {})
        result = data.get("result", {})

        suggestion = suggest_knowledge_graph_update(tool_name, args, result)
        if suggestion:
            print(json.dumps(suggestion))

    elif action == "session_start":
        print(json.dumps({
            "type": "requirement",
            "message": "📚 SESSION START: Load project context with aim_read_graph() before beginning work.\n" +
                      "This is mandatory per CLAUDE.md knowledge graph requirements."
        }))

    sys.exit(0)

if __name__ == "__main__":
    main()