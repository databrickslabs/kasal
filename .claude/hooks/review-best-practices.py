#!/usr/bin/env python3
"""
Best Practices Review Hook for Kasal Project
Triggers the best-practices-reviewer agent to verify code compliance
"""

import json
import subprocess
import sys
from pathlib import Path

def get_changed_files():
    """Get list of files that will be pushed"""
    try:
        # Get files that are staged or modified
        result = subprocess.run(
            ['git', 'diff', '--name-only', '--cached', 'HEAD'],
            capture_output=True,
            text=True,
            check=True
        )
        staged_files = result.stdout.strip().split('\n') if result.stdout.strip() else []
        
        # Also get files modified but not staged (for comprehensive review)
        result = subprocess.run(
            ['git', 'diff', '--name-only', 'HEAD'],
            capture_output=True,
            text=True,
            check=True
        )
        modified_files = result.stdout.strip().split('\n') if result.stdout.strip() else []
        
        # Combine and filter for relevant files
        all_files = set(staged_files + modified_files)
        
        # Filter for Python and TypeScript/JavaScript files
        relevant_extensions = {'.py', '.ts', '.tsx', '.js', '.jsx'}
        changed_files = [
            f for f in all_files 
            if f and Path(f).suffix in relevant_extensions
        ]
        
        return changed_files
    except subprocess.CalledProcessError:
        return []

def trigger_review_agent():
    """
    Trigger the best-practices-reviewer agent via Claude Code CLI
    """
    changed_files = get_changed_files()
    
    if not changed_files:
        print("✅ No relevant files to review")
        return 0
    
    print(f"🔍 Reviewing {len(changed_files)} file(s) for best practices compliance...")
    print(f"   Files: {', '.join(changed_files[:5])}" + 
          (f" and {len(changed_files)-5} more" if len(changed_files) > 5 else ""))
    
    # Create the review request for Claude Code
    review_request = {
        "action": "review",
        "files": changed_files,
        "agent": "best-practices-reviewer",
        "context": "Pre-push hook triggered for best practices verification"
    }
    
    # Output for Claude Code to process
    print("\n" + "="*60)
    print("CLAUDE CODE AGENT REVIEW REQUEST")
    print("="*60)
    print(json.dumps(review_request, indent=2))
    print("="*60)
    
    # Note: In actual implementation, this would trigger Claude Code's Task tool
    # For now, we'll output instructions for manual review
    print("\n📋 To complete the review, run:")
    print("   Use the Task tool with the 'best-practices-reviewer' agent")
    print("   to review the modified files for compliance with CLAUDE.md rules")
    
    # Return 0 to allow push (non-blocking for now)
    # Change to return 1 to block push on violations
    return 0

def main():
    """Main hook entry point"""
    print("\n🚀 Kasal Best Practices Pre-Push Hook")
    print("-" * 40)
    
    exit_code = trigger_review_agent()
    
    if exit_code == 0:
        print("\n✅ Pre-push hook completed. Proceeding with push...")
    else:
        print("\n❌ Pre-push hook failed. Push blocked.")
        print("   Fix the issues and try again.")
    
    return exit_code

if __name__ == "__main__":
    sys.exit(main())