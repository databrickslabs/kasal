#!/usr/bin/env python3
"""
Power BI MCP Integration Test Script (with argparse)

Tests Power BI authentication and querying with Service Principal.
Uses command-line arguments or environment variables.

NOTE: This is a standalone CLI script, not a pytest test module.
      All executable code is guarded behind ``if __name__ == "__main__"``
      so that pytest can safely collect files in this directory.

Usage:
    # Use all defaults (requires environment variables to be set)
    python test_powerbi_mcp.py

    # Specify parameters
    python test_powerbi_mcp.py \
        --tenant-id "<YOUR_AZURE_TENANT_ID>" \
        --client-id "<YOUR_AZURE_CLIENT_ID>" \
        --client-secret "<YOUR_CLIENT_SECRET>" \
        --workspace-id "<YOUR_POWERBI_WORKSPACE_ID>" \
        --semantic-model-id "<YOUR_POWERBI_SEMANTIC_MODEL_ID>" \
        --question "What is the average Net Sales Revenue (NSR) per product?"
"""


def main():
    # Import the libs
    import os
    import msal
    import requests
    import argparse

    # Fetch input parameters
    parser = argparse.ArgumentParser(
        description="Test Power BI Service Principal authentication and querying",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="")

    parser.add_argument(
        "--tenant-id",
        default=os.environ.get("AZURE_TENANT_ID", ""),
        help="Azure AD Tenant ID (default: from AZURE_TENANT_ID env var)",
    )

    parser.add_argument(
        "--client-id",
        default=os.environ.get("AZURE_CLIENT_ID", ""),
        help="Azure AD Client ID (Application ID) (default: from AZURE_CLIENT_ID env var)",
    )

    parser.add_argument(
        "--client-secret",
        default=os.environ.get("AZURE_CLIENT_SECRET", ""),
        help="Azure AD Client Secret (default: from AZURE_CLIENT_SECRET env var)",
    )

    parser.add_argument(
        "--workspace-id",
        default=os.environ.get("POWERBI_WORKSPACE_ID", ""),
        help="Power BI Workspace ID (default: from POWERBI_WORKSPACE_ID env var)",
    )

    parser.add_argument(
        "--semantic-model-id",
        default=os.environ.get("POWERBI_SEMANTIC_MODEL_ID", ""),
        help="Power BI Semantic Model ID (Dataset ID) (default: from POWERBI_SEMANTIC_MODEL_ID env var)",
    )

    # Query parameters
    parser.add_argument(
        "--question",
        default="What is the average net sales revenue (NSR) per product?",
        help="Any user-query that should be answered based upon your dataset",
    )

    args = parser.parse_args()

    # Service Principal Auth
    authority = f"https://login.microsoftonline.com/{args.tenant_id}"
    app = msal.ConfidentialClientApplication(
        args.client_id,
        authority=authority,
        client_credential=args.client_secret,
    )

    # Get token for Power BI API
    result = app.acquire_token_for_client(
        scopes=["https://analysis.windows.net/powerbi/api/.default"]
    )

    if not result or "access_token" not in result:
        error_msg = result.get("error_description", "Unknown error") if result else "No response"
        raise RuntimeError(f"Failed to acquire token: {error_msg}")

    access_token = result["access_token"]

    # Use with Power BI REST API
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }

    # Now call Power BI APIs or MCP remote server
    response = requests.post(
        "https://api.fabric.microsoft.com/v1/mcp/powerbi",
        headers=headers,
        json={"query": args.question}
    )
    print(response)


if __name__ == "__main__":
    main()