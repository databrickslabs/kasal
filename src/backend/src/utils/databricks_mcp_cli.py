"""The Databricks CLI token used for MCP, fetched without blocking the loop.

Split out of ``databricks_auth`` (which re-exports it). ``databricks auth
token`` is a subprocess: it runs on a worker thread and is bounded, because a
hung CLI (for example one waiting on an interactive login) used to freeze the
whole server with no timeout.
"""

import asyncio
import json
import logging
import subprocess
from typing import Optional, Tuple

logger = logging.getLogger(__name__)

# Upper bound for `databricks auth token`; it normally answers in about a second.
_MCP_CLI_TIMEOUT_SECONDS = 15


async def get_mcp_access_token() -> Tuple[Optional[str], Optional[str]]:
    """
    Get an MCP access token by calling the Databricks CLI directly.
    This is the most reliable approach since we know 'databricks auth token -p mcp' works.

    Returns:
        Tuple[Optional[str], Optional[str]]: (access_token, error_message)
    """
    try:
        logger.info("Getting MCP token using Databricks CLI")

        # Call the CLI command that we know works. Off the event loop, and
        # bounded: a hung CLI (e.g. waiting on a login) used to freeze the
        # server indefinitely.
        result = await asyncio.to_thread(
            subprocess.run,
            ["databricks", "auth", "token", "-p", "mcp"],
            capture_output=True,
            text=True,
            check=True,
            timeout=_MCP_CLI_TIMEOUT_SECONDS,
        )

        # Parse the JSON output
        token_data = json.loads(result.stdout)
        access_token = token_data.get("access_token")

        if not access_token:
            return None, "No access token found in CLI response"

        # Verify this is a JWT token (should start with eyJ)
        if access_token.startswith("eyJ"):
            logger.info("Successfully obtained JWT token from CLI for MCP")
            return access_token, None
        else:
            logger.warning(f"Token doesn't look like JWT: {access_token[:20]}...")
            return access_token, None

    except subprocess.CalledProcessError as e:
        return None, f"CLI command failed: {e.stderr}"
    except subprocess.TimeoutExpired:
        return None, f"CLI command timed out after {_MCP_CLI_TIMEOUT_SECONDS}s"
    except json.JSONDecodeError as e:
        return None, f"Failed to parse CLI output: {e}"
    except Exception as e:
        logger.error(f"Error getting MCP token from CLI: {e}")
        return None, str(e)
