"""Follow-up policies for MCP servers that expose long-running work as a
start-tool + poll-tool pair.

MCP itself is agnostic — and so is this package. Some servers model an async
job as TWO tools: the start tool returns an in-progress envelope and the
CALLER is expected to poll a sibling tool until the work finishes. LLM agents
are unreliable at driving that loop (they give up after a poll or two and
fabricate an answer, or mix the ids up), so Kasal follows the pair internally
and hands the agent one finished result.

The split keeps the layers honest:

* :mod:`.runner` — the machinery: poll loop, deadline, failure cap, logging.
* :mod:`.config` — specs built from the SERVER'S OWN ``follow`` configuration
  (``additional_config.follow`` on the MCP server record). No server is named
  in code; the managed Databricks Genie catalog entries ship their convention
  as preset data, and any other server can declare the same.
"""

from src.services.tools.mcp_follow.config import follow_spec_from_config
from src.services.tools.mcp_follow.runner import FollowSpec, follow_tool_call

__all__ = ["FollowSpec", "follow_tool_call", "follow_spec_from_config"]
