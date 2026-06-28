from pathlib import Path

from dotenv import load_dotenv
from fastapi import Request
from mlflow.genai.agent_server import AgentServer

from agent_server.otel import setup_otel_logging

# Load env vars from .env before importing the agent for proper auth
load_dotenv(dotenv_path=Path(__file__).parent.parent / ".env", override=True)

# Export app logs to Unity Catalog (otel_logs) when Databricks App telemetry is
# enabled in the app settings (no-op otherwise). Set up before the agent imports
# so its startup logs are captured too.
setup_otel_logging()

# Need to import the agent to register the functions with the server
import agent_server.agent  # noqa: E402

# We do NOT use enable_chat_proxy. The MLflow chat proxy decompresses upstream
# gzip bodies but forwards the original `Content-Encoding: gzip` header, so the
# browser tries to gunzip already-plain bytes and fails with
# ERR_CONTENT_DECODING_FAILED on every JS/CSS asset. Instead we serve the built
# SPA directly from this app (single origin, single process) via StaticFiles,
# which never double-encodes. The agent's /invocations endpoint is unaffected.
agent_server = AgentServer("ResponsesAgent")
# Define the app as a module level variable to enable multiple workers
app = agent_server.app  # noqa: F841


@app.get("/me")
def _whoami(request: Request):
    """Identify the signed-in user from Databricks' forwarded headers.

    Databricks Apps inject the authenticated identity as X-Forwarded-* request
    headers; locally (no proxy) these are absent and the UI falls back to "You".
    """
    h = request.headers
    email = h.get("x-forwarded-email") or h.get("x-forwarded-user") or ""
    username = h.get("x-forwarded-preferred-username") or ""
    name = username or (email.split("@")[0] if email else "")
    return {"email": email, "username": username, "name": name}


@app.get("/progress/{conversation_id}")
def _progress(conversation_id: str):
    """Subtle, ephemeral "what is the agent doing right now" for the UI to poll
    while a turn runs. Returns {status, seq} or {status: null} when idle. Nothing
    is persisted — see agent_server.progress."""
    from agent_server import progress

    return progress.get(conversation_id) or {"status": None}


@app.get("/a2ui/{conversation_id}")
def _a2ui(conversation_id: str):
    """Poll for this turn's A2UI surface. It is composed out-of-band so the answer
    request returns fast (Databricks Apps time out long-held connections). Returns
    {status: pending|ready|none|idle, surface}. See agent_server.a2ui_store."""
    from agent_server import a2ui_store

    return a2ui_store.get(conversation_id)


@app.post("/cancel/{conversation_id}")
def _cancel(conversation_id: str):
    """Stop the running turn for this conversation. Cooperative — the crew aborts
    at the next step boundary (before the next LLM call), so token spend stops
    even though the in-flight request finishes. See agent_server.cancel."""
    from agent_server import cancel

    cancel.request(conversation_id)
    return {"cancelled": True}


# Mount the bundled, pre-built frontend (frontend/dist) at the web root. Added
# AFTER the AgentServer routes so /invocations and friends still match first;
# everything else (index.html, /assets/*) is served from the static build.
# html=True makes "/" return index.html.
_FRONTEND_DIST = Path(__file__).parent.parent / "frontend" / "dist"
if _FRONTEND_DIST.is_dir():
    from fastapi.staticfiles import StaticFiles

    app.mount(
        "/", StaticFiles(directory=str(_FRONTEND_DIST), html=True), name="frontend"
    )
    print(f"[start_server] Serving bundled UI from {_FRONTEND_DIST}")
else:
    print(
        f"[start_server] No bundled UI at {_FRONTEND_DIST} "
        "(run the frontend build); serving API only."
    )

# NOTE: we intentionally do NOT call setup_mlflow_git_based_version_tracking().
# It needs a git checkout (apps-deploy has none), so it creates a junk
# "<name>-no-git" LoggedModel, sets it active, and links every trace to that
# model instead of leaving them clean in the experiment. Traces still flow to
# MLFLOW_EXPERIMENT_ID via mlflow.crewai.autolog() + the @mlflow.trace on each
# conversation turn — no version tracking needed.


def main():
    agent_server.run(app_import_string="agent_server.start_server:app")
