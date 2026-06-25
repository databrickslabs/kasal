from pathlib import Path

from dotenv import load_dotenv
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

agent_server = AgentServer("ResponsesAgent", enable_chat_proxy=True)
# Define the app as a module level variable to enable multiple workers
app = agent_server.app  # noqa: F841

# NOTE: we intentionally do NOT call setup_mlflow_git_based_version_tracking().
# It needs a git checkout (apps-deploy has none), so it creates a junk
# "<name>-no-git" LoggedModel, sets it active, and links every trace to that
# model instead of leaving them clean in the experiment. Traces still flow to
# MLFLOW_EXPERIMENT_ID via mlflow.crewai.autolog() + the @mlflow.trace on each
# conversation turn — no version tracking needed.


def main():
    agent_server.run(app_import_string="agent_server.start_server:app")
