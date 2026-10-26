"""
Chat — the answer path behind ChatMode in the UI.

One agent, run IN-PROCESS (``Agent.kickoff_async``) for sub-second latency: no
crew, no task graph, no planning pass. It writes its own terminal status so a
fast answer is fetchable over REST without waiting on the trace pipeline.

    ChatMode UI -> dispatcher/chat routes -> ExecutionService(execution_type="agent")
                -> KasalExecutionService -> chat.service.run_light_agent_execution

Its memory wiring, tool tracing and A2UI surface composition MIRROR the crew
path but stay independent on purpose — do not merge them to remove the ~8 lines
of glue that differ.

Naming: this is "chat" in the UI and "light agent" in the code and
``execution_type="agent"`` on the wire. The wire value is persisted in
execution_history rows, so it cannot be renamed without a migration; the code
names are being brought over to the UI vocabulary as each piece moves.
"""
