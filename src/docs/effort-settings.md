# Effort settings

Effort controls native model reasoning, rounds per agent turn, execution time,
and output tokens per request. Choices follow the served model's capabilities.
Models without native reasoning still receive runtime and output limits.

In the Agent form, select **LLM Configuration → Effort**. The selection is saved
with the agent and is used by catalog crews and API launches. Selecting a preset
sets its rounds/time defaults and clears previous native/output overrides.
Individual overrides in the form can then refine it.

The builder's input menu defaults to **Use agent settings**. Selecting another
effort there explicitly overrides the agents for that run. Chat saves its own
preference and applies it to its light agent through the same backend resolver.
Loading a catalog crew in Chat preserves that crew's agent settings.

Precedence is model configuration, saved agent effort, explicit saved agent
fields, then an explicit run effort override. Endpoint output ceilings still
apply. An unsupported native tier maps to the nearest supported lower tier (or
the lowest supported tier); the effective value is logged.

## API

Create/update an agent with `execution_effort`, for example:

```json
{
  "execution_effort": { "tier": "high" },
  "max_iter": 20
}
```

The API fills omitted per-agent limits from the profile and preserves explicitly
supplied limits. Setting `execution_effort` to `null` removes the preset; existing
individual settings remain. A tier-only PATCH resets rounds/time to that tier's
defaults. Updating only an individual field keeps the saved tier.

For an explicit crew run override, send:

```json
{
  "inputs": {
    "execution_effort": {
      "tier": "high",
      "max_iter": 30,
      "max_execution_time": 900,
      "run_max_seconds": 1200
    }
  }
}
```

Supported tiers and default limits are returned as `effort_profiles` by
`GET /models/enabled`. Tier names are `none`, `minimal`, `low`, `medium`, `high`,
`xhigh`, and `max`; each model exposes its accepted native subset.

The whole crew shares one deadline across agents, retries, and parallel tasks.
Without a run override, profiled agents use the largest saved profile's run
allowance as the crew deadline, without multiplying it by agent count. Per-agent
turn limits apply within that deadline. Legacy agents without a profile keep
their previous limits. Kasal and CrewAI use the same LLM resolution and deadline
transport. Execution history snapshots the agent settings used for the launch.

Effort does not change task guardrails or their failure policy. Chat preserves
partial output at exhaustion and records a failed, rather than completed, run.
Output limits apply to each LLM request, including reasoning tokens; they are
not cumulative token or spending caps. A round may invoke several tools.
