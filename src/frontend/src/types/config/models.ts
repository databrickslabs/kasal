export interface ModelConfig {
  name: string;
  temperature?: number;
  provider?: string;
  extended_thinking?: boolean;
  context_window?: number;
  max_output_tokens?: number;
  enabled?: boolean;
  /**
   * Whether this model accepts a native reasoning-effort budget. Derived
   * server-side from the same allow-list the engine uses
   * (backend src/utils/model_config.py), so the UI cannot offer a Reasoning
   * Effort setting that the engine will silently discard.
   */
  supports_reasoning_effort?: boolean;
  /**
   * Which Anthropic thinking control this model takes. Derived server-side from
   * the transport's own model lists (backend
   * core/llm/transport/completion.thinking_mode), for the same reason as
   * `supports_reasoning_effort`: the two controls are mutually exclusive and
   * sending the wrong one is a hard 400 on a real run, so the UI must not decide
   * this for itself.
   *
   *  'manual'   — takes a token budget (Claude 4.1–4.6)
   *  'adaptive' — takes an effort level (Claude 4.7+/5/Fable)
   *  undefined  — no thinking surface; show neither control
   */
  thinking_mode?: 'manual' | 'adaptive' | null;
  /** Token budget, MANUAL models only. Null/absent = Kasal default. */
  thinking_budget_tokens?: number | null;
  /** Depth, models with an effort scale only. Null/absent = endpoint default. */
  reasoning_effort?: string | null;
  /**
   * The effort values THIS model accepts, increasing in depth — server-derived
   * and NEVER to be hardcoded here. There are five distinct scales across the
   * catalogue and a value the endpoint refuses is a 400, not a warning:
   * Anthropic adaptive takes low..max, gpt-5 takes minimal..high but rejects
   * 'none', gpt-5-1 takes 'none' but rejects 'minimal', the 5-2/5-4/5-6 line
   * adds 'xhigh', and Gemini takes only low/medium/high.
   *
   * Empty means this model has no effort control — render none.
   */
  allowed_efforts?: string[];
  /**
   * Sampling parameters this model REJECTS. Hide a control for each: the
   * catalogue used to declare nothing, which is why Edit Model offered
   * `temperature` on claude-opus-5, a model that answers it with a 400.
   */
  refused_params?: string[];
  /**
   * Whether the thinking TEXT can be displayed. False does not mean the model
   * does not reason — every gpt-5* reasons and bills for it, and simply never
   * returns the trace.
   */
  returns_thinking_text?: boolean;
}

export interface Models {
  [key: string]: ModelConfig;
} 