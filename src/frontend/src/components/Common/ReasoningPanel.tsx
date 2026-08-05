/**
 * The model's reasoning/thinking, collapsed by default.
 *
 * Reasoning is deliberately NOT part of the answer — the backend splits it out
 * (see `split_message_content` in core/llm/transport/response_parsing.py) so a
 * model's private deliberation never lands in task output or memory. This is the
 * one place it surfaces, and it starts collapsed for the same reason: it is
 * context for a curious reader, not the result.
 *
 * Shared by the three surfaces that show run activity — the trace timeline
 * (Jobs), Agent Builder and ChatMode — so the affordance is identical in all of
 * them and there is one component to change.
 *
 * Renders NOTHING when there is no reasoning text. Three cases produce content:
 *  - unprompted: the gemini-3.x family (with `reasoning_effort`), inkling, kimi
 *  - opt-in: any Claude with Extended Thinking enabled on the model, which makes
 *    the transport send `thinking` + `display: "summarized"`
 *  - the REDACTED sentinel, when a reasoning block arrived carrying only an
 *    encrypted `signature` — usually because Extended Thinking is OFF
 *
 * GPT-5 never produces content: it reasons, and the trace is not retrievable via
 * chat completions at all.
 */
import React, { useState } from 'react';
import { Box, Collapse, Link, Typography } from '@mui/material';
import ExpandMoreIcon from '@mui/icons-material/ExpandMore';
import PsychologyIcon from '@mui/icons-material/Psychology';

export interface ReasoningPanelProps {
  /** The reasoning text. Empty/undefined renders nothing at all. */
  reasoning?: unknown;
  /** Start expanded. Default false — this is supporting detail. */
  defaultExpanded?: boolean;
  /**
   * Optional cap on the body height, which makes it scroll internally.
   *
   * UNCAPPED by default, deliberately: expanding is an explicit request to read
   * the whole thing, and a fixed inner box turns a 2,000-character train of
   * thought into a nested scrollbar you have to fight. Let it grow and let the
   * surrounding pane (which already scrolls) do the scrolling.
   */
  maxHeight?: number | string;
  /** Extra sx applied to the outer box (spacing differs per surface). */
  sx?: object;
}

/**
 * Sentinel the backend sends when the model DID reason but the provider withheld
 * the text. Must match `REDACTED_REASONING` in
 * core/llm/transport/response_parsing.py.
 */
export const REDACTED_REASONING = '__kasal_reasoning_redacted__';

/**
 * Seeded Databricks models that actually return reasoning TEXT, newest probe
 * first by volume. Every one was verified against the live workspace on
 * 2026-08-05 by asking a step-by-step question and reading the response:
 *
 *   gemini-3-1-flash-lite  2,226 chars    inkling         309 chars
 *   gemini-3-5-flash       2,104 chars    kimi-k2-7-code  137 chars
 *   gemini-3-1-pro         1,648 chars
 *
 * The Gemini three need `reasoning_effort` on the request (see
 * utils/model_config._REASONING_EFFORT_SUBSTRINGS); inkling and kimi return
 * `reasoning_content` unprompted.
 *
 * Deliberately NOT exhaustive-by-inference: this is a list of models observed
 * returning text, not a guess from model family. gpt-5* accepts a reasoning
 * budget and still returns nothing, which is why naming families would mislead.
 */
export const REASONING_VISIBLE_MODELS = [
  'databricks-gemini-3-1-pro',
  'databricks-gemini-3-5-flash',
  'databricks-gemini-3-1-flash-lite',
  'databricks-inkling',
  'databricks-kimi-k2-7-code',
] as const;

/** The reasoning text, or '' when this row carries none. */
export function reasoningText(value: unknown): string {
  if (typeof value === 'string') return value.trim();
  // Defensive: a provider could hand back a list of summary parts.
  if (Array.isArray(value)) {
    return value
      .map(part =>
        typeof part === 'string'
          ? part
          : typeof part === 'object' && part !== null
            ? String((part as Record<string, unknown>).text ?? '')
            : '',
      )
      .join('')
      .trim();
  }
  return '';
}

export const ReasoningPanel: React.FC<ReasoningPanelProps> = ({
  reasoning,
  defaultExpanded = false,
  maxHeight,
  sx = {},
}) => {
  const [expanded, setExpanded] = useState(defaultExpanded);
  const text = reasoningText(reasoning);
  if (!text) return null;

  // The model reasoned but the provider encrypted the trace (Anthropic Claude on
  // Databricks). Showing nothing here would read as "this model does not think",
  // which is the wrong claim — so say what actually happened.
  const redacted = text === REDACTED_REASONING;
  const summary = redacted
    ? 'Reasoning (hidden by provider)'
    : `Reasoning (${text.length.toLocaleString()} chars)`;

  return (
    <Box sx={{ mt: 1, ...sx }}>
      <Box
        role="button"
        tabIndex={0}
        aria-expanded={expanded}
        aria-label={expanded ? 'Hide model reasoning' : 'Show model reasoning'}
        onClick={() => setExpanded(v => !v)}
        onKeyDown={e => {
          if (e.key === 'Enter' || e.key === ' ') {
            e.preventDefault();
            setExpanded(v => !v);
          }
        }}
        // Styled as an obvious control, not a label. The first version was
        // muted grey text plus a small chevron, which read as a static heading —
        // people saw "Reasoning (732 chars)" and reported the content missing
        // when it was merely collapsed. Chip-like border + accent colour + an
        // explicit Show/Hide verb make the affordance unmistakable.
        sx={{
          display: 'inline-flex',
          alignItems: 'center',
          gap: 0.5,
          cursor: 'pointer',
          color: 'primary.main',
          border: 1,
          borderColor: 'divider',
          borderRadius: 4,
          py: 0.25,
          px: 1,
          '&:hover': { bgcolor: 'action.hover', borderColor: 'primary.main' },
        }}
      >
        <PsychologyIcon sx={{ fontSize: 16 }} />
        <Typography variant="caption" sx={{ fontWeight: 600 }}>
          {summary}
        </Typography>
        <Typography variant="caption" sx={{ opacity: 0.8 }}>
          — {expanded ? 'hide' : 'show'}
        </Typography>
        <ExpandMoreIcon
          sx={{
            fontSize: 16,
            transition: 'transform 150ms',
            transform: expanded ? 'rotate(180deg)' : 'none',
          }}
        />
      </Box>

      <Collapse in={expanded} unmountOnExit>
        <Box
          sx={{
            mt: 0.5,
            p: 1,
            // Only constrain (and therefore scroll) when a caller asks for it.
            ...(maxHeight ? { maxHeight, overflow: 'auto' } : {}),
            bgcolor: 'action.hover',
            borderLeft: 2,
            borderColor: 'divider',
            borderRadius: 0.5,
          }}
        >
          {redacted ? (
            <Typography variant="caption" sx={{ color: 'text.secondary' }} component="div">
              This model reasoned before answering but returned only an encrypted{' '}
              <code>signature</code>, with no thinking text.
              <Box component="p" sx={{ mt: 1, mb: 0 }}>
                For Anthropic Claude this is usually fixable:{' '}
                <strong>enable Extended Thinking</strong> on the model in Settings.
                Claude only returns thinking text when the request asks for it —{' '}
                <code>display</code> defaults to <code>&quot;omitted&quot;</code>{' '}
                on Claude 5, Fable 5, Opus 4.7 and Opus 4.8, which
                &ldquo;returns thinking blocks with an empty{' '}
                <code>thinking</code> field&rdquo; (
                <Link
                  href="https://platform.claude.com/docs/en/build-with-claude/thinking#controlling-thinking-display"
                  target="_blank"
                  rel="noopener noreferrer"
                >
                  Anthropic: Controlling thinking display
                </Link>
                ). With it enabled, Kasal opts in and the summary comes back.
              </Box>
              <Box component="p" sx={{ mt: 1, mb: 0 }}>
                Note that no setting returns the raw chain of thought — what you
                get is a summary of it, by design.
              </Box>
              <Box component="p" sx={{ mt: 1, mb: 0 }}>
                The GPT-5 family is different: it reasons but the trace is
                unobtainable. &ldquo;While reasoning tokens are not visible via
                the API, they still occupy space in the model&apos;s context
                window and are billed as output tokens&rdquo; (
                <Link
                  href="https://developers.openai.com/api/docs/guides/reasoning"
                  target="_blank"
                  rel="noopener noreferrer"
                >
                  OpenAI: Reasoning
                </Link>
                ). Summaries exist only on the Responses API, which this endpoint
                does not expose — observed here as{' '}
                <code>reasoning_tokens</code> billed (1,344 at effort=high) with
                no reasoning field in the message.
              </Box>
              <Box component="p" sx={{ mt: 1, mb: 0 }}>
                Reasoning is visible without any configuration on:{' '}
                {REASONING_VISIBLE_MODELS.map((m, i) => (
                  <React.Fragment key={m}>
                    {i > 0 && ', '}
                    <code>{m}</code>
                  </React.Fragment>
                ))}
                . Llama, Qwen and Gemma do not reason.
              </Box>
            </Typography>
          ) : (
            <Typography
              variant="caption"
              component="pre"
              sx={{
                m: 0,
                whiteSpace: 'pre-wrap',
                wordBreak: 'break-word',
                fontFamily: 'monospace',
                color: 'text.secondary',
              }}
            >
              {text}
            </Typography>
          )}
        </Box>
      </Collapse>
    </Box>
  );
};

export default ReasoningPanel;
