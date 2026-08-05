/**
 * Readable view of an LLM request/response row in the trace timeline.
 *
 * The generic output view dumps the row's JSON, which for an LLM call means a
 * wall of escaped `\n` around the one thing worth reading — the prompt. This
 * pulls the text out, puts the model/token/timing facts on chips, and leaves
 * the raw object one click away.
 */
import React, { useState } from 'react';
import { Alert, Box, Button, Chip, Stack, Typography } from '@mui/material';
import { PaginatedOutput, ReasoningPanel } from '../Common';
import { SelectedTraceEvent } from '../../types/execution/trace';
import { asCount, asText, extractLlmText } from './llmEventText';

export interface LlmEventDetailsProps {
  event: SelectedTraceEvent;
  /** Height of the text pane; matches the generic output view. */
  maxHeight?: string;
}

export const LlmEventDetails: React.FC<LlmEventDetailsProps> = ({ event, maxHeight = '55vh' }) => {
  const [showRaw, setShowRaw] = useState(false);

  const extra = (event.extraData ?? {}) as Record<string, unknown>;
  const text = extractLlmText(event);
  const isRequest = event.type !== 'llm_response';

  const model = asText(extra.model);
  const messageCount = asCount(extra.message_count);
  const promptTokens = asCount(extra.prompt_tokens);
  const completionTokens = asCount(extra.completion_tokens);
  const totalTokens = asCount(extra.total_tokens);
  const cachedTokens = asCount(extra.cached_prompt_tokens);

  const chips: JSX.Element[] = [];
  if (model) {
    chips.push(<Chip key="model" size="small" color="primary" variant="outlined" label={model} />);
  }
  if (messageCount != null) {
    chips.push(<Chip key="messages" size="small" variant="outlined" label={`${messageCount} messages`} />);
  }
  if (promptTokens != null) {
    chips.push(<Chip key="prompt-tokens" size="small" variant="outlined" label={`${promptTokens.toLocaleString()} prompt tokens`} />);
  }
  if (cachedTokens) {
    chips.push(<Chip key="cached-tokens" size="small" variant="outlined" color="success" label={`${cachedTokens.toLocaleString()} cached`} />);
  }
  if (completionTokens != null) {
    chips.push(<Chip key="completion-tokens" size="small" variant="outlined" label={`${completionTokens.toLocaleString()} completion tokens`} />);
  }
  if (totalTokens != null) {
    chips.push(<Chip key="total-tokens" size="small" variant="outlined" label={`${totalTokens.toLocaleString()} total`} />);
  }

  const isMemoryLabelling = extra.llm_purpose === 'memory_labelling';

  return (
    <Box>
      {isMemoryLabelling && (
        <Alert severity="info" variant="outlined" sx={{ mb: 2 }}>
          Memory bookkeeping, not the agent&apos;s reasoning: after the task finished, the
          memory layer asked the model to tag the record it was saving. That is why it
          appears below &ldquo;Task Completed&rdquo;.
        </Alert>
      )}

      {chips.length > 0 && (
        <Stack direction="row" spacing={1} sx={{ flexWrap: 'wrap', gap: 1, mb: 2 }}>
          {chips}
        </Stack>
      )}

      {/* The model's thinking, collapsed. Above the answer because it came
          first, and separate because the backend deliberately keeps it out of
          the response text. Renders nothing when the model sent none. */}
      <ReasoningPanel reasoning={extra.reasoning} sx={{ mt: 0, mb: 2 }} />

      {text ? (
        <>
          <Typography variant="caption" color="text.secondary" sx={{ display: 'block', mb: 0.5 }}>
            {isMemoryLabelling
              ? (isRequest ? 'Record being labelled' : 'Labels applied')
              : (isRequest ? 'Prompt' : 'Response')}
          </Typography>
          <PaginatedOutput
            content={text}
            pageSize={10000}
            // The prompt is assembled text, not markdown — rendering it as
            // markdown eats the '#', '-' and '*' the instructions rely on.
            enableMarkdown={!isRequest}
            showCopyButton
            maxHeight={maxHeight}
            eventType={event.type}
          />
        </>
      ) : (
        <Typography variant="body2" color="text.secondary" sx={{ mb: 2 }}>
          This row carries no prompt or response text.
        </Typography>
      )}

      <Box sx={{ mt: 1 }}>
        <Button size="small" onClick={() => setShowRaw(v => !v)}>
          {showRaw ? 'Hide raw event data' : 'Show raw event data'}
        </Button>
        {showRaw && (
          <Box sx={{ mt: 1 }}>
            <PaginatedOutput
              content={event.output ?? extra}
              pageSize={10000}
              showCopyButton
              maxHeight="30vh"
              eventType={event.type}
            />
          </Box>
        )}
      </Box>
    </Box>
  );
};

export default LlmEventDetails;
