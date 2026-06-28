import React, { useState } from 'react';
import Box from '@mui/material/Box';
import GenieSpaceSelector from './GenieSpaceSelector';
import { GenerationCompleteData } from '../../types/dispatcher';
import { stripGenieTools } from '../../api/crews';
import { useSessionStore } from '../../store/sessionStore';
import { buttonResetSx } from '../../chatSx';

/**
 * Minimal inline Genie-space prompt — replaces the full crew card in the chat
 * flow. A Genie crew can't run until a space is picked, so this is the ONLY
 * crew-generation UI that remains in the conversation; generation steps fold
 * into the run-activity element and results render in the preview pane.
 */

// Selection survives remounts within the session; the write-through to the
// message's resultData (server-side) makes it survive session switches too.
const promptStore = new Map<string, { spaceId: string; ran: boolean }>();

interface GenieSpacePromptProps {
  data: GenerationCompleteData;
  messageId: string;
  onExecute?: (data: GenerationCompleteData, spaceId?: string) => void;
}

const GenieSpacePrompt: React.FC<GenieSpacePromptProps> = ({ data, messageId, onExecute }) => {
  const persisted = data as GenerationCompleteData & { genieSpaceId?: string; genieRan?: boolean };
  const [selectedSpaceId, setSelectedSpaceId] = useState(
    () => promptStore.get(messageId)?.spaceId ?? persisted.genieSpaceId ?? '',
  );
  const [ran, setRan] = useState(
    () => promptStore.get(messageId)?.ran ?? persisted.genieRan ?? false,
  );

  const persist = (next: { spaceId?: string; ran?: boolean }) => {
    const prev = promptStore.get(messageId) ?? { spaceId: selectedSpaceId, ran };
    const merged = { ...prev, ...next };
    promptStore.set(messageId, merged);
    try {
      useSessionStore.getState().updateMessage(messageId, {
        resultType: 'genie_space_prompt',
        resultData: { ...data, genieSpaceId: merged.spaceId, genieRan: merged.ran },
      });
    } catch {
      /* best-effort; in-memory store still covers the live session */
    }
  };

  return (
    <Box sx={{ px: 2, my: 1, maxWidth: '48rem' }}>
      <Box sx={{ pt: 0.5, '& > * + *': { mt: 1 } }}>
        <Box component="p" sx={{ fontSize: 12, px: 0.5, color: 'text.disabled' }}>
          This crew queries a Genie space — pick one to run.
        </Box>
        <GenieSpaceSelector
          value={selectedSpaceId}
          onChange={(v) => {
            setSelectedSpaceId(v);
            persist({ spaceId: v });
          }}
        />
        <Box
          component="button"
          type="button"
          onClick={() => {
            setRan(true);
            persist({ ran: true });
            onExecute?.(data, selectedSpaceId);
          }}
          disabled={!selectedSpaceId || ran}
          sx={{
            ...buttonResetSx,
            width: '100%',
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'center',
            gap: 1,
            borderRadius: '8px',
            px: 1.5,
            py: 1,
            fontSize: 14,
            fontWeight: 500,
            transition: 'all 0.15s',
            backgroundColor: (t) => t.chat.bgSecondary,
            color: 'text.secondary',
            border: 1,
            borderColor: 'divider',
            '&:hover': { opacity: 0.8 },
            '&:disabled': { opacity: 0.5, cursor: 'not-allowed' },
          }}
        >
          <Box component="svg" sx={{ width: 16, height: 16 }} fill="currentColor" viewBox="0 0 24 24">
            <path d="M8 5v14l11-7z" />
          </Box>
          {ran ? 'Running…' : selectedSpaceId ? 'Run crew' : 'Select a Genie space to run'}
        </Box>
        {/* Skip: run WITHOUT Genie — the tool is stripped from the generated
            crew so it doesn't run blind against an unconfigured space. */}
        <Box
          component="button"
          type="button"
          onClick={() => {
            setRan(true);
            persist({ ran: true });
            onExecute?.(stripGenieTools(data));
          }}
          disabled={ran}
          sx={{
            ...buttonResetSx,
            width: '100%',
            borderRadius: '8px',
            px: 1.5,
            py: 0.75,
            fontSize: 12,
            fontWeight: 500,
            transition: 'all 0.15s',
            color: 'text.disabled',
            '&:hover': { opacity: 0.8 },
            '&:disabled': { opacity: 0.5, cursor: 'not-allowed' },
          }}
        >
          Skip — run without Genie
        </Box>
      </Box>
    </Box>
  );
};

export default GenieSpacePrompt;
