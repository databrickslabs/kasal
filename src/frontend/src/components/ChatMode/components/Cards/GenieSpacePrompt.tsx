import React, { useState } from 'react';
import GenieSpaceSelector from './GenieSpaceSelector';
import { GenerationCompleteData } from '../../hooks/useGenerationStream';
import { useSessionStore } from '../../store/sessionStore';

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
    <div className="px-4 my-2 max-w-3xl">
      <div className="pt-1 space-y-2">
        <p className="text-xs px-1" style={{ color: 'var(--text-muted)' }}>
          This crew queries a Genie space — pick one to run.
        </p>
        <GenieSpaceSelector
          value={selectedSpaceId}
          onChange={(v) => {
            setSelectedSpaceId(v);
            persist({ spaceId: v });
          }}
        />
        <button
          type="button"
          onClick={() => {
            setRan(true);
            persist({ ran: true });
            onExecute?.(data, selectedSpaceId);
          }}
          disabled={!selectedSpaceId || ran}
          className="w-full flex items-center justify-center gap-2 rounded-lg px-3 py-2 text-sm font-medium transition-all hover:opacity-80 disabled:opacity-50 disabled:cursor-not-allowed"
          style={{ backgroundColor: 'var(--bg-secondary)', color: 'var(--text-secondary)', border: '1px solid var(--border-color)' }}
        >
          <svg className="w-4 h-4" fill="currentColor" viewBox="0 0 24 24">
            <path d="M8 5v14l11-7z" />
          </svg>
          {ran ? 'Running…' : selectedSpaceId ? 'Run crew' : 'Select a Genie space to run'}
        </button>
      </div>
    </div>
  );
};

export default GenieSpacePrompt;
