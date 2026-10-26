import React, { useMemo, useState } from 'react';
import { useExecutionStore } from '../../store/executionStore';
import { assessChatModeTrifecta } from '../../../../utils/toolCapabilityManifest';

/**
 * Inline, non-blocking "lethal trifecta" notice for ChatMode.
 *
 * The trifecta (sensitive internal read + untrusted-content ingestion + external
 * communication) is what lets an indirect prompt injection exfiltrate your data.
 * We can enumerate the finite set of sensitive Databricks sources (Genie,
 * Databricks SQL, Unity Catalog Functions, AI Search, Agent Bricks); we CANNOT
 * enumerate every internet-capable tool, since any MCP endpoint could reach the
 * web — so an unrecognised server is assumed untrusted+external. The notice fires
 * when a recognised sensitive source is combined with such a channel (and Agent
 * Bricks, an opaque agent that may browse the web, trips it on its own).
 *
 * Purely informational: it never blocks Send. It reacts to the chat picker
 * selections (MCP servers + Agent Bricks endpoints) and is dismissible until the
 * selection changes.
 */
const AMBER = '#d97706';

const TrifectaNotice: React.FC = () => {
  const mcpServers = useExecutionStore((s) => s.selectedMcpServers);
  const agentBricksEndpoints = useExecutionStore((s) => s.selectedAgentBricksEndpoints) ?? [];
  const [dismissedSig, setDismissedSig] = useState<string | null>(null);

  const assessment = useMemo(
    () => assessChatModeTrifecta({ mcpServers, agentBricksEndpoints }),
    [mcpServers, agentBricksEndpoints],
  );

  // Re-show the notice whenever the offending selection changes (a new sensitive
  // source or egress channel was added), even if a prior state was dismissed.
  const signature = useMemo(
    () =>
      [...assessment.sensitiveTools, ...assessment.untrustedTools, ...assessment.externalTools]
        .sort()
        .join('|'),
    [assessment],
  );

  if (!assessment.hasTrifecta || dismissedSig === signature) return null;

  const sensitive = assessment.sensitiveTools;
  const channels = [...new Set([...assessment.untrustedTools, ...assessment.externalTools])].filter(
    (t) => !sensitive.includes(t),
  );

  return (
    <div
      role="status"
      data-testid="trifecta-notice"
      className="mb-2 rounded-xl px-3 py-2 flex items-start gap-2 text-[11px] leading-snug animate-slide-up"
      style={{
        color: AMBER,
        backgroundColor: `color-mix(in srgb, ${AMBER} 12%, transparent)`,
        border: `1px solid color-mix(in srgb, ${AMBER} 40%, transparent)`,
      }}
    >
      <svg className="w-4 h-4 mt-0.5 flex-shrink-0" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
        <path
          strokeLinecap="round"
          strokeLinejoin="round"
          d="M12 9v3.75m9-.75a9 9 0 11-18 0 9 9 0 0118 0zm-9 3.75h.008v.008H12v-.008z"
        />
      </svg>
      <div className="flex-1 min-w-0">
        <span style={{ fontWeight: 600 }}>Security check: possible data-exfiltration risk. </span>
        This run combines an internal data source ({sensitive.join(', ')})
        {channels.length > 0 ? <> with tools that can reach the internet or untrusted content ({channels.join(', ')})</> : null}.
        A prompt injection could use the external channel to leak the internal data. You can still send;
        this is just a heads-up.
      </div>
      <button
        type="button"
        onClick={() => setDismissedSig(signature)}
        aria-label="Dismiss security notice"
        className="flex-shrink-0 -mr-1 -mt-0.5 p-0.5 rounded hover:opacity-70 transition-opacity"
        style={{ color: AMBER }}
      >
        <svg className="w-3.5 h-3.5" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2.5}>
          <path strokeLinecap="round" strokeLinejoin="round" d="M6 18L18 6M6 6l12 12" />
        </svg>
      </button>
    </div>
  );
};

export default TrifectaNotice;
