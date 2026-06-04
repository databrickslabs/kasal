import React, { useRef, useEffect, useMemo, useState } from 'react';
import ReactMarkdown from 'react-markdown';
import remarkGfm from 'remark-gfm';
import { parseUiDocument } from '../../utils/uiDocument';
import UiRenderer from './UiRenderer';

export type PreviewContentType = 'html' | 'json' | 'markdown' | 'text' | 'ui';

export interface PreviewContent {
  type: PreviewContentType;
  data: string;
  title?: string;
}

interface PreviewPanelProps {
  content: PreviewContent;
  onClose: () => void;
  chatCollapsed: boolean;
  onToggleChat: () => void;
  /** Refine the current artifact with a natural-language instruction. */
  onRefine?: (instruction: string) => void;
  /** All previewable task outputs of the run, oldest → newest. */
  history?: PreviewContent[];
  /** Index into `history` currently shown. */
  index?: number;
  /** Switch the displayed preview to another history entry. */
  onNavigate?: (index: number) => void;
}

function detectContentType(raw: string): PreviewContentType {
  const trimmed = raw.trim();

  // Check for HTML
  if (
    trimmed.startsWith('<!DOCTYPE') ||
    trimmed.startsWith('<!doctype') ||
    trimmed.startsWith('<html') ||
    trimmed.startsWith('<HTML') ||
    /<html[\s>]/i.test(trimmed) ||
    (trimmed.includes('<script') && trimmed.includes('</script>')) ||
    (trimmed.startsWith('<') && trimmed.includes('</') && /<\w+[\s>]/.test(trimmed))
  ) {
    return 'html';
  }

  // Check for JSON
  if (
    (trimmed.startsWith('{') && trimmed.endsWith('}')) ||
    (trimmed.startsWith('[') && trimmed.endsWith(']'))
  ) {
    try {
      JSON.parse(trimmed);
      return 'json';
    } catch {
      // not valid JSON
    }
  }

  // Check for structured markdown (headers + significant length)
  const hasHeaders = /^#{1,3}\s/m.test(trimmed);
  const hasStructure =
    (trimmed.includes('```') ? 1 : 0) +
    (/^\|.+\|$/m.test(trimmed) ? 1 : 0) +
    (/^\s*[-*]\s/m.test(trimmed) ? 1 : 0);
  if (hasHeaders && (trimmed.length > 500 || hasStructure >= 1)) {
    return 'markdown';
  }

  return 'text';
}

/**
 * Strip a leading `**Task name**\n\n` prefix the chat layer prepends to
 * task-output messages. The prefix is for chat display only; the preview
 * should render just the body.
 */
function stripTaskTitlePrefix(raw: string): string {
  const match = raw.match(/^\s*\*\*[^\n*][^\n]*?\*\*\s*\n\n+/);
  return match ? raw.slice(match[0].length) : raw;
}

/**
 * Strip markdown code fences if present.
 * Handles: ```html\n...\n``` or ```json\n...\n``` etc.
 */
function stripCodeFences(raw: string): string {
  const trimmed = raw.trim();
  const fenceMatch = trimmed.match(/^```\w*\s*\n([\s\S]*?)\n\s*```\s*$/);
  if (fenceMatch) {
    return fenceMatch[1];
  }
  // Also handle when there's text before/after the code fence
  const innerMatch = trimmed.match(/```(?:html|json|xml)\s*\n([\s\S]*?)\n\s*```/);
  if (innerMatch) {
    return innerMatch[1];
  }
  return raw;
}

/**
 * Extract the largest embedded HTML document from mixed text content.
 * Handles cases where task output contains text + JSON + one or more full HTML
 * documents all concatenated together.
 */
function extractEmbeddedHtml(raw: string): string | null {
  // Find all complete HTML documents: <!DOCTYPE html>...  </html>
  const htmlDocRegex = /<!DOCTYPE\s+html[\s\S]*?<\/html>/gi;
  const matches = raw.match(htmlDocRegex);
  if (!matches || matches.length === 0) {
    // Also try <html>...</html> without DOCTYPE
    const htmlTagRegex = /<html[\s>][\s\S]*?<\/html>/gi;
    const tagMatches = raw.match(htmlTagRegex);
    if (!tagMatches || tagMatches.length === 0) return null;
    // Pick the largest match (most complete document)
    return tagMatches.reduce((a, b) => (a.length >= b.length ? a : b));
  }
  // Pick the largest match (most complete document)
  return matches.reduce((a, b) => (a.length >= b.length ? a : b));
}

export function parsePreviewContent(raw: string): PreviewContent | null {
  if (!raw || raw.length < 10) return null;

  // Drop the chat layer's bold-title prefix so the preview shows only the body.
  const body = stripTaskTitlePrefix(raw);

  // Strip markdown code fences that often wrap HTML/JSON output
  const cleaned = stripCodeFences(body);

  // A2UI documents are JSON too, so check before generic JSON detection: an
  // A2UI doc declares a surface / catalog components and renders via our
  // brand-consistent renderer instead of a raw JSON table.
  if (parseUiDocument(cleaned)) {
    return { type: 'ui', data: cleaned };
  }

  const type = detectContentType(cleaned);
  if (type !== 'text') {
    return { type, data: cleaned };
  }

  // The cleaned content looks like plain text — but it may contain embedded
  // HTML documents mixed with other text (common in crew task output that
  // includes descriptions, JSON data, AND HTML dashboards).
  const embeddedHtml = extractEmbeddedHtml(body);
  if (embeddedHtml && embeddedHtml.length > 100) {
    return { type: 'html', data: embeddedHtml };
  }

  return null; // Genuinely plain text — don't preview
}

function JsonTable({ data }: { data: unknown }) {
  if (Array.isArray(data) && data.length > 0 && typeof data[0] === 'object') {
    // Array of objects → table
    const allKeys = new Set<string>();
    data.forEach((row) => {
      if (row && typeof row === 'object') {
        Object.keys(row as Record<string, unknown>).forEach((k) => allKeys.add(k));
      }
    });
    const keys = Array.from(allKeys);

    return (
      <div className="overflow-auto">
        <table className="w-full text-sm" style={{ borderCollapse: 'collapse' }}>
          <thead>
            <tr>
              {keys.map((key) => (
                <th
                  key={key}
                  className="text-left px-3 py-2 text-xs font-semibold uppercase tracking-wider"
                  style={{
                    color: 'var(--text-muted)',
                    borderBottom: '2px solid var(--border-color)',
                    backgroundColor: 'var(--bg-secondary)',
                    whiteSpace: 'nowrap',
                  }}
                >
                  {key}
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {data.map((row, i) => (
              <tr key={i}>
                {keys.map((key) => {
                  const val = (row as Record<string, unknown>)?.[key];
                  const display =
                    val === null || val === undefined
                      ? ''
                      : typeof val === 'object'
                        ? JSON.stringify(val)
                        : String(val);
                  return (
                    <td
                      key={key}
                      className="px-3 py-2"
                      style={{
                        color: 'var(--text-primary)',
                        borderBottom: '1px solid var(--border-color)',
                        maxWidth: '300px',
                        overflow: 'hidden',
                        textOverflow: 'ellipsis',
                      }}
                    >
                      {display}
                    </td>
                  );
                })}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    );
  }

  if (typeof data === 'object' && data !== null && !Array.isArray(data)) {
    // Single object → key-value table
    const entries = Object.entries(data as Record<string, unknown>);
    return (
      <div className="overflow-auto">
        <table className="w-full text-sm" style={{ borderCollapse: 'collapse' }}>
          <thead>
            <tr>
              <th
                className="text-left px-3 py-2 text-xs font-semibold uppercase tracking-wider"
                style={{
                  color: 'var(--text-muted)',
                  borderBottom: '2px solid var(--border-color)',
                  backgroundColor: 'var(--bg-secondary)',
                }}
              >
                Key
              </th>
              <th
                className="text-left px-3 py-2 text-xs font-semibold uppercase tracking-wider"
                style={{
                  color: 'var(--text-muted)',
                  borderBottom: '2px solid var(--border-color)',
                  backgroundColor: 'var(--bg-secondary)',
                }}
              >
                Value
              </th>
            </tr>
          </thead>
          <tbody>
            {entries.map(([key, val]) => {
              const display =
                val === null || val === undefined
                  ? ''
                  : typeof val === 'object'
                    ? JSON.stringify(val, null, 2)
                    : String(val);
              return (
                <tr key={key}>
                  <td
                    className="px-3 py-2 font-medium"
                    style={{
                      color: 'var(--text-secondary)',
                      borderBottom: '1px solid var(--border-color)',
                      whiteSpace: 'nowrap',
                    }}
                  >
                    {key}
                  </td>
                  <td
                    className="px-3 py-2"
                    style={{
                      color: 'var(--text-primary)',
                      borderBottom: '1px solid var(--border-color)',
                      whiteSpace: 'pre-wrap',
                      wordBreak: 'break-word',
                    }}
                  >
                    {display}
                  </td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>
    );
  }

  // Fallback: array of primitives or other
  return (
    <pre
      className="text-sm p-4 overflow-auto"
      style={{ color: 'var(--text-primary)' }}
    >
      {JSON.stringify(data, null, 2)}
    </pre>
  );
}

const PreviewPanel: React.FC<PreviewPanelProps> = ({ content, onClose, chatCollapsed, onToggleChat, onRefine, history, index, onNavigate }) => {
  const iframeRef = useRef<HTMLIFrameElement>(null);
  const [refineOpen, setRefineOpen] = useState(false);
  const [refineValue, setRefineValue] = useState('');

  const submitRefine = () => {
    const trimmed = refineValue.trim();
    if (!trimmed) return;
    // Only reachable from the refine bar, which renders only when onRefine is set.
    onRefine!(trimmed);
    setRefineValue('');
    setRefineOpen(false);
  };

  // Heal already-stored previews that include the chat layer's bold-title prefix.
  const displayData = useMemo(() => stripTaskTitlePrefix(content.data), [content]);

  // For HTML: write content into a sandboxed iframe
  useEffect(() => {
    if (content.type === 'html' && iframeRef.current) {
      const doc = iframeRef.current.contentDocument;
      if (doc) {
        doc.open();
        doc.write(displayData);
        doc.close();
      }
    }
  }, [content, displayData]);

  const jsonData = useMemo(() => {
    if (content.type === 'json') {
      try {
        return JSON.parse(displayData);
      } catch {
        return null;
      }
    }
    return null;
  }, [content, displayData]);

  // Parse the A2UI document for the brand-consistent renderer.
  const uiSurface = useMemo(
    () => (content.type === 'ui' ? parseUiDocument(displayData) : null),
    [content, displayData],
  );

  return (
    <aside
      className="flex flex-col h-full"
      style={{
        flex: chatCollapsed ? '1 1 100%' : '1 1 50%',
        minWidth: '300px',
        backgroundColor: 'var(--bg-primary)',
        borderLeft: chatCollapsed ? 'none' : '1px solid var(--border-color)',
      }}
    >
      {/* Header */}
      <div
        className="flex items-center justify-between px-4 py-3 flex-shrink-0"
        style={{ borderBottom: '1px solid var(--border-color)' }}
      >
        <div className="flex items-center gap-2">
          {/* Toggle chat button */}
          <button
            onClick={onToggleChat}
            className="w-7 h-7 rounded-lg flex items-center justify-center transition-colors hover:opacity-70"
            style={{ color: 'var(--text-muted)' }}
            title={chatCollapsed ? 'Show chat' : 'Hide chat'}
          >
            <svg className="w-4 h-4" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
              {chatCollapsed ? (
                <path strokeLinecap="round" strokeLinejoin="round" d="M18.75 19.5l-7.5-7.5 7.5-7.5m-6 15L5.25 12l7.5-7.5" />
              ) : (
                <path strokeLinecap="round" strokeLinejoin="round" d="M11.25 4.5l7.5 7.5-7.5 7.5m-6-15l7.5 7.5-7.5 7.5" />
              )}
            </svg>
          </button>
          <svg
            className="w-4 h-4"
            style={{ color: 'var(--accent)' }}
            fill="none"
            viewBox="0 0 24 24"
            stroke="currentColor"
            strokeWidth={2}
          >
            {content.type === 'html' ? (
              <path
                strokeLinecap="round"
                strokeLinejoin="round"
                d="M17.25 6.75L22.5 12l-5.25 5.25m-10.5 0L1.5 12l5.25-5.25m7.5-3l-4.5 16.5"
              />
            ) : content.type === 'markdown' ? (
              <path
                strokeLinecap="round"
                strokeLinejoin="round"
                d="M19.5 14.25v-2.625a3.375 3.375 0 00-3.375-3.375h-1.5A1.125 1.125 0 0113.5 7.125v-1.5a3.375 3.375 0 00-3.375-3.375H8.25m0 12.75h7.5m-7.5 3H12M10.5 2.25H5.625c-.621 0-1.125.504-1.125 1.125v17.25c0 .621.504 1.125 1.125 1.125h12.75c.621 0 1.125-.504 1.125-1.125V11.25a9 9 0 00-9-9z"
              />
            ) : (
              <path
                strokeLinecap="round"
                strokeLinejoin="round"
                d="M3.375 19.5h17.25m-17.25 0a1.125 1.125 0 01-1.125-1.125M3.375 19.5h7.5c.621 0 1.125-.504 1.125-1.125m-9.75 0V5.625m0 12.75v-1.5c0-.621.504-1.125 1.125-1.125m18.375 2.625V5.625m0 12.75c0 .621-.504 1.125-1.125 1.125m1.125-1.125v-1.5c0-.621-.504-1.125-1.125-1.125m0 3.75h-7.5A1.125 1.125 0 0112 18.375m9.75-12.75c0-.621-.504-1.125-1.125-1.125H3.375c-.621 0-1.125.504-1.125 1.125m19.5 0v1.5c0 .621-.504 1.125-1.125 1.125M2.25 5.625v1.5c0 .621.504 1.125 1.125 1.125m0 0h17.25m-17.25 0h7.5c.621 0 1.125.504 1.125 1.125M3.375 8.25c-.621 0-1.125.504-1.125 1.125v1.5c0 .621.504 1.125 1.125 1.125m17.25-3.75h-7.5c-.621 0-1.125.504-1.125 1.125m8.625-1.125c.621 0 1.125.504 1.125 1.125v1.5c0 .621-.504 1.125-1.125 1.125m-17.25 0h7.5m-7.5 0c-.621 0-1.125.504-1.125 1.125v1.5c0 .621.504 1.125 1.125 1.125M12 10.875v-1.5m0 1.5c0 .621-.504 1.125-1.125 1.125M12 10.875c0 .621.504 1.125 1.125 1.125m-2.25 0c.621 0 1.125.504 1.125 1.125M13.125 12h7.5m-7.5 0c-.621 0-1.125.504-1.125 1.125M20.625 12c.621 0 1.125.504 1.125 1.125v1.5c0 .621-.504 1.125-1.125 1.125m-17.25 0h7.5M12 14.625v-1.5m0 1.5c0 .621-.504 1.125-1.125 1.125M12 14.625c0 .621.504 1.125 1.125 1.125m-2.25 0c.621 0 1.125.504 1.125 1.125m0 0v1.5c0 .621-.504 1.125-1.125 1.125"
              />
            )}
          </svg>
          <span
            className="text-sm font-medium"
            style={{ color: 'var(--text-primary)' }}
          >
            {content.title || (content.type === 'html' ? 'Preview' : content.type === 'markdown' ? 'Report' : content.type === 'ui' ? 'App' : 'Result')}
          </span>
          <span
            className="text-[10px] px-1.5 py-0.5 rounded font-mono"
            style={{
              color: 'var(--text-muted)',
              backgroundColor: 'var(--bg-secondary)',
            }}
          >
            {content.type.toUpperCase()}
          </span>
        </div>
        <div className="flex items-center gap-1">
          {history && history.length > 1 && onNavigate && (() => {
            const current = index ?? history.length - 1;
            return (
              <div
                className="flex items-center gap-0.5 mr-1 rounded-lg"
                style={{ backgroundColor: 'var(--bg-secondary)' }}
              >
                <button
                  onClick={() => onNavigate(current - 1)}
                  disabled={current <= 0}
                  className="w-7 h-7 rounded-lg flex items-center justify-center transition-colors hover:opacity-70 disabled:opacity-30 disabled:cursor-not-allowed"
                  style={{ color: 'var(--text-muted)' }}
                  title="Previous output"
                  aria-label="Previous output"
                >
                  <svg className="w-4 h-4" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
                    <path strokeLinecap="round" strokeLinejoin="round" d="M15.75 19.5L8.25 12l7.5-7.5" />
                  </svg>
                </button>
                <span
                  className="text-[11px] font-mono tabular-nums px-1 select-none"
                  style={{ color: 'var(--text-muted)' }}
                  title="Task output (latest shown first)"
                >
                  {current + 1}/{history.length}
                </span>
                <button
                  onClick={() => onNavigate(current + 1)}
                  disabled={current >= history.length - 1}
                  className="w-7 h-7 rounded-lg flex items-center justify-center transition-colors hover:opacity-70 disabled:opacity-30 disabled:cursor-not-allowed"
                  style={{ color: 'var(--text-muted)' }}
                  title="Next output"
                  aria-label="Next output"
                >
                  <svg className="w-4 h-4" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
                    <path strokeLinecap="round" strokeLinejoin="round" d="M8.25 4.5l7.5 7.5-7.5 7.5" />
                  </svg>
                </button>
              </div>
            );
          })()}
          {onRefine && (
            <button
              onClick={() => setRefineOpen((v) => !v)}
              className="flex items-center gap-1.5 h-7 px-2.5 rounded-lg text-xs font-medium transition-colors hover:opacity-80"
              style={{
                color: refineOpen ? 'var(--accent)' : 'var(--text-secondary)',
                backgroundColor: 'var(--bg-secondary)',
              }}
              title="Refine this result with an instruction"
            >
              <svg className="w-3.5 h-3.5" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={1.8}>
                <path strokeLinecap="round" strokeLinejoin="round" d="M9.813 15.904L9 18.75l-.813-2.846a4.5 4.5 0 00-3.09-3.09L2.25 12l2.846-.813a4.5 4.5 0 003.09-3.09L9 5.25l.813 2.846a4.5 4.5 0 003.09 3.09L15.75 12l-2.846.813a4.5 4.5 0 00-3.09 3.09z" />
              </svg>
              Refine
            </button>
          )}
          <button
            onClick={onClose}
            className="w-7 h-7 rounded-lg flex items-center justify-center transition-colors hover:opacity-70"
            style={{ color: 'var(--text-muted)' }}
            title="Close preview"
          >
            <svg
              className="w-4 h-4"
              fill="none"
              viewBox="0 0 24 24"
              stroke="currentColor"
              strokeWidth={2}
            >
              <path
                strokeLinecap="round"
                strokeLinejoin="round"
                d="M6 18L18 6M6 6l12 12"
              />
            </svg>
          </button>
        </div>
      </div>

      {/* Refine instruction bar */}
      {onRefine && refineOpen && (
        <div
          className="flex items-center gap-2 px-4 py-2.5 flex-shrink-0"
          style={{ borderBottom: '1px solid var(--border-color)', backgroundColor: 'var(--bg-secondary)' }}
        >
          <input
            autoFocus
            value={refineValue}
            onChange={(e) => setRefineValue(e.target.value)}
            onKeyDown={(e) => {
              if (e.key === 'Enter') submitRefine();
              if (e.key === 'Escape') { setRefineOpen(false); setRefineValue(''); }
            }}
            placeholder="Describe how to improve this result…"
            className="flex-1 rounded-lg px-3 py-1.5 text-sm outline-none"
            style={{
              backgroundColor: 'var(--bg-input)',
              color: 'var(--text-primary)',
              border: '1px solid var(--border-color)',
            }}
          />
          <button
            onClick={submitRefine}
            disabled={!refineValue.trim()}
            className="px-3 py-1.5 rounded-lg text-sm font-medium text-white transition-all disabled:opacity-50 disabled:cursor-not-allowed"
            style={{ backgroundColor: 'var(--accent)' }}
          >
            Refine
          </button>
        </div>
      )}

      {/* Content */}
      <div className="flex-1 overflow-auto">
        {content.type === 'html' && (
          <iframe
            ref={iframeRef}
            className="w-full h-full border-0"
            sandbox="allow-scripts allow-same-origin"
            title="Execution result preview"
            style={{ backgroundColor: '#ffffff' }}
          />
        )}

        {content.type === 'json' && jsonData !== null && (
          <div className="p-4">
            <JsonTable data={jsonData} />
          </div>
        )}

        {content.type === 'markdown' && (
          <div
            className="prose max-w-none p-6"
            style={{ color: 'var(--text-primary)' }}
          >
            <ReactMarkdown remarkPlugins={[remarkGfm]}>
              {displayData}
            </ReactMarkdown>
          </div>
        )}

        {content.type === 'ui' && uiSurface && (
          <UiRenderer surface={uiSurface} />
        )}
      </div>
    </aside>
  );
};

export default PreviewPanel;
