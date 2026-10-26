/**
 * A step's code payload — the SQL a tool ran, the JSON it was called with —
 * shown as a syntax-highlighted code block.
 *
 * It used to be flattened into the prose surface, one wrapped line per source
 * line, so a CREATE TABLE arrived as sixteen paragraphs with the JSON envelope
 * still around it. Code wants a frame, a gutter, highlighting and its own
 * scrolling; prose formatting actively destroys all four.
 */
import React, { useMemo, useState } from 'react';
import { Highlight, themes } from 'prism-react-renderer';
import { Check, Copy } from 'lucide-react';
import { useTheme } from '../../../../hooks/global/useTheme';

/** Prism's language ids, from the ones the trace hands us. */
const PRISM_LANGUAGE: Record<string, string> = {
  sql: 'sql',
  json: 'json',
  python: 'python',
  bash: 'bash',
  text: 'markup',
};

/** A 400-row result set is not something to scroll past to reach the rest of
 *  the pane, so a long payload opens collapsed. */
const MAX_LINES = 60;

export interface CodeSnippetProps {
  text: string;
  /** Shown as the block's caption ("sql", "json", …) when known. */
  language?: string;
  /** What produced it, e.g. the tool name. */
  title?: string;
}

const CodeSnippet: React.FC<CodeSnippetProps> = ({ text, language = 'text', title }) => {
  const { isDarkMode } = useTheme();
  const [copied, setCopied] = useState(false);
  const [expanded, setExpanded] = useState(false);

  const lines = useMemo(() => text.split('\n'), [text]);
  const truncated = !expanded && lines.length > MAX_LINES;
  const shown = truncated ? lines.slice(0, MAX_LINES).join('\n') : text;

  const copy = async () => {
    try {
      await navigator.clipboard.writeText(text);
      setCopied(true);
      window.setTimeout(() => setCopied(false), 1500);
    } catch {
      // Clipboard permission denied — the text is selectable either way.
    }
  };

  const theme = isDarkMode ? themes.oneDark : themes.oneLight;

  return (
    <div className="my-3 rounded-xl overflow-hidden" style={{ border: '1px solid var(--border-color)' }}>
      <div
        className="flex items-center gap-2 px-3 py-1.5 text-[11px]"
        style={{
          borderBottom: '1px solid var(--border-color)',
          color: 'var(--text-secondary)',
          backgroundColor: 'var(--bg-secondary)',
        }}
      >
        <span className="font-mono uppercase tracking-wide">{language}</span>
        {title && <span className="truncate">· {title}</span>}
        <button
          type="button"
          onClick={copy}
          className="ml-auto flex items-center gap-1 transition-colors hover:opacity-80"
          aria-label="Copy code"
          title="Copy"
        >
          {copied ? <Check size={12} /> : <Copy size={12} />}
          {copied ? 'Copied' : 'Copy'}
        </button>
      </div>

      <Highlight theme={theme} code={shown} language={PRISM_LANGUAGE[language] || 'markup'}>
        {({ style, tokens, getLineProps, getTokenProps }) => (
          // Every box property is set inline. The chat's global `pre` rule gives
          // every <pre> its own radius, border, padding and dark background —
          // which drew a second frame inside this one.
          <pre
            className="text-xs overflow-x-auto"
            style={{
              ...style,
              margin: 0,
              border: 'none',
              borderRadius: 0,
              padding: '12px 0',
              maxHeight: '60vh',
              overflowY: 'auto',
              lineHeight: 1.6,
            }}
          >
            {tokens.map((line, i) => (
              <div key={i} {...getLineProps({ line })} style={{ display: 'flex' }}>
                {/* Non-selectable gutter, so copying a range out of the block
                    does not take the line numbers with it. */}
                <span
                  className="select-none text-right flex-shrink-0"
                  style={{ width: '3ch', marginLeft: 12, marginRight: 12, opacity: 0.35 }}
                >
                  {i + 1}
                </span>
                <span className="flex-1 min-w-0 pr-3 whitespace-pre-wrap break-words">
                  {line.map((token, key) => (
                    <span key={key} {...getTokenProps({ token })} />
                  ))}
                </span>
              </div>
            ))}
          </pre>
        )}
      </Highlight>

      {truncated && (
        <button
          type="button"
          onClick={() => setExpanded(true)}
          className="w-full px-3 py-1.5 text-[11px] transition-colors hover:opacity-80"
          style={{
            borderTop: '1px solid var(--border-color)',
            color: 'var(--text-secondary)',
            backgroundColor: 'var(--bg-secondary)',
          }}
        >
          Show all {lines.length} lines
        </button>
      )}
    </div>
  );
};

export default CodeSnippet;
