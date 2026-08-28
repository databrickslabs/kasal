import React, { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { Check, ClipboardCopy, Code2, Eye, Maximize2 } from 'lucide-react';
import { buildMdSandboxCell, notebookSafe } from '../../utils/mdSandboxDiagram';
import ScaledFrame from './ScaledFrame';
import FullscreenModal from './FullscreenModal';

/**
 * Renders an agent-authored, self-contained HTML/SVG diagram inside a sandboxed,
 * scale-to-fit iframe (see utils/scaledFrame), with a toolbar to toggle the
 * source and copy it as a Databricks `%md-sandbox` cell. During streaming the
 * `code` prop grows per token; we throttle the (expensive) iframe re-mount while
 * keeping the code view current.
 */

// Re-mount the live preview at most this often while a diagram streams in.
const THROTTLE_MS = 400;

interface HtmlDiagramBlockProps {
  /** The raw HTML/SVG body of the diagram block (without the ``` fences). */
  code: string;
  /** True while the block is still being written (unclosed fence). */
  streaming?: boolean;
}

const HtmlDiagramBlock: React.FC<HtmlDiagramBlockProps> = ({ code, streaming = false }) => {
  const [showCode, setShowCode] = useState(false);
  const [copied, setCopied] = useState(false);
  const [full, setFull] = useState(false);

  const safeCode = useMemo(() => notebookSafe(code), [code]);

  // Throttle the source we actually mount so a streaming diagram doesn't
  // re-mount the iframe on every token. When not streaming, mount immediately.
  const [preview, setPreview] = useState(safeCode);
  const lastMountRef = useRef(0);
  const timerRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  useEffect(() => {
    const flush = () => {
      lastMountRef.current = Date.now();
      setPreview(safeCode);
    };
    if (!streaming) {
      flush();
      return;
    }
    const since = Date.now() - lastMountRef.current;
    if (since >= THROTTLE_MS) {
      flush();
      return;
    }
    if (timerRef.current) clearTimeout(timerRef.current);
    timerRef.current = setTimeout(flush, THROTTLE_MS - since);
    return () => {
      if (timerRef.current) clearTimeout(timerRef.current);
    };
  }, [safeCode, streaming]);

  const copyCell = useCallback(async () => {
    const cell = buildMdSandboxCell(code);
    try {
      await navigator.clipboard.writeText(cell);
    } catch {
      const ta = document.createElement('textarea');
      ta.value = cell;
      document.body.appendChild(ta);
      ta.select();
      document.execCommand('copy');
      document.body.removeChild(ta);
    }
    setCopied(true);
    setTimeout(() => setCopied(false), 1800);
  }, [code]);

  const btnClass =
    'inline-flex items-center gap-1 rounded px-1.5 py-0.5 transition-colors hover:bg-black/5 dark:hover:bg-white/10';

  return (
    <div
      className="my-2 overflow-hidden rounded-lg border"
      style={{ borderColor: 'var(--border-color, rgba(0,0,0,0.12))' }}
    >
      <div
        className="flex items-center justify-between gap-2 px-3 py-1.5 text-xs"
        style={{ background: 'var(--bg-secondary, #f5f5f5)', color: 'var(--text-muted, rgba(0,0,0,0.6))' }}
      >
        <span className="font-medium">{streaming ? 'Building diagram…' : ''}</span>
        <div className="flex items-center gap-1">
          <button type="button" onClick={() => setShowCode((s) => !s)} className={btnClass} title={showCode ? 'Show the rendered diagram' : 'Show the diagram source'}>
            {showCode ? <Eye size={13} /> : <Code2 size={13} />}
          </button>
          <button type="button" onClick={() => setFull(true)} className={btnClass} title="View fullscreen">
            <Maximize2 size={13} />
          </button>
          <button type="button" onClick={copyCell} className={btnClass} title="Copy as a Databricks %md-sandbox notebook cell">
            {copied ? <Check size={13} /> : <ClipboardCopy size={13} />}
          </button>
        </div>
      </div>
      {showCode ? (
        <pre
          className="m-0 max-h-[360px] overflow-auto p-3 text-[13px] leading-[1.5]"
          style={{ background: 'var(--bg-input, #fff)', color: 'var(--text-primary, #111)' }}
        >
          <code>{`%md-sandbox\n${safeCode}`}</code>
        </pre>
      ) : (
        <ScaledFrame html={preview} title="Rendered diagram" />
      )}
      {full && (
        <FullscreenModal onClose={() => setFull(false)}>
          <ScaledFrame html={safeCode} contain title="Rendered diagram (fullscreen)" />
        </FullscreenModal>
      )}
    </div>
  );
};

export default HtmlDiagramBlock;
