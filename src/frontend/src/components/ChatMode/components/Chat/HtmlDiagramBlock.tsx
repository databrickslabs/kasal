import React, { useCallback, useMemo, useState } from 'react';
import { Check, ClipboardCopy, Code2, Eye, Maximize2 } from 'lucide-react';
import { buildMdSandboxCell, notebookSafe } from '../../utils/mdSandboxDiagram';
import ScaledFrame from './ScaledFrame';
import { useThrottledPreview } from '../../utils/scaledFrame';
import { useResolvedAssetHtml } from '../../hooks/useResolvedAssetHtml';
import FullscreenModal from './FullscreenModal';

/**
 * Renders an agent-authored, self-contained HTML/SVG diagram inside a sandboxed,
 * scale-to-fit iframe (see utils/scaledFrame), with a toolbar to toggle the
 * source and copy it as a Databricks `%md-sandbox` cell. During streaming the
 * `code` prop grows per token; we throttle the (expensive) iframe re-mount while
 * keeping the code view current.
 */

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
  // re-mount the iframe on every token (shared with the slide deck).
  const throttled = useThrottledPreview(safeCode, streaming);
  // `asset:<id>` references (attached images) become data URLs for the frame.
  const preview = useResolvedAssetHtml(throttled);

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
        <ScaledFrame html={preview} streaming={streaming} title="Rendered diagram" />
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
