import React, { useEffect, useMemo, useRef, useState } from 'react';
import { ChevronLeft, ChevronRight, Download, Loader2, Maximize2, SquarePen } from 'lucide-react';
import { SLIDE_W, refinedSlideIndex, splitSlides, stageFor } from '../../utils/htmlDeck';
import DeckStudio from '../Deck/DeckStudio';
import { useThrottledPreview } from '../../utils/scaledFrame';
import { downloadDeckPdf, downloadDeckPptx } from '../../utils/deckExport';
import ScaledFrame from './ScaledFrame';
import DeckPresentation from './DeckPresentation';

/**
 * Renders an agent-authored HTML slide deck: a ```html block whose slides are
 * `<section class="slide">…</section>`. One slide at a time is shown on a fixed
 * 1280×720 stage, scaled to fit (see utils/scaledFrame), with prev/next
 * navigation, a fullscreen view, and PDF / PowerPoint download. While the deck
 * streams in, we follow the newest slide so the user watches it build.
 */

interface HtmlDeckBlockProps {
  /** The raw HTML of the whole deck (without the ``` fences). */
  code: string;
  /** True while the deck is still being written (unclosed fence). */
  streaming?: boolean;
  /** True when the message ENDED without closing the fence (output cut off):
   *  the deck finalizes (paging/download work) but is labelled incomplete. */
  truncated?: boolean;
  /** The chat message the deck lives in — the studio writes its edits back there. */
  messageId?: string;
}

const HtmlDeckBlock: React.FC<HtmlDeckBlockProps> = ({
  code,
  streaming = false,
  truncated = false,
  messageId,
}) => {
  // While the deck streams in, rebuild the (expensive) iframe at most every
  // 400ms instead of per token — same throttle the diagram card uses. The
  // stream's end flushes immediately.
  const liveCode = useThrottledPreview(code, streaming);
  const slides = useMemo(() => splitSlides(liveCode), [liveCode]);
  const count = slides.length;
  // A deck a slide edit just changed opens on THAT slide (it carries the
  // refined marker), so the reader lands on the change rather than the cover.
  const [idx, setIdx] = useState(() => Math.max(0, refinedSlideIndex(code)));
  const [full, setFull] = useState(false);
  const [studio, setStudio] = useState(false);
  const [menu, setMenu] = useState(false);
  const [busy, setBusy] = useState<'' | 'pdf' | 'pptx'>('');
  const menuRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    if (streaming && count > 0) setIdx(count - 1);
    else setIdx((i) => Math.min(i, Math.max(0, count - 1)));
  }, [streaming, count]);
  // The refined deck folds into the message the run streamed on, so the same
  // block instance sees the new code: jump to the changed slide then too.
  useEffect(() => {
    if (streaming) return;
    const refined = refinedSlideIndex(code);
    if (refined >= 0) setIdx(refined);
  }, [code, streaming]);

  const prev = () => setIdx((i) => Math.max(0, i - 1));
  const next = () => setIdx((i) => Math.min(count - 1, i + 1));

  // Keyboard paging. Presentation mode listens window-wide (DeckPresentation
  // owns the screen); inline, the deck pages while it has focus — it is focusable
  // (tabIndex) and grabs focus on click, so "click the presentation, then
  // arrow through it" works. Left/Right, PageUp/PageDown, Home/End.
  const pageKey = (key: string): (() => void) | null => {
    if (key === 'ArrowLeft' || key === 'PageUp') return prev;
    if (key === 'ArrowRight' || key === 'PageDown' || key === ' ') return next;
    if (key === 'Home') return () => setIdx(0);
    if (key === 'End') return () => setIdx(Math.max(0, count - 1));
    return null;
  };

  // Close the download menu on outside click.
  useEffect(() => {
    if (!menu) return;
    const onDoc = (e: MouseEvent) => {
      if (menuRef.current && !menuRef.current.contains(e.target as Node)) setMenu(false);
    };
    document.addEventListener('mousedown', onDoc);
    return () => document.removeEventListener('mousedown', onDoc);
  }, [menu]);

  const shown = Math.min(idx, Math.max(0, count - 1));
  const stage = useMemo(() => stageFor(slides[shown] ?? ''), [slides, shown]);

  if (count === 0) return null;

  const label = streaming
    ? 'Building deck…'
    : `Slide ${shown + 1} / ${count}${truncated ? ' · incomplete' : ''}`;
  const navBtn =
    'inline-flex items-center justify-center rounded p-0.5 transition-colors ' +
    'hover:bg-black/5 dark:hover:bg-white/10 disabled:opacity-40 disabled:hover:bg-transparent';

  const runExport = async (kind: 'pdf' | 'pptx') => {
    setMenu(false);
    setBusy(kind);
    try {
      if (kind === 'pdf') await downloadDeckPdf(slides);
      else await downloadDeckPptx(slides);
    } catch (err) {
      console.error('[deck] export failed', err);
    } finally {
      setBusy('');
    }
  };

  const nav = (
    <>
      <button
        type="button"
        className={navBtn}
        title="Previous slide"
        disabled={shown <= 0}
        onClick={prev}
      >
        <ChevronLeft size={15} />
      </button>
      <button
        type="button"
        className={navBtn}
        title="Next slide"
        disabled={shown >= count - 1}
        onClick={next}
      >
        <ChevronRight size={15} />
      </button>
    </>
  );

  return (
    <div
      className="my-2 overflow-hidden rounded-lg border focus:outline-none focus-visible:ring-2 focus-visible:ring-blue-400"
      style={{ borderColor: 'var(--border-color, rgba(0,0,0,0.12))' }}
      tabIndex={0}
      role="group"
      aria-label={`Slide deck, ${count} slides — use arrow keys to navigate`}
      onClick={(e) => (e.currentTarget as HTMLDivElement).focus({ preventScroll: true })}
      onKeyDown={(e) => {
        if (full || studio) return; // the overlay's own listener owns the keys
        const go = pageKey(e.key);
        if (go) {
          e.preventDefault();
          go();
        }
      }}
    >
      <div
        className="flex items-center justify-between gap-2 px-3 py-1.5 text-xs"
        style={{ background: 'var(--bg-secondary, #f5f5f5)', color: 'var(--text-muted, rgba(0,0,0,0.6))' }}
      >
        <span className="font-medium">{label}</span>
        <div className="flex items-center gap-1">
          {nav}
          <button
            type="button"
            className={navBtn}
            title="Edit deck"
            disabled={streaming}
            // The deck studio: thumbnails, one slide large, an instruction bar
            // that edits ONE slide at a time and writes back into this message.
            onClick={() => setStudio(true)}
          >
            <SquarePen size={14} />
          </button>
          <button type="button" className={navBtn} title="Present" onClick={() => setFull(true)}>
            <Maximize2 size={14} />
          </button>
          <div ref={menuRef} className="relative">
            <button
              type="button"
              className={navBtn}
              title="Download"
              disabled={!!busy || streaming}
              onClick={() => setMenu((m) => !m)}
            >
              {busy ? <Loader2 size={14} className="animate-spin" /> : <Download size={14} />}
            </button>
            {menu && (
              <div
                className="absolute right-0 z-10 mt-1 min-w-[9rem] overflow-hidden rounded-md border py-1 shadow-lg"
                style={{
                  background: 'var(--bg-primary, #fff)',
                  borderColor: 'var(--border-color, rgba(0,0,0,0.12))',
                  color: 'var(--text-primary, #111)',
                }}
              >
                <button
                  type="button"
                  className="block w-full px-3 py-1.5 text-left hover:bg-black/5 dark:hover:bg-white/10"
                  onClick={() => runExport('pdf')}
                >
                  Download PDF
                </button>
                <button
                  type="button"
                  className="block w-full px-3 py-1.5 text-left hover:bg-black/5 dark:hover:bg-white/10"
                  onClick={() => runExport('pptx')}
                >
                  Download PowerPoint
                </button>
              </div>
            )}
          </div>
        </div>
      </div>
      {/* Clicks inside the iframe never bubble out, so a transparent catcher
          over the (static) slide takes the click and focuses the deck — that
          is what makes "click the presentation, then use the arrows" work. */}
      <div className="relative">
        <ScaledFrame
          html={stage}
          baseWidth={SLIDE_W}
          fill={false}
          streaming={streaming}
          title="Slide deck"
        />
        <div className="absolute inset-0" aria-hidden="true" />
      </div>
      {studio && (
        <DeckStudio code={code} messageId={messageId} initialIndex={shown} onClose={() => setStudio(false)} />
      )}
      {full && (
        // Presentation mode: the slide fills the screen on black, arrow keys
        // page, Esc (or leaving native fullscreen) returns to the chat.
        <DeckPresentation
          stage={stage}
          index={shown}
          count={count}
          onPrev={prev}
          onNext={next}
          onFirst={() => setIdx(0)}
          onLast={() => setIdx(Math.max(0, count - 1))}
          onClose={() => setFull(false)}
        />
      )}
    </div>
  );
};

export default HtmlDeckBlock;
