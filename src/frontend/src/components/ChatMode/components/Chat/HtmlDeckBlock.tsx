import React, { useEffect, useMemo, useRef, useState } from 'react';
import { ChevronLeft, ChevronRight, Download, Loader2, Maximize2 } from 'lucide-react';
import { SLIDE_H, SLIDE_W, splitSlides } from '../../utils/htmlDeck';
import { downloadDeckPdf, downloadDeckPptx } from '../../utils/deckExport';
import ScaledFrame from './ScaledFrame';
import FullscreenModal from './FullscreenModal';

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
}

// Wrap a slide section on the fixed stage; force the section to the stage size
// even if the model omitted explicit dimensions.
function stageFor(section: string): string {
  return (
    `<style>.kwrap>section.slide{width:${SLIDE_W}px;height:${SLIDE_H}px;` +
    'box-sizing:border-box;overflow:hidden;}</style>' +
    `<div class="kwrap" style="width:${SLIDE_W}px;height:${SLIDE_H}px;overflow:hidden;background:#fff">` +
    `${section}</div>`
  );
}

const HtmlDeckBlock: React.FC<HtmlDeckBlockProps> = ({ code, streaming = false }) => {
  const slides = useMemo(() => splitSlides(code), [code]);
  const count = slides.length;
  const [idx, setIdx] = useState(0);
  const [full, setFull] = useState(false);
  const [menu, setMenu] = useState(false);
  const [busy, setBusy] = useState<'' | 'pdf' | 'pptx'>('');
  const menuRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    if (streaming && count > 0) setIdx(count - 1);
    else setIdx((i) => Math.min(i, Math.max(0, count - 1)));
  }, [streaming, count]);

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

  const label = streaming ? 'Building deck…' : `Slide ${shown + 1} / ${count}`;
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
        onClick={() => setIdx((i) => Math.max(0, i - 1))}
      >
        <ChevronLeft size={15} />
      </button>
      <button
        type="button"
        className={navBtn}
        title="Next slide"
        disabled={shown >= count - 1}
        onClick={() => setIdx((i) => Math.min(count - 1, i + 1))}
      >
        <ChevronRight size={15} />
      </button>
    </>
  );

  return (
    <div
      className="my-2 overflow-hidden rounded-lg border"
      style={{ borderColor: 'var(--border-color, rgba(0,0,0,0.12))' }}
    >
      <div
        className="flex items-center justify-between gap-2 px-3 py-1.5 text-xs"
        style={{ background: 'var(--bg-secondary, #f5f5f5)', color: 'var(--text-muted, rgba(0,0,0,0.6))' }}
      >
        <span className="font-medium">{label}</span>
        <div className="flex items-center gap-1">
          {nav}
          <button type="button" className={navBtn} title="View fullscreen" onClick={() => setFull(true)}>
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
      <ScaledFrame html={stage} baseWidth={SLIDE_W} fill={false} title="Slide deck" />
      {full && (
        <FullscreenModal title={label} onClose={() => setFull(false)} toolbar={nav}>
          <ScaledFrame html={stage} baseWidth={SLIDE_W} fill={false} contain title="Slide deck (fullscreen)" />
        </FullscreenModal>
      )}
    </div>
  );
};

export default HtmlDeckBlock;
