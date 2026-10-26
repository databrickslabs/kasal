import React, { useEffect, useRef, useState } from 'react';
import { X } from 'lucide-react';
import { SLIDE_W } from '../../utils/htmlDeck';
import ScaledFrame from './ScaledFrame';

/**
 * Presentation mode for an HTML deck: the current slide fills the screen,
 * letterboxed on black, with nothing else on screen (the exit control shows
 * on mouse movement) — the browser's Fullscreen API when available, a fixed
 * black overlay otherwise. Position is exposed to assistive tech only.
 *
 * Keyboard paging works because the iframe never gets focus: a transparent
 * catcher sits over it (clicks page — left half back, right half forward),
 * and the overlay itself is focused on open, so keydowns reach the window
 * listener instead of dying inside the sandboxed document.
 */
interface DeckPresentationProps {
  /** The current slide, already wrapped on the fixed stage. */
  stage: string;
  index: number;
  count: number;
  onPrev: () => void;
  onNext: () => void;
  onFirst: () => void;
  onLast: () => void;
  onClose: () => void;
}

const DeckPresentation: React.FC<DeckPresentationProps> = ({
  stage,
  index,
  count,
  onPrev,
  onNext,
  onFirst,
  onLast,
  onClose,
}) => {
  const rootRef = useRef<HTMLDivElement>(null);
  const enteredRef = useRef(false);
  // Nothing on screen but the slide. The exit control appears on mouse
  // movement and fades again after a moment, like any presenter's cursor.
  const [chrome, setChrome] = useState(false);
  const chromeTimer = useRef<ReturnType<typeof setTimeout> | null>(null);
  const showChrome = () => {
    setChrome(true);
    if (chromeTimer.current) clearTimeout(chromeTimer.current);
    chromeTimer.current = setTimeout(() => setChrome(false), 1800);
  };
  useEffect(() => () => {
    if (chromeTimer.current) clearTimeout(chromeTimer.current);
  }, []);

  // Keys, window-wide: the overlay owns the screen while it is open.
  useEffect(() => {
    const onKey = (e: KeyboardEvent) => {
      const k = e.key;
      let go: (() => void) | null = null;
      if (k === 'ArrowLeft' || k === 'PageUp' || k === 'ArrowUp') go = onPrev;
      else if (k === 'ArrowRight' || k === 'PageDown' || k === 'ArrowDown' || k === ' ') go = onNext;
      else if (k === 'Home') go = onFirst;
      else if (k === 'End') go = onLast;
      else if (k === 'Escape') go = onClose;
      if (go) {
        e.preventDefault();
        go();
      }
    };
    window.addEventListener('keydown', onKey);
    return () => window.removeEventListener('keydown', onKey);
  }, [onPrev, onNext, onFirst, onLast, onClose]);

  // Real fullscreen when the browser allows it (we are still inside the user's
  // click activation window); the fixed overlay is the fallback. Leaving native
  // fullscreen by any means (Esc, the browser's own control) closes the deck.
  useEffect(() => {
    const el = rootRef.current;
    if (!el) return;
    el.focus({ preventScroll: true });
    const doc = document as Document & { fullscreenElement?: Element | null };
    const request = (el as HTMLElement & { requestFullscreen?: () => Promise<void> }).requestFullscreen;
    if (typeof request === 'function') {
      request.call(el).then(() => { enteredRef.current = true; }).catch(() => { /* overlay fallback */ });
    }
    const onChange = () => {
      if (enteredRef.current && !doc.fullscreenElement) onClose();
    };
    document.addEventListener('fullscreenchange', onChange);
    return () => {
      document.removeEventListener('fullscreenchange', onChange);
      if (doc.fullscreenElement === el && typeof document.exitFullscreen === 'function') {
        void document.exitFullscreen().catch(() => undefined);
      }
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  return (
    <div
      ref={rootRef}
      tabIndex={-1}
      role="dialog"
      aria-modal="true"
      aria-label={`Presentation, slide ${index + 1} of ${count}`}
      className="fixed inset-0 outline-none"
      style={{ zIndex: 10000, background: '#000', cursor: chrome ? 'default' : 'none' }}
      onMouseMove={showChrome}
    >
      <div className="absolute inset-0">
        <ScaledFrame
          html={stage}
          baseWidth={SLIDE_W}
          fill={false}
          upscale
          contain
          background="#000"
          pad={0}
          title="Presentation"
        />
      </div>
      {/* Click catcher: keeps focus out of the iframe; halves page the deck. */}
      <div className="absolute inset-0 flex" aria-hidden="true">
        <div className="flex-1 cursor-pointer" data-testid="deck-prev-zone" onClick={onPrev} />
        <div className="flex-1 cursor-pointer" data-testid="deck-next-zone" onClick={onNext} />
      </div>
      <button
        type="button"
        onClick={onClose}
        title="Exit presentation (Esc)"
        aria-label="Exit presentation"
        className="absolute top-3 right-3 inline-flex items-center justify-center rounded p-1 transition-opacity"
        style={{
          background: 'rgba(0,0,0,0.55)',
          color: 'rgba(255,255,255,0.85)',
          opacity: chrome ? 1 : 0,
          pointerEvents: chrome ? 'auto' : 'none',
        }}
      >
        <X size={16} />
      </button>
    </div>
  );
};

export default DeckPresentation;
