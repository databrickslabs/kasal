import React, { useLayoutEffect, useRef, useState } from 'react';
import { ChevronDown, ChevronUp, Copy, Loader2, Plus, Trash2 } from 'lucide-react';
import ScaledFrame from '../Chat/ScaledFrame';
import { SLIDE_W, stageFor } from '../../utils/htmlDeck';
import { hasPendingAssets } from '../../utils/assetRefs';

/**
 * The studio's left rail: every slide small and numbered, the selected one
 * outlined. Click selects, drag reorders, hover shows duplicate / delete, a
 * "+" between slides (and "Add slide" at the bottom) inserts a blank one
 * there at once. All of it instant — no model involved.
 */

interface ThumbnailRailProps {
  slides: string[];
  selected: number;
  /** Slides with generation or saving in progress. */
  working: ReadonlySet<number>;
  locked: boolean;
  onSelect: (index: number) => void;
  onMove: (from: number, to: number) => void;
  onDuplicate: (index: number) => void;
  onRemove: (index: number) => void;
  /** Add a new slide so that it becomes slide `index` (0-based). */
  onAddAt: (index: number) => void;
}

const ThumbnailRail: React.FC<ThumbnailRailProps> = ({
  slides,
  selected,
  working,
  locked,
  onSelect,
  onMove,
  onDuplicate,
  onRemove,
  onAddAt,
}) => {
  const railRef = useRef<HTMLDivElement>(null);
  const selectedRef = useRef<HTMLDivElement>(null);
  const dragging = useRef<number | null>(null);
  const [gap, setGap] = useState<number | null>(null);
  const [over, setOver] = useState<number | null>(null);

  useLayoutEffect(() => {
    const rail = railRef.current;
    const slide = selectedRef.current;
    if (!rail || !slide) return;
    const reveal = () => {
      if (dragging.current !== null) return;
      const viewport = rail.getBoundingClientRect();
      const bounds = slide.getBoundingClientRect();
      const top = viewport.top + rail.clientTop;
      const bottom = top + rail.clientHeight;
      // Scroll only this rail, by the minimum needed; don't move keyboard focus
      // or the surrounding chat. Already-visible thumbnails stay in place.
      if (bounds.top < top) rail.scrollTop += bounds.top - top;
      else if (bounds.bottom > bottom) rail.scrollTop += Math.min(bounds.bottom - bottom, bounds.top - top);
    };
    reveal();
    // Iframes fit asynchronously; earlier thumbnails can change this one's position.
    if (typeof ResizeObserver === 'undefined') return;
    const observer = new ResizeObserver(reveal);
    observer.observe(rail);
    rail.querySelectorAll('[role="listitem"]').forEach(row => observer.observe(row));
    return () => observer.disconnect();
  }, [selected, slides.length]);

  const resetDrag = () => {
    dragging.current = null;
    setOver(null);
    setGap(null);
  };
  const acceptDrag = (event: React.DragEvent) => {
    if (locked || dragging.current === null) return false;
    event.preventDefault();
    event.stopPropagation();
    if (event.dataTransfer) event.dataTransfer.dropEffect = 'move';
    return true;
  };
  const drop = (event: React.DragEvent, to: number) => {
    const from = dragging.current;
    if (!acceptDrag(event) || from === null) { resetDrag(); return; }
    resetDrag();
    if (from !== to) onMove(from, to);
  };

  const insertButton = (at: number) => (
    <button
      type="button"
      key={`add-${at}`}
      className="group/add flex h-4 w-full items-center justify-center opacity-0 transition-opacity hover:opacity-100 focus:opacity-100"
      style={{ opacity: gap === at ? 1 : undefined }}
      onDragEnter={event => { if (acceptDrag(event)) { setGap(at); setOver(null); } }}
      onDragOver={event => { if (acceptDrag(event)) { setGap(at); setOver(null); } }}
      onDragLeave={() => setGap(null)}
      onDrop={event => drop(event, at > (dragging.current ?? at) ? at - 1 : at)}
      title="Add a slide here"
      aria-label={`Add a slide at position ${at + 1}`}
      disabled={locked}
      onClick={() => onAddAt(at)}
    >
      <span className="pointer-events-none h-px flex-1" style={{ background: gap === at ? 'var(--accent)' : 'var(--border-color)' }} />
      <Plus className="pointer-events-none" size={12} style={{ color: 'var(--text-secondary)' }} />
      <span className="pointer-events-none h-px flex-1" style={{ background: gap === at ? 'var(--accent)' : 'var(--border-color)' }} />
    </button>
  );

  return (
    <div
      ref={railRef}
      className="flex h-full w-56 flex-col overflow-y-auto px-3 py-3"
      // scrollbar-gutter: stable reserves the scrollbar's width whether or not it
      // shows. Without it, a width-consuming scrollbar (macOS with a mouse, or
      // "Always show scrollbars") sets up a feedback loop: each thumbnail iframe
      // scales to the rail's inner width, so toggling the scrollbar refits every
      // thumbnail, which changes the total height, which re-toggles the
      // scrollbar — the rail shakes forever. A reserved gutter keeps the inner
      // width constant, so a fit happens once and settles.
      style={{ background: 'var(--bg-sidebar)', borderRight: '1px solid var(--border-color)', scrollbarGutter: 'stable' }}
      role="list"
      aria-label="Slides"
    >
      {insertButton(0)}
      {slides.map((slide, i) => {
        const isSelected = i === selected;
        const isWorking = working.has(i);
        return (
          <React.Fragment key={i}>
            <div
              ref={isSelected ? selectedRef : undefined}
              role="listitem"
              aria-label={`Slide ${i + 1}`}
              aria-busy={isWorking}
              aria-current={isSelected ? 'true' : undefined}
              draggable={!locked}
              onDragStart={event => {
                if (locked) { event.preventDefault(); resetDrag(); return; }
                event.stopPropagation();
                dragging.current = i;
                // Give native drag a payload (required by some browsers), but
                // trust only our local source ref when accepting a drop.
                if (event.dataTransfer) {
                  event.dataTransfer.setData('text/plain', String(i));
                  event.dataTransfer.effectAllowed = 'move';
                }
              }}
              onDragOver={event => {
                if (acceptDrag(event)) { setOver(i); setGap(null); }
              }}
              onDragLeave={() => setOver(o => o === i ? null : o)}
              onDrop={event => drop(event, i)}
              onDragEnd={resetDrag}
              className="group relative flex cursor-pointer gap-2"
            >
              <div className="flex w-5 shrink-0 flex-col items-center gap-1 pt-1" style={{ color: 'var(--text-secondary)' }}>
                <span className="text-[11px] tabular-nums" style={{ color: isSelected ? 'var(--text-primary)' : undefined }}>{i + 1}</span>
                <button
                  type="button"
                  className="rounded p-0.5 opacity-0 transition-opacity group-hover:opacity-100 group-focus-within:opacity-100 hover:bg-[var(--bg-rail-hover)] focus-visible:outline disabled:text-[var(--text-muted)]"
                  title={locked ? 'Wait for slide edits to finish saving' : 'Move slide up'}
                  aria-label={`Move slide ${i + 1} up`}
                  disabled={locked || i === 0}
                  onClick={() => onMove(i, i - 1)}
                >
                  <ChevronUp size={14} />
                </button>
                <button
                  type="button"
                  className="rounded p-0.5 opacity-0 transition-opacity group-hover:opacity-100 group-focus-within:opacity-100 hover:bg-[var(--bg-rail-hover)] focus-visible:outline disabled:text-[var(--text-muted)]"
                  title={locked ? 'Wait for slide edits to finish saving' : 'Move slide down'}
                  aria-label={`Move slide ${i + 1} down`}
                  disabled={locked || i === slides.length - 1}
                  onClick={() => onMove(i, i + 1)}
                >
                  <ChevronDown size={14} />
                </button>
              </div>
              <div
                className="relative flex-1 overflow-hidden rounded-md transition-shadow"
                style={{
                  outline: isSelected ? '2px solid var(--accent)' : over === i ? '2px dashed var(--text-secondary)' : '1px solid var(--border-color)',
                  outlineOffset: 1,
                  // Match the rail while the slide frame is fitting.
                  background: 'var(--bg-secondary)',
                }}
              >
                <ScaledFrame
                  // Remount once the slide's images are in (see hasPendingAssets).
                  key={hasPendingAssets(slide) ? 'pending' : 'ready'}
                  html={stageFor(slide)}
                  baseWidth={SLIDE_W}
                  fill={false}
                  pad={0}
                  background="transparent"
                  title={`Slide ${i + 1} thumbnail`}
                />
                {/* The iframe swallows clicks; a transparent catcher over it selects.
                    Selection is on pointer-DOWN, not click: the row is draggable,
                    so a press that moves even slightly starts a native drag and
                    the click never fires — the rail scrolled but the slide never
                    got selected. pointerdown fires on press regardless. onClick
                    stays for keyboard/assistive activation of the button. */}
                <button
                  type="button"
                  className="absolute inset-0 h-full w-full"
                  aria-label={`Select slide ${i + 1}`}
                  draggable={!locked}
                  onPointerDown={() => onSelect(i)}
                  onClick={() => onSelect(i)}
                />
                {isWorking && (
                  <div className="pointer-events-none absolute inset-0 flex items-center justify-center" style={{ background: 'rgba(0,0,0,0.45)' }}>
                    <Loader2 size={18} className="animate-spin" style={{ color: '#fff' }} />
                  </div>
                )}
                <div className="absolute right-1 top-1 hidden gap-1 group-hover:flex">
                  <button
                    type="button"
                    className="rounded p-1"
                    style={{ background: 'rgba(0,0,0,0.65)', color: '#fff' }}
                    title="Duplicate slide"
                    aria-label={`Duplicate slide ${i + 1}`}
                    disabled={locked}
                    onClick={() => onDuplicate(i)}
                  >
                    <Copy size={12} />
                  </button>
                  <button
                    type="button"
                    className="rounded p-1 disabled:opacity-40"
                    style={{ background: 'rgba(0,0,0,0.65)', color: '#fff' }}
                    title="Delete slide"
                    aria-label={`Delete slide ${i + 1}`}
                    disabled={locked || slides.length <= 1}
                    onClick={() => onRemove(i)}
                  >
                    <Trash2 size={12} />
                  </button>
                </div>
              </div>
            </div>
            {insertButton(i + 1)}
          </React.Fragment>
        );
      })}
      {/* The "+" between slides only shows on hover; this one is always there. */}
      <button
        type="button"
        className="mt-2 inline-flex items-center justify-center gap-1.5 rounded-md border !px-2 !py-1.5 text-xs hover:bg-[var(--bg-rail-hover)]"
        style={{ borderColor: 'var(--border-color)', color: 'var(--text-secondary)' }}
        disabled={locked}
        onDragOver={event => { if (acceptDrag(event)) setGap(slides.length); }}
        onDrop={event => drop(event, slides.length - 1)}
        onClick={() => onAddAt(slides.length)}
      >
        <Plus size={13} /> Add slide
      </button>
    </div>
  );
};

export default ThumbnailRail;
