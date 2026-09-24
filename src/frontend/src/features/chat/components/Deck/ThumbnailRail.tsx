import React, { useState } from 'react';
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
  const [dragging, setDragging] = useState<number | null>(null);
  const [over, setOver] = useState<number | null>(null);

  const insertButton = (at: number) => (
    <button
      type="button"
      key={`add-${at}`}
      className="group/add flex h-4 w-full items-center justify-center opacity-0 transition-opacity hover:opacity-100 focus:opacity-100"
      title="Add a slide here"
      aria-label={`Add a slide at position ${at + 1}`}
      disabled={locked}
      onClick={() => onAddAt(at)}
    >
      <span className="h-px flex-1" style={{ background: 'var(--border-color)' }} />
      <Plus size={12} style={{ color: 'var(--text-secondary)' }} />
      <span className="h-px flex-1" style={{ background: 'var(--border-color)' }} />
    </button>
  );

  return (
    <div
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
              role="listitem"
              aria-label={`Slide ${i + 1}`}
              aria-busy={isWorking}
              aria-current={isSelected ? 'true' : undefined}
              draggable={!locked}
              onDragStart={() => setDragging(i)}
              onDragOver={(e) => {
                e.preventDefault();
                setOver(i);
              }}
              onDragLeave={() => setOver((o) => (o === i ? null : o))}
              onDrop={(e) => {
                e.preventDefault();
                if (!locked && dragging !== null && dragging !== i) onMove(dragging, i);
                setDragging(null);
                setOver(null);
              }}
              onDragEnd={() => {
                setDragging(null);
                setOver(null);
              }}
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
        onClick={() => onAddAt(slides.length)}
      >
        <Plus size={13} /> Add slide
      </button>
    </div>
  );
};

export default ThumbnailRail;
