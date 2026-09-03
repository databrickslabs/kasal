import React, { useState } from 'react';
import { Copy, Loader2, Plus, Trash2 } from 'lucide-react';
import ScaledFrame from '../Chat/ScaledFrame';
import { SLIDE_W, stageFor } from '../../utils/htmlDeck';

/**
 * The studio's left rail: every slide small and numbered, the selected one
 * outlined. Click selects, drag reorders, hover shows duplicate / delete, a
 * "+" between slides (and "Add slide" at the bottom) inserts a blank one
 * there at once. All of it instant — no model involved.
 */

interface ThumbnailRailProps {
  slides: string[];
  selected: number;
  /** Index of the slide a model call is rewriting, if any. */
  working: number | null;
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
      onClick={() => onAddAt(at)}
    >
      <span className="h-px flex-1" style={{ background: '#3a3a3a' }} />
      <Plus size={12} style={{ color: '#bbb' }} />
      <span className="h-px flex-1" style={{ background: '#3a3a3a' }} />
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
      style={{ background: '#0d0d0d', borderRight: '1px solid #222', scrollbarGutter: 'stable' }}
      role="list"
      aria-label="Slides"
    >
      {insertButton(0)}
      {slides.map((slide, i) => {
        const isSelected = i === selected;
        const isWorking = working === i;
        return (
          <React.Fragment key={i}>
            <div
              role="listitem"
              aria-label={`Slide ${i + 1}`}
              aria-current={isSelected ? 'true' : undefined}
              draggable
              onDragStart={() => setDragging(i)}
              onDragOver={(e) => {
                e.preventDefault();
                setOver(i);
              }}
              onDragLeave={() => setOver((o) => (o === i ? null : o))}
              onDrop={(e) => {
                e.preventDefault();
                if (dragging !== null && dragging !== i) onMove(dragging, i);
                setDragging(null);
                setOver(null);
              }}
              onDragEnd={() => {
                setDragging(null);
                setOver(null);
              }}
              className="group relative flex cursor-pointer gap-2"
            >
              <span className="w-4 pt-1 text-right text-[11px] tabular-nums" style={{ color: isSelected ? '#fff' : '#777' }}>
                {i + 1}
              </span>
              <div
                className="relative flex-1 overflow-hidden rounded-md transition-shadow"
                style={{
                  outline: isSelected ? '2px solid #e5734a' : over === i ? '2px dashed #666' : '1px solid #2a2a2a',
                  outlineOffset: 1,
                  // Dark until the frame has fitted its slide (a few hundred ms per
                  // thumbnail): a white flash read as an empty, broken rail.
                  background: '#1a1a1a',
                }}
              >
                <ScaledFrame
                  html={stageFor(slide)}
                  baseWidth={SLIDE_W}
                  fill={false}
                  pad={0}
                  background="#1a1a1a"
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
                  <div className="absolute inset-0 flex items-center justify-center" style={{ background: 'rgba(0,0,0,0.45)' }}>
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
                    disabled={slides.length <= 1}
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
        className="mt-2 inline-flex items-center justify-center gap-1.5 rounded-md border !px-2 !py-1.5 text-xs hover:bg-white/10"
        style={{ borderColor: '#333', color: '#cfcfcf' }}
        onClick={() => onAddAt(slides.length)}
      >
        <Plus size={13} /> Add slide
      </button>
    </div>
  );
};

export default ThumbnailRail;
