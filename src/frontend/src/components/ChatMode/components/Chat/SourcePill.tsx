import React, { useEffect, useRef, useState } from 'react';

import { useAppStore } from '../../store/appStore';
import { useExecutionStore } from '../../store/executionStore';
import { useAnchoredFixedStyle } from '../../hooks/useAnchoredFixedStyle';
import {
  NOTHING_PUBLISHED_REASON,
  SOURCE_MODES,
  SourceModeId,
  nothingPublishedToChat,
} from '../../utils/sourceModes';

interface SourcePillProps {
  /**
   * Which way the menu opens — 'down' on the centred landing composer, 'up'
   * once the composer is pinned to the bottom. Same value the other composer
   * pills get; the POSITION is computed here, against this pill's own trigger.
   */
  menuPlacement?: 'up' | 'down';
  menuAnimClass?: string;
  onPicked?: () => void;
}

/**
 * "Build new" / "Use existing" — the SOURCE control, beside the answer-mode pill.
 *
 * Its own component rather than more surface on ChatInput, which is already
 * well past the file-size ceiling.
 *
 * What is available comes from the SHARED catalog in appStore — the same
 * chat-published list the rail's Catalog section renders — rather than a fetch
 * of its own. One read, one truth: publishing refreshes that catalog, and this
 * control enables itself without a reload because it is subscribed to it. A
 * private fetch here would have gone stale the moment anything was published.
 *
 * It is used only for the DISABLED state: with nothing published to chat there
 * is nothing to reuse, and the reason points at the publish dialog rather than
 * leaving a dead end. No count on the label — it wrapped the pill onto two
 * lines, and the rail names the same set.
 */
const SourcePill: React.FC<SourcePillProps> = ({
  menuPlacement = 'up',
  menuAnimClass = '',
  onPicked,
}) => {
  const preferExisting = useExecutionStore((s) => s.preferExisting);
  const setPreferExisting = useExecutionStore((s) => s.setPreferExisting);

  // The chat-published catalog, shared with the rail. `catalogLoaded` is what
  // separates "still loading" from "nothing published" — an empty list means
  // both, and only the second may disable the control.
  const publishedCrews = useAppStore((s) => s.savedCrews);
  const publishedFlows = useAppStore((s) => s.savedFlows);
  const catalogLoaded = useAppStore((s) => s.catalogLoaded);

  const [open, setOpen] = useState(false);
  const pickerRef = useRef<HTMLDivElement>(null);
  // Anchored to THIS trigger. The menu is `position: fixed` so it escapes the
  // chat layout's overflow-hidden containers instead of being clipped by them.
  const menuStyle = useAnchoredFixedStyle(open, pickerRef, menuPlacement);

  useEffect(() => {
    if (!open) return;
    const onClickOutside = (event: MouseEvent) => {
      if (!pickerRef.current?.contains(event.target as Node)) setOpen(false);
    };
    document.addEventListener('mousedown', onClickOutside);
    return () => document.removeEventListener('mousedown', onClickOutside);
  }, [open]);

  const active = preferExisting ? SOURCE_MODES[1] : SOURCE_MODES[0];
  const noneAvailable = nothingPublishedToChat(
    catalogLoaded ? publishedCrews.length + publishedFlows.length : null,
  );

  const pick = (id: SourceModeId) => {
    if (id === 'existing' && noneAvailable) return;
    setPreferExisting(id === 'existing');
    setOpen(false);
    onPicked?.();
  };

  return (
    <div className="relative" ref={pickerRef}>
      <button
        type="button"
        onClick={() => setOpen(!open)}
        aria-label={`Source: ${active.label}`}
        title={active.hint}
        className="flex items-center gap-1.5 px-2.5 py-1.5 rounded-lg text-xs font-medium transition-colors hover:opacity-80"
        style={{
          color: preferExisting ? 'var(--accent)' : 'var(--text-secondary)',
          backgroundColor: 'transparent',
          border: 'none',
        }}
      >
        <svg
          className="w-3.5 h-3.5"
          fill="none"
          viewBox="0 0 24 24"
          stroke="currentColor"
          strokeWidth={2}
        >
          <path
            strokeLinecap="round"
            strokeLinejoin="round"
            d="M20.25 6.375c0 2.278-3.694 4.125-8.25 4.125S3.75 8.653 3.75 6.375m16.5 0c0-2.278-3.694-4.125-8.25-4.125S3.75 4.097 3.75 6.375m16.5 0v11.25c0 2.278-3.694 4.125-8.25 4.125s-8.25-1.847-8.25-4.125V6.375m16.5 0v3.75m-16.5-3.75v3.75m16.5 0v3.75C20.25 16.153 16.556 18 12 18s-8.25-1.847-8.25-4.125v-3.75"
          />
        </svg>
        {/* No count on the label. It wrapped the pill onto two lines in the
            composer's control row, and the rail's Catalog section already shows
            what is available — the number was teaching the same thing twice. */}
        <span>{active.short}</span>
        <svg
          className={`w-3 h-3 transition-transform ${open ? 'rotate-180' : ''}`}
          fill="none"
          viewBox="0 0 24 24"
          stroke="currentColor"
          strokeWidth={2.5}
        >
          <path strokeLinecap="round" strokeLinejoin="round" d="M19.5 8.25l-7.5 7.5-7.5-7.5" />
        </svg>
      </button>

      {open && (
        <div
          className={`kasal-popover ${menuAnimClass} w-72 rounded-xl overflow-hidden z-50`}
          style={{
            ...menuStyle,
            backgroundColor: 'var(--bg-input)',
            border: '1px solid var(--border-color)',
          }}
        >
          <div className="px-3 py-2">
            <span
              className="text-[10px] font-semibold uppercase tracking-wider"
              style={{ color: 'var(--text-muted)' }}
            >
              Source
            </span>
          </div>
          <div className="px-1.5 pb-1.5">
            {SOURCE_MODES.map((mode) => {
              const disabled = mode.id === 'existing' && noneAvailable;
              const selected = (mode.id === 'existing') === preferExisting;
              return (
                <button
                  key={mode.id}
                  disabled={disabled}
                  onClick={() => pick(mode.id)}
                  aria-label={`Source: ${mode.label}`}
                  title={disabled ? NOTHING_PUBLISHED_REASON : undefined}
                  className={`w-full text-left !px-2.5 !py-2 my-0.5 rounded-lg flex items-center justify-between transition-colors ${
                    disabled
                      ? 'opacity-50 cursor-not-allowed'
                      : selected
                        ? 'bg-[var(--bg-active-chip)]'
                        : 'hover:bg-[var(--bg-rail-hover)]'
                  }`}
                >
                  <div>
                    <div
                      className="text-sm font-medium"
                      style={{ color: 'var(--text-primary)' }}
                    >
                      {mode.label}
                    </div>
                    <div
                      className="text-[11px] mt-0.5"
                      style={{ color: 'var(--text-muted)' }}
                    >
                      {mode.hint}
                    </div>
                  </div>
                  {selected && !disabled && (
                    <svg
                      className="w-4 h-4 flex-shrink-0"
                      style={{ color: 'var(--accent)' }}
                      fill="none"
                      viewBox="0 0 24 24"
                      stroke="currentColor"
                      strokeWidth={2.5}
                    >
                      <path strokeLinecap="round" strokeLinejoin="round" d="M5 13l4 4L19 7" />
                    </svg>
                  )}
                </button>
              );
            })}
            {noneAvailable && (
              <div
                className="!px-2.5 !py-2 text-[11px] leading-snug"
                style={{ color: 'var(--text-muted)' }}
              >
                {NOTHING_PUBLISHED_REASON}
              </div>
            )}
          </div>
        </div>
      )}
    </div>
  );
};

export default SourcePill;
