import React, { useEffect, useRef, useState } from 'react';
import { ArrowUp, Loader2, X } from 'lucide-react';
import { FILL_CHIPS, REFINE_CHIPS } from './chips';

/**
 * The studio's instruction bar: what should change on the selected slide (or
 * what a just-inserted blank slide should cover), a few one-click chips, Apply.
 * Enter applies, Shift+Enter breaks a line. Drawn like the chat composer — one
 * rounded input box, chips and tools along its bottom, an arrow to send.
 */

interface SlideInstructionBarProps {
  /** Which slide the bar is about (1-based, for the label). */
  slideNumber: number;
  controls?: React.ReactNode;
  /** `fill` writes a just-inserted blank slide from scratch instead of revising one. */
  mode: 'refine' | 'fill';
  working: boolean;
  error: string | null;
  onApply: (instruction: string) => void;
  /** Leave fill mode — the blank slide stays as it is. */
  onCancelFill: () => void;
}

const SlideInstructionBar: React.FC<SlideInstructionBarProps> = ({
  slideNumber,
  controls,
  mode,
  working,
  error,
  onApply,
  onCancelFill,
}) => {
  const [value, setValue] = useState('');
  const ref = useRef<HTMLTextAreaElement>(null);
  // A new target (slide or mode) starts a fresh instruction.
  useEffect(() => {
    setValue('');
  }, [slideNumber, mode]);

  useEffect(() => {
    // Someone typing into the slide itself (the stage frame) keeps the caret.
    if (document.activeElement?.tagName === 'IFRAME') return;
    // A disabled textarea cannot keep focus. Without a modal fallback, arrow
    // keys go to the page and scroll it instead of navigating running slides.
    const target = working
      ? ref.current?.closest<HTMLElement>('[role="dialog"]')
      : ref.current;
    target?.focus({ preventScroll: true });
  }, [slideNumber, mode, working]);

  const submit = (text: string) => {
    const t = text.trim();
    if (!t || working) return;
    onApply(t);
    setValue('');
  };
  const chips = mode === 'fill' ? FILL_CHIPS : REFINE_CHIPS;

  const ready = !working && !!value.trim();
  // Chips and the send arrow share the composer's look: hairline border, quiet text.
  const chipStyle = { border: '1px solid var(--border-color)', color: 'var(--text-secondary)' };

  return (
    // Right padding keeps the box clear of the app's floating help button.
    <div className="flex flex-col gap-2 py-3 pl-6 pr-14" style={{ background: 'var(--bg-secondary)', color: 'var(--text-primary)' }}>
      <div className="flex items-center gap-2 text-xs" style={{ color: 'var(--text-secondary)' }}>
        <span className="font-medium" style={{ color: 'var(--text-primary)' }}>
          {mode === 'fill' ? `New slide ${slideNumber} — what should it cover?` : `Slide ${slideNumber}`}
        </span>
        {mode === 'fill' && (
          <button type="button" className="inline-flex items-center gap-1 hover:opacity-80" onClick={onCancelFill}>
            <X size={12} /> leave it blank
          </button>
        )}
        {error && (
          <span role="alert" className="ml-auto" style={{ color: 'hsl(var(--a2-destructive))' }}>
            {error}
          </span>
        )}
      </div>
      <div
        className="rounded-3xl"
        style={{ background: 'var(--bg-input)', border: '1px solid var(--border-color)', boxShadow: 'var(--shadow-input)' }}
      >
        <textarea
          ref={ref}
          value={value}
          onChange={(e) => setValue(e.target.value)}
          onKeyDown={(e) => {
            if (e.key === 'Enter' && !e.shiftKey) {
              e.preventDefault();
              submit(value);
            }
          }}
          rows={1}
          disabled={working}
          placeholder={mode === 'fill' ? 'Describe the new slide' : 'What should change on this slide?'}
          aria-label={mode === 'fill' ? 'New slide instruction' : 'Slide instruction'}
          className="block w-full resize-none border-0 bg-transparent !px-5 !pt-3.5 !pb-1 text-sm leading-relaxed outline-none disabled:opacity-50"
          style={{ color: 'var(--text-primary)' }}
        />
        <div className="flex flex-wrap items-center gap-1.5 !px-3 !pb-2.5 !pt-1">
          {chips.map((chip) => (
            <button
              key={chip}
              type="button"
              disabled={working}
              onClick={() => submit(chip)}
              className="rounded-full !px-2.5 !py-1 text-xs transition-colors hover:bg-[var(--bg-rail-hover)] disabled:opacity-40"
              style={chipStyle}
            >
              {chip}
            </button>
          ))}
          <div className="ml-auto flex items-center gap-1.5">
            {controls}
            <button
              type="button"
              onClick={() => submit(value)}
              disabled={!ready}
              aria-label="Apply"
              title={working ? 'Working…' : `Apply to slide ${slideNumber} (Enter)`}
              className="flex h-8 w-8 flex-shrink-0 items-center justify-center rounded-xl !p-0 transition-opacity hover:opacity-80 disabled:cursor-not-allowed"
              style={{ ...chipStyle, background: 'var(--bg-secondary)', color: ready ? 'var(--text-secondary)' : 'var(--text-muted)' }}
            >
              {working ? <Loader2 size={16} className="animate-spin" /> : <ArrowUp size={16} />}
            </button>
          </div>
        </div>
      </div>
    </div>
  );
};

export default SlideInstructionBar;
