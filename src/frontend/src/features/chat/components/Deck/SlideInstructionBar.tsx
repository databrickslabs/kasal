import React, { useEffect, useRef, useState } from 'react';
import { Loader2, Sparkles, X } from 'lucide-react';
import { FILL_CHIPS, REFINE_CHIPS } from './chips';

/**
 * The studio's instruction bar: what should change on the selected slide (or
 * what a just-inserted blank slide should cover), a few one-click chips, Apply. Enter applies,
 * Shift+Enter breaks a line.
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

  return (
    <div className="flex flex-col gap-2 px-6 py-3" style={{ background: 'var(--bg-secondary)', color: 'var(--text-primary)' }}>
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
      <div className="flex items-end gap-2">
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
          className="flex-1 resize-none rounded-lg px-3 py-2 text-sm outline-none focus:ring-2 focus:ring-blue-500/60"
          style={{ background: 'var(--bg-input)', color: 'var(--text-primary)', border: '1px solid var(--border-color)' }}
        />
        <button
          type="button"
          onClick={() => submit(value)}
          disabled={working || !value.trim()}
          className="inline-flex items-center gap-1.5 rounded-lg !px-3 !py-2 text-sm font-medium disabled:opacity-40"
          style={{ background: '#2f6feb', color: '#fff' }}
        >
          {working ? <Loader2 size={14} className="animate-spin" /> : <Sparkles size={14} />}
          {working ? 'Working…' : 'Apply'}
        </button>
      </div>
      <div className="flex flex-wrap items-center gap-1.5">
        {controls}
        {chips.map((chip) => (
          <button
            key={chip}
            type="button"
            disabled={working}
            onClick={() => submit(chip)}
            className="rounded-full border !px-2.5 !py-1 text-xs hover:opacity-90 disabled:opacity-40"
            style={{ borderColor: 'var(--border-color)', color: 'var(--text-secondary)', background: 'transparent' }}
          >
            {chip}
          </button>
        ))}
      </div>
    </div>
  );
};

export default SlideInstructionBar;
