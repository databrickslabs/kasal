import React, { useEffect, useRef, useState } from 'react';
import { Loader2, Sparkles, X } from 'lucide-react';

/**
 * The studio's instruction bar: what should change on the selected slide (or
 * what a new slide should cover), a few one-click chips, Apply. Enter applies,
 * Shift+Enter breaks a line.
 */

export const REFINE_CHIPS = [
  'Shorten the text',
  'Make it more visual',
  'Bigger title',
  'Turn the text into bullets',
  'Add speaker notes',
  'Change the layout',
];

export const ADD_CHIPS = ['An agenda', 'A summary of the previous slide', 'Next steps', 'A section divider'];

interface SlideInstructionBarProps {
  /** Which slide the bar is about (1-based, for the label). */
  slideNumber: number;
  /** `add` writes a new slide at a position instead of revising one. */
  mode: 'refine' | 'add';
  working: boolean;
  error: string | null;
  onApply: (instruction: string) => void;
  /** Leave add mode without adding. */
  onCancelAdd: () => void;
}

const SlideInstructionBar: React.FC<SlideInstructionBarProps> = ({
  slideNumber,
  mode,
  working,
  error,
  onApply,
  onCancelAdd,
}) => {
  const [value, setValue] = useState('');
  const ref = useRef<HTMLTextAreaElement>(null);
  // A new target (slide or mode) starts a fresh instruction.
  useEffect(() => {
    setValue('');
    ref.current?.focus();
  }, [slideNumber, mode]);

  const submit = (text: string) => {
    const t = text.trim();
    if (!t || working) return;
    onApply(t);
    setValue('');
  };
  const chips = mode === 'add' ? ADD_CHIPS : REFINE_CHIPS;

  return (
    <div className="flex flex-col gap-2 px-6 py-3" style={{ background: '#161616', color: '#e5e5e5' }}>
      <div className="flex items-center gap-2 text-xs" style={{ color: '#9a9a9a' }}>
        <span className="font-medium" style={{ color: '#e5e5e5' }}>
          {mode === 'add' ? `New slide at position ${slideNumber}` : `Slide ${slideNumber}`}
        </span>
        {mode === 'add' && (
          <button type="button" className="inline-flex items-center gap-1 hover:opacity-80" onClick={onCancelAdd}>
            <X size={12} /> cancel
          </button>
        )}
        {error && (
          <span role="alert" className="ml-auto" style={{ color: '#ff8a80' }}>
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
          placeholder={mode === 'add' ? 'What should the new slide cover?' : 'What should change on this slide?'}
          aria-label={mode === 'add' ? 'New slide instruction' : 'Slide instruction'}
          className="flex-1 resize-none rounded-lg px-3 py-2 text-sm outline-none focus:ring-2 focus:ring-blue-500/60"
          style={{ background: '#242424', color: '#f2f2f2', border: '1px solid #333' }}
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
      <div className="flex flex-wrap gap-1.5">
        {chips.map((chip) => (
          <button
            key={chip}
            type="button"
            disabled={working}
            onClick={() => submit(chip)}
            className="rounded-full border !px-2.5 !py-1 text-xs hover:opacity-90 disabled:opacity-40"
            style={{ borderColor: '#3a3a3a', color: '#cfcfcf', background: 'transparent' }}
          >
            {chip}
          </button>
        ))}
      </div>
    </div>
  );
};

export default SlideInstructionBar;
