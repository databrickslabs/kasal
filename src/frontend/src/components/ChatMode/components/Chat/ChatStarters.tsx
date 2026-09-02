import React from 'react';
import type { LucideIcon } from 'lucide-react';
import {
  Brain,
  CircleHelp,
  LayoutDashboard,
  Layers,
  Map,
  Presentation,
  Sparkles,
  TrendingUp,
  Workflow,
} from 'lucide-react';

/**
 * Starter chips for a NEW chat: nine things Kasal builds from a sentence.
 * Each chip drops an opening phrase into the composer (via the landing's
 * prefill) so the user finishes the sentence — it never sends on its own.
 * Shown only on the landing (ChatEmptyState), never mid-conversation, and
 * deliberately not in the "+" picker, which is for settings and attachments.
 */
export interface Starter {
  label: string;
  /** Opening text placed in the composer, with a trailing space to continue. */
  prefill: string;
  icon: LucideIcon;
}

export const STARTERS: Starter[] = [
  { label: 'Create a skill', prefill: 'Create a skill for ', icon: Sparkles },
  { label: 'Create a presentation', prefill: 'Create a presentation about ', icon: Presentation },
  { label: 'Create a diagram', prefill: 'Create a diagram of ', icon: Workflow },
  { label: 'Create a quiz', prefill: 'Create a quiz about ', icon: CircleHelp },
  { label: 'Create a dashboard', prefill: 'Create a dashboard of ', icon: LayoutDashboard },
  { label: 'Show a map', prefill: 'Show the map of ', icon: Map },
  { label: 'Create a mindmap', prefill: 'Create a mindmap of ', icon: Brain },
  { label: 'Create flashcards', prefill: 'Create flashcards about ', icon: Layers },
  { label: 'Create a forecast', prefill: 'Create a forecast of ', icon: TrendingUp },
];

const ChatStarters: React.FC<{ onPick: (text: string) => void }> = ({ onPick }) => (
  <div
    className="grid grid-cols-3 gap-2 w-full max-w-2xl mx-auto"
    role="group"
    aria-label="Start with"
    data-testid="chat-starters"
  >
    {STARTERS.map(({ label, prefill, icon: Icon }) => (
      <button
        key={label}
        type="button"
        onClick={() => onPick(prefill)}
        className="flex items-center gap-2 rounded-lg border !px-3 !py-2 text-xs text-left transition-colors hover:bg-[var(--bg-rail-hover)]"
        style={{
          backgroundColor: 'var(--bg-primary)',
          borderColor: 'var(--border-color)',
          color: 'var(--text-secondary)',
        }}
      >
        <Icon size={14} className="shrink-0" />
        <span className="truncate">{label}</span>
      </button>
    ))}
  </div>
);

export default ChatStarters;
