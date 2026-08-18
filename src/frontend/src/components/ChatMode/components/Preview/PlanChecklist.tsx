/**
 * A `todo` / `plan_updated` step, rendered as the checklist it is.
 *
 * The payload literally holds "[>] 1. Create the table" — bracket markers a
 * human is expected to decode. The trace dialog already parses these into
 * structured items ({@link extractPlanItems}); this draws them in ChatMode's
 * own styling: a progress line, then one row per step with its state.
 */
import React from 'react';
import { Check, CircleDashed, CircleDot, X } from 'lucide-react';
import type { PlanItem } from '../../../Jobs/TracePlanView';

function StatusIcon({ status }: { status: string }): JSX.Element {
  const common = { size: 14, className: 'flex-shrink-0 mt-0.5' } as const;
  if (status === 'completed') return <Check {...common} style={{ color: 'var(--success, #16a34a)' }} />;
  if (status === 'in_progress') return <CircleDot {...common} style={{ color: 'var(--accent)' }} />;
  if (status === 'cancelled') return <X {...common} style={{ color: 'var(--text-muted)' }} />;
  return <CircleDashed {...common} style={{ color: 'var(--text-muted)' }} />;
}

const PlanChecklist: React.FC<{ items: PlanItem[] }> = ({ items }) => {
  const done = items.filter((i) => i.status === 'completed').length;
  const pct = items.length ? Math.round((done / items.length) * 100) : 0;

  return (
    <div className="p-4">
      <div className="flex items-center gap-2 mb-3">
        <span className="text-xs font-medium" style={{ color: 'var(--text-primary)' }}>
          Plan
        </span>
        <span className="text-xs" style={{ color: 'var(--text-secondary)' }}>
          {done}/{items.length} done
        </span>
      </div>
      <div
        className="h-1 rounded-full mb-4 overflow-hidden"
        style={{ backgroundColor: 'var(--bg-secondary)' }}
        role="progressbar"
        aria-valuenow={pct}
        aria-valuemin={0}
        aria-valuemax={100}
      >
        <div className="h-full rounded-full" style={{ width: `${pct}%`, backgroundColor: 'var(--accent)' }} />
      </div>
      <ol className="space-y-2">
        {items.map((item, idx) => {
          const struck = item.status === 'completed' || item.status === 'cancelled';
          return (
            <li key={item.id || idx} className="flex items-start gap-2 text-xs">
              <StatusIcon status={item.status} />
              <div className="min-w-0">
                <span
                  style={{
                    color: struck ? 'var(--text-muted)' : 'var(--text-primary)',
                    textDecoration: item.status === 'cancelled' ? 'line-through' : 'none',
                  }}
                >
                  {item.label || item.content}
                </span>
                {/* The label is a model-written shorthand; keep the full step
                    under it so nothing the plan actually said is hidden. */}
                {item.label && item.content && item.label !== item.content && (
                  <div className="mt-0.5" style={{ color: 'var(--text-secondary)' }}>
                    {item.content}
                  </div>
                )}
              </div>
            </li>
          );
        })}
      </ol>
    </div>
  );
};

export default PlanChecklist;
