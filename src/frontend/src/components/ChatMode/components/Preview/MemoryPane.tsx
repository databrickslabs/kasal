/**
 * The run's memory, rendered INSIDE the chat's right preview pane — the chat
 * counterpart of the MemoryRecordsBrowser dialog, restyled with the chat's
 * design tokens (var(--*)) instead of MUI. Pinned to one run: what it SAVED
 * (records written in its time window) or RECALLED (ids from its retrieval
 * traces), as a concept graph or a record list. All derivation comes from the
 * shared MemoryBackend/memoryData layer, so the pane and the dialog agree.
 */
import React, { useMemo, useState } from 'react';
import { ConceptForceGraph } from '../../../MemoryBackend/ConceptForceGraph';
import {
  formatRelative,
  importanceColor,
  MemoryRecord,
  recordAgent,
} from '../../../MemoryBackend/memoryData';
import { useRunMemory, MemoryMode } from '../../hooks/useRunMemory';

type View = 'graph' | 'records';

const SEGMENT =
  '!px-3 !py-1.5 rounded-md text-xs font-medium transition-colors';

const Segmented: React.FC<{
  value: string;
  options: { id: string; label: string }[];
  onChange: (id: string) => void;
  'aria-label': string;
}> = ({ value, options, onChange, ...aria }) => (
  <div
    role="tablist"
    aria-label={aria['aria-label']}
    className="inline-flex items-center gap-1 rounded-lg p-1"
    style={{ backgroundColor: 'var(--bg-secondary)' }}
  >
    {options.map((o) => (
      <button
        key={o.id}
        type="button"
        role="tab"
        aria-selected={o.id === value}
        onClick={() => onChange(o.id)}
        className={SEGMENT}
        style={{
          color: o.id === value ? 'var(--text-primary)' : 'var(--text-muted)',
          backgroundColor: o.id === value ? 'var(--bg-primary)' : 'transparent',
          boxShadow: o.id === value ? 'var(--shadow-input)' : 'none',
        }}
      >
        {o.label}
      </button>
    ))}
  </div>
);

const Stat: React.FC<{ label: string; value: string | number }> = ({ label, value }) => (
  <div
    className="flex flex-col rounded-xl px-3 py-2 min-w-[88px]"
    style={{ backgroundColor: 'var(--bg-secondary)' }}
  >
    <span className="text-[10px] uppercase tracking-wide" style={{ color: 'var(--text-muted)' }}>
      {label}
    </span>
    <span className="text-[15px] font-semibold tabular-nums" style={{ color: 'var(--text-primary)' }}>
      {value}
    </span>
  </div>
);

const RecordRow: React.FC<{ record: MemoryRecord }> = ({ record }) => {
  const [expanded, setExpanded] = useState(false);
  const agent = recordAgent(record);
  return (
    <button
      type="button"
      onClick={() => setExpanded((v) => !v)}
      aria-expanded={expanded}
      className="w-full text-left rounded-xl !px-3 !py-2.5 transition-colors hover:bg-[var(--bg-rail-hover)]"
      style={{ border: '1px solid var(--border-color)' }}
    >
      <div
        className={`text-[13px] leading-relaxed ${expanded ? '' : 'line-clamp-2'}`}
        style={{ color: 'var(--text-primary)' }}
      >
        {record.content}
      </div>
      <div className="mt-1.5 flex flex-wrap items-center gap-1.5">
        <span
          className="w-1.5 h-1.5 rounded-full flex-shrink-0"
          style={{ backgroundColor: importanceColor(record.importance) }}
          aria-hidden="true"
        />
        <span className="text-[10.5px] tabular-nums" style={{ color: 'var(--text-muted)' }}>
          {record.importance.toFixed(2)}
        </span>
        {agent && (
          <span className="text-[10.5px]" style={{ color: 'var(--text-muted)' }}>
            · {agent}
          </span>
        )}
        <span className="text-[10.5px]" style={{ color: 'var(--text-muted)' }}>
          · {formatRelative(record.created_at)}
        </span>
        {(record.categories ?? []).slice(0, 4).map((c) => (
          <span
            key={c}
            className="text-[10px] rounded-full px-1.5 py-0.5"
            style={{ backgroundColor: 'var(--bg-secondary)', color: 'var(--text-secondary)' }}
          >
            {c}
          </span>
        ))}
      </div>
    </button>
  );
};

const MemoryPane: React.FC<{ runId: string }> = ({ runId }) => {
  const memory = useRunMemory(runId);
  const [view, setView] = useState<View>('graph');
  const [pinned, setPinned] = useState<Set<string>>(new Set());

  const categoryStats = useMemo(
    () =>
      [...memory.index.categories.values()].sort(
        (a, b) => b.count - a.count || b.avgImportance - a.avgImportance,
      ),
    [memory.index],
  );

  // Stable node array — a fresh identity every render would re-feed the graph
  // mid-simulation on every pin click / hover re-render.
  const graphNodes = useMemo(
    () =>
      categoryStats.map((c) => ({
        id: c.key,
        label: c.label,
        count: c.count,
        avgImportance: c.avgImportance,
      })),
    [categoryStats],
  );

  // Pinning graph nodes narrows the record list under the graph to records
  // mentioning a pinned concept (same interaction as the browser dialog).
  const visibleRecords = useMemo(() => {
    if (pinned.size === 0) return memory.records;
    const ids = new Set<string>();
    for (const key of pinned) {
      const stat = memory.index.categories.get(key);
      stat?.recordIds.forEach((id) => ids.add(id));
    }
    return memory.records.filter((r) => r.id && ids.has(r.id));
  }, [memory.records, memory.index, pinned]);

  const togglePin = (id: string) =>
    setPinned((prev) => {
      const next = new Set(prev);
      if (next.has(id)) next.delete(id);
      else next.add(id);
      return next;
    });

  if (memory.loading) {
    return (
      <div className="p-6 text-[13px]" style={{ color: 'var(--text-muted)' }}>
        Loading memory…
      </div>
    );
  }
  if (memory.error) {
    return (
      <div className="p-6 text-[13px]" style={{ color: 'var(--text-muted)' }}>
        Could not load memory: {memory.error}
      </div>
    );
  }

  return (
    <div className="mx-auto w-full max-w-4xl p-4 sm:p-5">
      {/* Stats + backend badge */}
      <div data-testid="memory-stats" className="flex flex-wrap items-center gap-2">
        <Stat label="Records" value={memory.records.length} />
        <Stat label="Concepts" value={memory.index.categories.size} />
        <Stat
          label="Avg importance"
          value={memory.records.length ? memory.index.avgImportance.toFixed(2) : '—'}
        />
        {memory.backend && memory.backend.toLowerCase() !== 'default' && (
          <span
            className="ml-auto text-[10.5px] rounded-full px-2 py-1"
            style={{ backgroundColor: 'var(--bg-secondary)', color: 'var(--text-muted)' }}
          >
            {memory.backend}
          </span>
        )}
      </div>

      {/* Mode + view switchers */}
      <div className="mt-3 flex flex-wrap items-center justify-between gap-2">
        <Segmented
          aria-label="Memory mode"
          value={memory.mode}
          options={[
            { id: 'saved', label: 'Saved' },
            { id: 'recalled', label: 'Recalled' },
          ]}
          onChange={(id) => memory.setMode(id as MemoryMode)}
        />
        <Segmented
          aria-label="Memory view"
          value={view}
          options={[
            { id: 'graph', label: 'Graph' },
            { id: 'records', label: 'Records' },
          ]}
          onChange={(id) => setView(id as View)}
        />
      </div>

      {memory.records.length === 0 ? (
        <div className="mt-6 text-[13px]" style={{ color: 'var(--text-muted)' }}>
          {memory.mode === 'recalled'
            ? 'This run recalled nothing from memory.'
            : 'This run saved nothing to memory.'}
        </div>
      ) : view === 'graph' ? (
        <div className="mt-3">
          <ConceptForceGraph
            nodes={graphNodes}
            edges={memory.edges}
            activeIds={pinned}
            onToggleNode={togglePin}
            importanceColor={importanceColor}
            height={380}
          />
          {pinned.size > 0 && (
            <div className="mt-3 flex flex-col gap-2">
              <span className="text-[11px] font-medium" style={{ color: 'var(--text-muted)' }}>
                Records mentioning pinned concepts
              </span>
              {visibleRecords.map((r, i) => (
                <RecordRow key={r.id ?? i} record={r} />
              ))}
            </div>
          )}
        </div>
      ) : (
        <div className="mt-3 flex flex-col gap-2">
          {visibleRecords.map((r, i) => (
            <RecordRow key={r.id ?? i} record={r} />
          ))}
        </div>
      )}
    </div>
  );
};

export default MemoryPane;
