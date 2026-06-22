import React, { useMemo } from 'react';
import type { TransientPreviewItem } from '../../store/executionStore';
import { UiSurface, applyConfiguredTheme, buildResultsSurface } from '../../utils/uiDocument';
import { useWorkspaceThemes } from '../../hooks/useWorkspaceThemes';
import UiRenderer from './UiRenderer';
import ShowContextToggle from './ShowContextToggle';

/**
 * Compose the live (transient) answers into an A2UI surface — the query/topic as
 * a heading over the answer body — via the shared {@link buildResultsSurface}, so
 * the intermediate results render through the SAME brand-consistent
 * {@link UiRenderer} as the final deliverable (and identically to the Jobs
 * run-activity "Responses" view), instead of ad-hoc grey cards.
 *
 * Pure + exported for testing. Nothing here is persisted: the surface is rebuilt
 * from the in-memory transient feed on each render and discarded when the run
 * ends.
 */
export function buildTransientSurface(items: TransientPreviewItem[]): UiSurface {
  return buildResultsSurface(
    items.map((it) => ({ title: it.sublabel || it.label, body: it.detail })),
  );
}

interface PreviewActivityProps {
  items: TransientPreviewItem[];
}

/**
 * Transient, NON-PERSISTED live view shown in the preview pane WHILE a run is
 * building its deliverable. It surfaces the answers coming back (tool results /
 * findings) rendered through A2UI so they read like a polished document — then
 * it is replaced by the real deliverable when {@link PreviewPanel} takes over.
 *
 * Nothing here is saved to previewHistory or IndexedDB: the items live only in
 * `executionStore.transientPreview` for the duration of the run.
 */
const PreviewActivity: React.FC<PreviewActivityProps> = ({ items }) => {
  // Re-resolve the theme from the workspace palettes (source of truth), exactly
  // like PreviewPanel, so the transient view matches the final deliverable.
  const workspaceThemes = useWorkspaceThemes();
  const surface = useMemo(
    () => applyConfiguredTheme(buildTransientSurface(items), workspaceThemes),
    [items, workspaceThemes],
  );

  return (
    <aside
      className="flex flex-col h-full"
      style={{
        flex: '1 1 50%',
        minWidth: '300px',
        backgroundColor: 'var(--bg-primary)',
        borderLeft: '1px solid var(--border-color)',
      }}
      aria-busy="true"
      aria-label="Live results"
    >
      {/* Header — mirrors PreviewPanel's so the swap to the live renderer is seamless */}
      <div
        className="flex items-center gap-2 px-4 py-3 flex-shrink-0"
        style={{ borderBottom: '1px solid var(--border-color)' }}
      >
        <span
          className="w-1.5 h-1.5 rounded-full animate-pulse"
          style={{ backgroundColor: 'var(--accent)' }}
        />
        <span className="text-sm font-medium" style={{ color: 'var(--text-primary)' }}>
          Working…
        </span>
        <span className="text-[11px]" style={{ color: 'var(--text-muted)' }}>
          gathering results — running agent
        </span>
        <div className="ml-auto">
          <ShowContextToggle />
        </div>
      </div>

      {/* The live answers, rendered through the brand-consistent A2UI renderer. */}
      <div className="flex-1 overflow-auto">
        <UiRenderer surface={surface} />
      </div>
    </aside>
  );
};

export default PreviewActivity;
