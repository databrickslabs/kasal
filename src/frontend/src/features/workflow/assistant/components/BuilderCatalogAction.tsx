import { useMemo, useState } from 'react';
import { Bookmark } from 'lucide-react';
import { usePermissionStore } from '../../../../store/permissions';
import { useBuilderCanvasStore } from '../../../../app/sessions/builderCanvasStore';
import { useThemeStore } from '../../../../store/theme';
import { useAppStore } from '../../../chat/store/appStore';
import { saveCanvasToCatalog } from '../utils/saveCanvasToCatalog';

/** Direct catalog saving for generated crew and flow plans. */
export default function BuilderCatalogAction({ flow, suggestedName = '' }: { flow: boolean; suggestedName?: string }) {
  const dark = useThemeStore(state => state.isDarkMode);
  const [saving, setSaving] = useState(false);
  const tab = useBuilderCanvasStore(state => state.canvases.find(item => item.id === state.activeCanvasId));
  const revision = useMemo(() => JSON.stringify({
    id: tab?.id,
    nodes: (flow ? tab?.flowNodes : tab?.nodes)?.map(({ id, type, data }) => ({ id, type, data })),
    edges: (flow ? tab?.flowEdges : tab?.edges)?.map(({ id, source, target, sourceHandle, targetHandle, data }) => ({ id, source, target, sourceHandle, targetHandle, data })),
    config: tab?.executionConfig,
  }), [flow, tab]);
  const [saved, setSaved] = useState<{ name: string; revision: string } | null>(null);
  const savedName = saved?.revision === revision ? saved.name : '';
  const [error, setError] = useState('');
  const canSaveCrew = usePermissionStore(s => s.allowAgentBuilder && s.userRole !== 'operator');
  const canSaveFlow = usePermissionStore(s => s.allowFlowBuilder && s.userRole !== 'operator');
  const save = async () => {
    if (saving) return;
    setSaving(true);
    setError('');
    try {
      const saved = await saveCanvasToCatalog(flow, suggestedName);
      setSaved({ name: saved.name, revision });
      void useAppStore.getState().loadCatalog();
    } catch (error) {
      const status = (error as { response?: { status?: number } }).response?.status;
      setError(status === 409 ? 'A different item already uses this name. Rename the canvas or use the catalog to open the existing item.' : error instanceof Error ? error.message : 'Could not save this workload. Please try again.');
    } finally { setSaving(false); }
  };
  return <div className="kasal-chat-root" data-theme={dark ? 'dark' : 'light'}>
        {(flow ? canSaveFlow : canSaveCrew) && <button type="button" onClick={() => void save()} disabled={saving || Boolean(savedName)} title={savedName ? `Saved as “${savedName}”` : `Save the current ${flow ? 'flow' : 'crew'} to the catalog`}
          className="flex items-center gap-1.5 px-2.5 py-1.5 rounded-lg text-xs font-medium transition-all hover:opacity-80 disabled:opacity-50 disabled:cursor-not-allowed"
          style={{ color: 'var(--text-secondary)', backgroundColor: 'transparent', border: 'none' }}>
          <Bookmark size={14} fill={savedName ? 'currentColor' : 'none'} />{saving ? 'Saving…' : savedName ? 'Saved to catalog' : 'Save to catalog'}
        </button>}
    {error && <p role="alert" style={{ color: 'var(--text-secondary)', fontSize: 12, margin: '4px 10px' }}>{error}</p>}
  </div>;
}
