import { useState } from 'react';
import { ExternalLink } from 'lucide-react';
import { apiClient } from '../../../../shared/api/client';
import { useMLflowEnabled } from '../../../../hooks/global/useMLflowEnabled';

/** Resolve a run's trace on demand; reserve a tab before the asynchronous lookup. */
export default function MLflowRunAction({ executionId, disabled }: { executionId: string; disabled?: boolean }) {
  const enabled = useMLflowEnabled();
  const [opening, setOpening] = useState(false);
  const [error, setError] = useState(false);
  const [link, setLink] = useState<string>();
  if (!enabled) return null;

  const open = async () => {
    const tab = window.open('about:blank', '_blank');
    if (tab) tab.opener = null;
    setOpening(true); setError(false); setLink(undefined);
    try {
      const { data } = await apiClient.get<{ url?: string; experiment_id?: string }>('/mlflow/trace-link', { params: { job_id: executionId } });
      if (!data.url || !data.experiment_id || !/^https?:\/\//i.test(data.url)) throw new Error('Trace unavailable');
      if (tab && !tab.closed) tab.location.replace(data.url);
      else setLink(data.url);
    } catch {
      tab?.close(); setError(true);
    } finally { setOpening(false); }
  };

  return <>
    <button type="button" onClick={() => void open()} disabled={disabled || opening}
      className="flex items-center gap-1.5 px-2.5 py-1.5 rounded-lg text-xs font-medium transition-all hover:opacity-80 disabled:opacity-50 disabled:cursor-not-allowed"
      style={{ color: 'var(--text-secondary)', background: 'transparent', border: 'none' }}>
      <ExternalLink size={14} />{opening ? 'Opening MLflow…' : 'MLflow'}
    </button>
    {link && <a href={link} target="_blank" rel="noopener noreferrer" className="text-xs underline" style={{ color: 'var(--text-secondary)' }}>Open MLflow trace</a>}
    {error && <span role="alert" className="text-xs" style={{ color: 'var(--text-secondary)' }}>The MLflow trace link is unavailable. Try again shortly.</span>}
  </>;
}
