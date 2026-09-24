import { formatModelLabel } from '../../../../utils/modelDisplay';
import React, { useEffect, useState } from 'react';
import { fetchEnabledModels } from '../../api/models';
import { useAppStore } from '../../store/appStore';

interface Props {
  value: string;
  onChange: (value: string) => void;
  disabled: boolean;
}

/** An edit-local choice: changing it does not change the chat or builder model. */
export default function DeckModelPicker({ value, onChange, disabled }: Props) {
  const [models, setModels] = useState(() => useAppStore.getState().models);
  const [loadError, setLoadError] = useState(false);
  useEffect(() => {
    let active = true;
    // Builder mode can open the studio without ever mounting ChatWorkspace.
    void fetchEnabledModels().then((items) => {
      if (active) {
        if (Array.isArray(items)) setModels(items);
        else setLoadError(true);
      }
    }).catch(() => {
      if (active) setLoadError(true);
    });
    return () => { active = false; };
  }, []);

  return (
    <label className="mr-2 inline-flex max-w-full items-center gap-2 text-xs" style={{ color: 'var(--text-secondary)' }}>
      Model
      <select
        aria-label="Slide edit model"
        value={value}
        disabled={disabled}
        onChange={(event) => onChange(event.target.value)}
        // Arrow keys select a model; Escape closes the native menu, not the studio.
        onKeyDown={(event) => event.stopPropagation()}
        className="min-w-0 max-w-[16rem] rounded-md border px-2 py-1 focus:ring-2 focus:ring-blue-500/60 disabled:opacity-40"
        style={{ background: 'var(--bg-input)', color: 'var(--text-primary)', borderColor: 'var(--border-color)' }}
      >
        <option value="">Workspace default</option>
        {value && !models.some((item) => item.key === value) && <option value={value}>{value}</option>}
        {models.map((item) => <option key={item.key} value={item.key}>{formatModelLabel(item.name || item.key)}</option>)}
      </select>
      {loadError && <span role="status">Could not refresh models.</span>}
    </label>
  );
}
