import { ChevronDown } from 'lucide-react';
import { formatModelLabel } from '../../../../utils/modelDisplay';
import { useModelsStore } from '../../../../store/models';

interface Props {
  value: string;
  onChange: (value: string) => void;
  disabled: boolean;
}

/** An edit-local choice: changing it does not change the chat or builder model. */
export default function DeckModelPicker({ value, onChange, disabled }: Props) {
  // The shared live list: subscribing loads it (builder mode can open the
  // studio without ever mounting ChatWorkspace) and keeps it current.
  const models = useModelsStore((s) => s.models);
  const loadError = useModelsStore((s) => s.error !== null);

  return (
    <label className={`kasal-deck-pick text-xs${disabled ? ' opacity-40' : ''}`}>
      Model
      <select
        aria-label="Slide edit model"
        value={value}
        disabled={disabled}
        onChange={(event) => onChange(event.target.value)}
        // Arrow keys select a model; Escape closes the native menu, not the studio.
        onKeyDown={(event) => event.stopPropagation()}
        className="min-w-0 max-w-[12rem] truncate"
      >
        <option value="">Workspace default</option>
        {value && !models.some((item) => item.key === value) && <option value={value}>{value}</option>}
        {models.map((item) => <option key={item.key} value={item.key}>{formatModelLabel(item.name || item.key)}</option>)}
      </select>
      <ChevronDown size={12} aria-hidden />
      {loadError && <span role="status">Could not refresh models.</span>}
    </label>
  );
}
