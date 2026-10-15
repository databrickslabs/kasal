import { effortChoices } from '../../../../shared/components/EffortPicker';
import { useModelsStore } from '../../../../store/models';
import { EFFORT_LABELS, type EffortSettings, type EffortTier } from '../../../../types/workflow/effort';

interface Props {
  value: EffortSettings;
  onChange: (value: EffortSettings) => void;
  /** The edit's model key ('' = workspace default): its scale decides the levels. */
  model: string;
  disabled: boolean;
}

const TIERS = Object.keys(EFFORT_LABELS) as EffortTier[];

/** An edit-local choice, like the model: changing it does not change the chat's effort. */
export default function DeckEffortPicker({ value, onChange, model, disabled }: Props) {
  const models = useModelsStore((s) => s.models);
  // The composer's rule: the model's native levels, or the runtime-only levels
  // when it has none (or is the workspace default, whose scale is not known here).
  const choices = effortChoices(models.filter((item) => item.key === model));
  // A level the model lacks stays selectable; the run uses its nearest native level.
  const tiers = TIERS.filter((tier) => tier === value.tier || choices.includes(tier));

  return (
    <label className="mr-2 inline-flex items-center gap-2 text-xs" style={{ color: 'var(--text-secondary)' }}>
      Effort
      <select
        aria-label="Slide edit effort"
        value={value.tier}
        disabled={disabled}
        onChange={(event) => onChange({ tier: event.target.value as EffortTier })}
        // Arrow keys select a level; Escape closes the native menu, not the studio.
        onKeyDown={(event) => event.stopPropagation()}
        className="rounded-md border px-2 py-1 focus:ring-2 focus:ring-blue-500/60 disabled:opacity-40"
        style={{ background: 'var(--bg-input)', color: 'var(--text-primary)', borderColor: 'var(--border-color)' }}
      >
        {tiers.map((tier) => <option key={tier} value={tier}>{EFFORT_LABELS[tier]}</option>)}
      </select>
    </label>
  );
}
