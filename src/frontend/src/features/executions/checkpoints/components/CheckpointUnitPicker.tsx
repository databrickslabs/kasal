import React from 'react';
import {
  Box,
  Chip,
  FormControlLabel,
  Radio,
  RadioGroup,
  Tooltip,
  Typography,
} from '@mui/material';
import ChangeCircleIcon from '@mui/icons-material/ChangeCircle';
import CheckCircleIcon from '@mui/icons-material/CheckCircle';
import WarningAmberIcon from '@mui/icons-material/WarningAmber';

/**
 * One completed unit of work, as this picker needs it.
 *
 * A unit is a TASK for a crew run and a CREW for a flow run. Both callers map
 * their own shape onto this rather than each carrying their own copy of the
 * idea — that duplication is what this component exists to remove.
 */
export interface PickableUnit {
  /** Value submitted when this unit is chosen. */
  key: string;
  name: string | null;
  outputPreview?: string | null;
  /** The stored output was capped. Shown so reduced fidelity is never silent. */
  truncated?: boolean;
  /**
   * Whether a resume would replay this unit rather than re-run it.
   *
   * `undefined`/`null` is "cannot tell" and renders as neither — an old
   * checkpoint has no basis for the claim, and showing it as "will be reused"
   * would promise something run time may refuse.
   */
  willRestore?: boolean | null;
}

interface CheckpointUnitPickerProps {
  units: PickableUnit[];
  /** Empty string means "the default option", i.e. no explicit rewind. */
  value: string;
  onChange: (value: string) => void;
  /** Label for the leading option that resumes without rewinding. */
  defaultOptionLabel: React.ReactNode;
  /** Wording for a unit option. Callers phrase this differently on purpose. */
  renderUnitLabel: (unit: PickableUnit) => string;
  previewLength?: number;
  hideDefault?: boolean;
}

/**
 * Choose where a resumed run picks up.
 *
 * Extracted from the flow resume dialog and the run-detail checkpoint dialog,
 * which had grown near-identical copies of it. The two callers keep their own
 * wording — one is a pre-launch gate, the other inspects a failed run — so the
 * labels are injected rather than assumed here.
 */
const CheckpointUnitPicker: React.FC<CheckpointUnitPickerProps> = ({
  units,
  value,
  onChange,
  defaultOptionLabel,
  renderUnitLabel,
  previewLength = 60,
  hideDefault = false,
}) => (
  <RadioGroup value={value} onChange={(e) => onChange(e.target.value)}>
    {!hideDefault && <FormControlLabel
      value=""
      control={<Radio size="small" />}
      label={
        typeof defaultOptionLabel === 'string' ? (
          <Typography variant="body2">{defaultOptionLabel}</Typography>
        ) : (
          defaultOptionLabel
        )
      }
    />}
    {units.map((unit, index) => (
      <FormControlLabel
        key={unit.key}
        value={unit.key}
        sx={{ mx: 0, my: 0.5, p: 1, borderRadius: 3, alignItems: 'flex-start', bgcolor: value === unit.key ? 'action.selected' : 'transparent', '& .MuiFormControlLabel-label': { flex: 1, minWidth: 0 } }}
        control={<Radio size="small" sx={{ color: 'text.secondary', '&.Mui-checked': { color: 'text.primary' } }} />}
        label={
          <Box sx={{ display: 'flex', alignItems: 'center', flexWrap: 'wrap', gap: 1, py: 0.75 }}>
            {unit.willRestore === false ? (
              <Tooltip title="This unit changed since the run, or comes after one that did — it will re-run">
                <ChangeCircleIcon color="warning" sx={{ fontSize: 16 }} />
              </Tooltip>
            ) : (
              <CheckCircleIcon sx={{ fontSize: 16, color: 'text.secondary' }} />
            )}
            <Typography
              variant="body2"
              color={unit.willRestore === false ? 'text.secondary' : undefined}
            >
              {renderUnitLabel(unit)}
            </Typography>
            {unit.willRestore === false && (
              <Chip
                label="will re-run"
                size="small"
                color="warning"
                variant="outlined"
                sx={{ height: 20, fontSize: '0.7rem' }}
              />
            )}
            <Chip
              label={`${index + 1}`}
              size="small"
              variant="outlined"
              sx={{ height: 20, fontSize: '0.7rem' }}
            />
            {unit.truncated && (
              <Tooltip title="This output was capped when stored">
                <WarningAmberIcon color="warning" sx={{ fontSize: 16 }} />
              </Tooltip>
            )}
            {unit.outputPreview && (
              <Tooltip title={unit.outputPreview}>
                <Typography
                  variant="caption"
                  color="text.secondary"
                  sx={{
                    width: '100%',
                    minWidth: 0,
                    overflow: 'hidden',
                    textOverflow: 'ellipsis',
                    whiteSpace: 'nowrap',
                  }}
                >
                  {unit.outputPreview.substring(0, previewLength)}
                </Typography>
              </Tooltip>
            )}
          </Box>
        }
      />
    ))}
  </RadioGroup>
);

export default CheckpointUnitPicker;
