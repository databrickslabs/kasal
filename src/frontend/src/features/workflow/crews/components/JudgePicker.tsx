import React, { useState } from 'react';
import {
  Box,
  Button,
  Checkbox,
  Chip,
  CircularProgress,
  FormControlLabel,
  IconButton,
  ListItemText,
  Menu,
  MenuItem,
  Tooltip,
  Typography,
} from '@mui/material';
import AddIcon from '@mui/icons-material/Add';
import AutoFixHighIcon from '@mui/icons-material/AutoFixHigh';
import DeleteOutlineIcon from '@mui/icons-material/DeleteOutline';
import EditIcon from '@mui/icons-material/Edit';
import GavelIcon from '@mui/icons-material/Gavel';
import OpenInNewIcon from '@mui/icons-material/OpenInNew';
import { useTranslation } from 'react-i18next';
import {
  BuiltinJudge,
  JudgeRegistryInfo,
  LLMJudge,
} from '../../../../api/config/PromptOptimizationService';
import { BuiltinJudgesState } from '../hooks/useBuiltinJudges';

interface JudgePickerProps {
  judgeRegistry: JudgeRegistryInfo | null;
  assignedJudges: LLMJudge[];
  libraryJudges: LLMJudge[];
  /** Registry name of the judge being aligned, if any. */
  aligning: string | null;
  showJudgeForm: boolean;
  onToggleJudgeForm: () => void;
  onEdit: (judge: LLMJudge) => void;
  onUnassign: (fullName: string) => void;
  onAlign: (judge: LLMJudge) => void;
  onAssign: (name: string) => void;
  onDeleteLibrary: (judge: LLMJudge) => void;
  builtin: BuiltinJudgesState;
}

const GROUP_TITLE_SX = { fontWeight: 600, display: 'block', mt: 2, mb: 0.5 } as const;

type Translate = (key: string, options: { defaultValue: string }) => string;

/** Why a label judge is disabled: the labels it reads are not there yet. */
const missingLabelsReason = (judge: BuiltinJudge, t: Translate): string =>
  (judge.label_fields || []).includes('guidelines')
    ? t('optimize.judges.needsReviewNotes', {
        defaultValue: 'Needs review notes: grade past answers and say what they should contain',
      })
    : t('optimize.judges.needsExpected', {
        defaultValue: 'Add expected facts or an expected answer below',
      });

/**
 * The judges that score a crew optimization run, in two groups: the user's
 * own judges from the MLflow Prompt Registry, and MLflow's built-in judges,
 * which run on demand with the run's judge model and are never registered.
 */
const JudgePicker: React.FC<JudgePickerProps> = ({
  judgeRegistry,
  assignedJudges,
  libraryJudges,
  aligning,
  showJudgeForm,
  onToggleJudgeForm,
  onEdit,
  onUnassign,
  onAlign,
  onAssign,
  onDeleteLibrary,
  builtin,
}) => {
  const { t } = useTranslation();
  const [assignAnchor, setAssignAnchor] = useState<HTMLElement | null>(null);

  return (
    <Box>
      <Typography variant="caption" color="text.secondary" sx={GROUP_TITLE_SX}>
        {t('optimize.judges.yourJudges', { defaultValue: 'Your judges' })}
      </Typography>
      <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, flexWrap: 'wrap' }}>
        <GavelIcon sx={{ fontSize: 16, color: 'text.secondary' }} />
        <Typography variant="caption" color="text.secondary" sx={{ mr: 0.5 }}>
          Scoring criteria
          {judgeRegistry?.location && (
            <>
              {' · '}
              {judgeRegistry.url ? (
                <a
                  href={judgeRegistry.url}
                  target="_blank"
                  rel="noopener noreferrer"
                  style={{ color: 'inherit' }}
                >
                  {judgeRegistry.location}
                </a>
              ) : (
                judgeRegistry.location
              )}
            </>
          )}
        </Typography>
        <Chip
          size="small"
          variant="outlined"
          label="Quality (Kasal)"
          title="Grades every deliverable 0-10 on completeness, specificity, and fidelity to the expected outputs"
        />
        {assignedJudges.map((j) => (
          <Box key={j.full_name || j.name} sx={{ display: 'inline-flex', alignItems: 'center', gap: 0.25 }}>
            <Chip
              size="small"
              color="primary"
              variant="outlined"
              clickable
              label={j.name}
              title={`${j.instructions || ''}\n\nClick to edit this judge.`}
              onClick={() => onEdit(j)}
              onDelete={() => onUnassign(j.full_name || j.name)}
            />
            <Tooltip
              title={`Align "${j.name}" to the grades you gave with it selected: it learns where it disagreed with you and scores like you from then on`}
            >
              <span>
                <IconButton
                  size="small"
                  aria-label={`Align ${j.name}`}
                  disabled={aligning !== null}
                  onClick={() => onAlign(j)}
                >
                  {aligning === (j.full_name || j.name) ? (
                    <CircularProgress size={14} />
                  ) : (
                    <AutoFixHighIcon sx={{ fontSize: 16 }} />
                  )}
                </IconButton>
              </span>
            </Tooltip>
            {j.url && (
              <Tooltip title="Open this judge in MLflow">
                <IconButton
                  size="small"
                  component="a"
                  href={j.url}
                  target="_blank"
                  rel="noopener noreferrer"
                  aria-label={`Open ${j.name} in MLflow`}
                >
                  <OpenInNewIcon sx={{ fontSize: 15 }} />
                </IconButton>
              </Tooltip>
            )}
          </Box>
        ))}
        {libraryJudges.length > 0 && (
          <>
            <Chip
              size="small"
              clickable
              variant="outlined"
              icon={<AddIcon sx={{ fontSize: 16 }} />}
              label="Assign"
              onClick={(e) => setAssignAnchor(e.currentTarget)}
            />
            <Menu anchorEl={assignAnchor} open={Boolean(assignAnchor)} onClose={() => setAssignAnchor(null)}>
              {libraryJudges.map((j) => (
                <MenuItem
                  key={j.full_name || j.name}
                  onClick={() => {
                    setAssignAnchor(null);
                    onAssign(j.name);
                  }}
                >
                  <ListItemText
                    primary={<Typography variant="body2">{j.name}</Typography>}
                    secondary={
                      j.instructions ? (
                        <Typography
                          variant="caption"
                          color="text.secondary"
                          sx={{
                            display: 'block',
                            maxWidth: 320,
                            overflow: 'hidden',
                            textOverflow: 'ellipsis',
                            whiteSpace: 'nowrap',
                          }}
                        >
                          {j.instructions}
                        </Typography>
                      ) : undefined
                    }
                  />
                  <Tooltip title="Edit judge">
                    <IconButton
                      size="small"
                      edge="end"
                      sx={{ ml: 1 }}
                      onClick={(e) => {
                        e.stopPropagation();
                        setAssignAnchor(null);
                        onEdit(j);
                      }}
                    >
                      <EditIcon sx={{ fontSize: 16 }} />
                    </IconButton>
                  </Tooltip>
                  <Tooltip title="Delete from library">
                    <IconButton
                      size="small"
                      edge="end"
                      onClick={(e) => {
                        e.stopPropagation();
                        setAssignAnchor(null);
                        onDeleteLibrary(j);
                      }}
                    >
                      <DeleteOutlineIcon sx={{ fontSize: 16 }} />
                    </IconButton>
                  </Tooltip>
                </MenuItem>
              ))}
            </Menu>
          </>
        )}
        <Button size="small" onClick={onToggleJudgeForm}>
          {showJudgeForm ? 'Cancel' : '+ Custom criteria'}
        </Button>
      </Box>

      {builtin.judges.length > 0 && (
        <Box data-testid="builtin-judges">
          <Typography variant="caption" color="text.secondary" sx={GROUP_TITLE_SX}>
            {t('optimize.judges.builtinTitle', { defaultValue: 'MLflow built-in judges' })}
          </Typography>
          <Typography variant="caption" color="text.secondary" display="block" sx={{ mb: 0.5 }}>
            {t('optimize.judges.builtinHint', {
              defaultValue:
                'Optional. They use the judge model above and run on demand; nothing is registered or scheduled.',
            })}
          </Typography>
          {builtin.judges.map((judge) => {
            const enabled = builtin.isEnabled(judge);
            return (
              <Box key={judge.id} sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
                <FormControlLabel
                  sx={{ mr: 0 }}
                  disabled={!enabled}
                  control={
                    <Checkbox
                      size="small"
                      checked={builtin.selected.includes(judge.id)}
                      onChange={() => builtin.toggle(judge.id)}
                    />
                  }
                  label={<Typography variant="body2">{judge.label}</Typography>}
                />
                <Chip
                  size="small"
                  variant="outlined"
                  color={judge.role === 'gate' ? 'warning' : judge.needs_labels ? 'info' : 'default'}
                  label={
                    judge.role === 'gate'
                      ? t('optimize.judges.gate', { defaultValue: 'gate' })
                      : judge.needs_labels
                        ? t('optimize.judges.needsLabels', { defaultValue: 'needs labels' })
                        : t('optimize.judges.noLabels', { defaultValue: 'no labels needed' })
                  }
                />
                <Typography variant="caption" color="text.secondary" noWrap sx={{ minWidth: 0 }}>
                  {enabled ? judge.description : missingLabelsReason(judge, t)}
                </Typography>
              </Box>
            );
          })}
        </Box>
      )}
    </Box>
  );
};

export default JudgePicker;
