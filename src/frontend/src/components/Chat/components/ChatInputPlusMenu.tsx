import React, { useState } from 'react';
import {
  Badge,
  Divider,
  IconButton,
  ListItemIcon,
  ListItemText,
  ListSubheader,
  Menu,
  MenuItem,
  Switch,
  Tooltip,
  Typography,
} from '@mui/material';
import {
  Add as AddIcon,
  AttachFile as AttachFileIcon,
  Check as CheckIcon,
  ChevronRight as ChevronRightIcon,
} from '@mui/icons-material';
import { useCrewExecutionStore, ReasoningConfig } from '../../../store/crewExecution';
import {
  ReasoningModelCatalogue,
  reasoningUnsupportedReason,
  useReasoningSupport,
} from '../../../hooks/global/useReasoningSupport';

/**
 * The composer's "+" menu — occasional actions and rarely-changed run settings,
 * so the input row stays a text field, a model and a send button.
 *
 * **What belongs in here, and what does not.** Claude's and Perplexity's "+"
 * menus hold one-shot ACTIONS (attach a file, add a connector); Claude keeps its
 * mode switch OUTSIDE the menu, visibly, and that is the right line. A menu
 * hides state, which is free for an action you perform and forget, and costly
 * for a setting that changes what the next run does. So:
 *
 * - the "Add files" ACTION moves in here, while the attached-file CHIPS stay in
 *   the composer (a chip is state that goes out with the next message);
 * - Process and Reasoning move in here because they are set once per workflow,
 *   but anything non-default raises a dot on the "+" — otherwise someone flips
 *   Hierarchical, forgets, and cannot see why later runs behave differently.
 *
 * **Rows, not nested dropdowns.** An MUI `Select` inside a `Menu` opens a second
 * popover on top of the first and renders as a heavy boxed field in a surface
 * that is otherwise a list. Every choice here is a short closed set, so each is
 * a row with a check — which is what a menu is for. The manager model is the
 * one open-ended list, so it opens a submenu instead of filling the sheet.
 *
 * Settings read and write ``useCrewExecutionStore`` directly, the same source
 * the left sidebar used, so there is exactly ONE control for each.
 */

export interface ChatInputPlusMenuProps {
  /** Opens the knowledge-file picker (the imperative handle on the uploader). */
  onAddFiles: () => void;
  /** Attaching needs an agent and a task on the canvas to wire the file into. */
  attachDisabled?: boolean;
  /** Why attaching is unavailable, shown on the disabled row. */
  attachDisabledReason?: string;
  /** Model catalogue — decides whether reasoning can do anything, and lists
   *  the manager models for the hierarchical process. */
  models: ReasoningModelCatalogue & Record<string, { name?: string } | undefined>;
  /**
   * The composer's currently selected model. Counted alongside the canvas
   * agents' models when deciding whether reasoning is available, because it is
   * what generation stamps on the agents it is about to create — without it the
   * menu blames a stale canvas agent for a model the user has just changed.
   */
  selectedModel?: string;
  /** The whole menu is unavailable while a run is in flight. */
  disabled?: boolean;
}

type ProcessType = 'sequential' | 'hierarchical' | 'parallel';

// Same set and wording as the left sidebar's picker — this replaces it, so a
// missing option here is a capability the user simply loses.
const PROCESSES: { value: ProcessType; label: string; hint: string }[] = [
  { value: 'sequential', label: 'Sequential', hint: 'Linear task execution' },
  { value: 'hierarchical', label: 'Hierarchical', hint: 'Manager-based delegation' },
  { value: 'parallel', label: 'Parallel', hint: 'Independent tasks run at once' },
];

const EFFORTS: { value: NonNullable<ReasoningConfig['reasoning_effort']>; label: string; hint: string }[] = [
  { value: 'low', label: 'Low', hint: 'Minimal thinking (fastest)' },
  { value: 'medium', label: 'Medium', hint: 'Balanced thinking' },
  { value: 'high', label: 'High', hint: 'Maximum thinking (slowest)' },
];

const SUBHEADER_SX = {
  fontSize: '0.65rem',
  fontWeight: 600,
  textTransform: 'uppercase' as const,
  letterSpacing: '0.6px',
  lineHeight: '28px',
  color: 'text.secondary',
  bgcolor: 'transparent',
};

const ROW_SX = { py: 0.5, minHeight: 34 };

/** A choice row: label, muted hint, and a check when active. */
const ChoiceRow: React.FC<{
  label: string;
  hint: string;
  selected: boolean;
  disabled?: boolean;
  onClick: () => void;
}> = ({ label, hint, selected, disabled, onClick }) => (
  <MenuItem selected={selected} disabled={disabled} onClick={onClick} sx={ROW_SX}>
    <ListItemIcon sx={{ minWidth: 28 }}>
      {selected && <CheckIcon sx={{ fontSize: 16 }} color="primary" />}
    </ListItemIcon>
    <ListItemText
      primary={label}
      secondary={hint}
      primaryTypographyProps={{ fontSize: '0.8rem' }}
      secondaryTypographyProps={{ fontSize: '0.68rem' }}
    />
  </MenuItem>
);

const ChatInputPlusMenu: React.FC<ChatInputPlusMenuProps> = ({
  onAddFiles,
  attachDisabled = false,
  attachDisabledReason,
  models,
  selectedModel,
  disabled = false,
}) => {
  const [anchorEl, setAnchorEl] = useState<null | HTMLElement>(null);
  const [managerAnchorEl, setManagerAnchorEl] = useState<null | HTMLElement>(null);
  const open = Boolean(anchorEl);

  const {
    processType,
    setProcessType,
    managerLLM,
    setManagerLLM,
    reasoningEnabled,
    setReasoningEnabled,
    reasoningConfig,
    setReasoningConfig,
  } = useCrewExecutionStore();

  const { agentModelNames, supported: reasoningSupported } = useReasoningSupport(models, selectedModel);

  // What the user changed away from the defaults, so hidden state stays
  // discoverable — the one real cost of moving settings into a menu.
  const hasNonDefault = processType !== 'sequential' || reasoningEnabled;

  const close = () => setAnchorEl(null);

  return (
    <>
      <Tooltip title="Files and run settings">
        <span>
          <IconButton
            aria-label="Files and run settings"
            aria-haspopup="menu"
            aria-expanded={open}
            size="small"
            disabled={disabled}
            onClick={(e) => setAnchorEl(e.currentTarget)}
            sx={{
              padding: '4px',
              color: 'text.secondary',
              '&:hover': { backgroundColor: 'action.hover', color: 'primary.main' },
            }}
          >
            <Badge color="primary" variant="dot" invisible={!hasNonDefault}>
              <AddIcon sx={{ fontSize: 18 }} />
            </Badge>
          </IconButton>
        </span>
      </Tooltip>

      <Menu
        anchorEl={anchorEl}
        open={open}
        onClose={close}
        anchorOrigin={{ vertical: 'top', horizontal: 'left' }}
        transformOrigin={{ vertical: 'bottom', horizontal: 'left' }}
        slotProps={{ paper: { sx: { minWidth: 264, maxWidth: 300, py: 0.5 } } }}
      >
        <Tooltip title={attachDisabled ? attachDisabledReason ?? '' : ''} placement="right">
          <span>
            <MenuItem
              disabled={attachDisabled}
              onClick={() => {
                close();
                onAddFiles();
              }}
              sx={ROW_SX}
            >
              <ListItemIcon sx={{ minWidth: 28 }}>
                <AttachFileIcon sx={{ fontSize: 17 }} />
              </ListItemIcon>
              <ListItemText primary="Add files" primaryTypographyProps={{ fontSize: '0.8rem' }} />
            </MenuItem>
          </span>
        </Tooltip>

        <Divider sx={{ my: 0.5 }} />
        <ListSubheader sx={SUBHEADER_SX}>Process</ListSubheader>

        {PROCESSES.map((process) => (
          <ChoiceRow
            key={process.value}
            label={process.label}
            hint={process.hint}
            selected={processType === process.value}
            onClick={() => setProcessType(process.value)}
          />
        ))}

        {/* An open-ended list, so it opens a SUBMENU rather than becoming one
            row per model in the main sheet — the same shape the reference "+"
            menus use for their long lists. Shown only for the hierarchical
            process: a hierarchical run with no manager is the failure this
            pairing exists to prevent. */}
        {processType === 'hierarchical' && (
          <MenuItem
            onClick={(e) => setManagerAnchorEl(e.currentTarget)}
            sx={ROW_SX}
          >
            <ListItemIcon sx={{ minWidth: 28 }} />
            <ListItemText
              primary="Manager model"
              secondary={managerLLM ? models[managerLLM]?.name || managerLLM : 'Default'}
              primaryTypographyProps={{ fontSize: '0.8rem' }}
              secondaryTypographyProps={{ fontSize: '0.68rem', noWrap: true }}
            />
            <ChevronRightIcon sx={{ fontSize: 18, color: 'text.secondary' }} />
          </MenuItem>
        )}

        <Divider sx={{ my: 0.5 }} />
        <ListSubheader sx={SUBHEADER_SX}>Reasoning</ListSubheader>

        <MenuItem
          onClick={() => setReasoningEnabled(!reasoningEnabled)}
          sx={{ ...ROW_SX, pr: 1 }}
        >
          <ListItemText
            primary="Agent reasoning"
            secondary="The model's own thinking budget"
            primaryTypographyProps={{ fontSize: '0.8rem' }}
            secondaryTypographyProps={{ fontSize: '0.68rem' }}
          />
          <Switch
            checked={reasoningEnabled}
            size="small"
            inputProps={{ 'aria-label': 'Agent reasoning' }}
            onClick={(e) => e.stopPropagation()}
            onChange={(e) => setReasoningEnabled(e.target.checked)}
          />
        </MenuItem>

        {reasoningEnabled &&
          EFFORTS.map((effort) => (
            <ChoiceRow
              key={effort.value}
              label={effort.label}
              hint={effort.hint}
              selected={(reasoningConfig.reasoning_effort ?? 'low') === effort.value}
              disabled={!reasoningSupported}
              onClick={() =>
                setReasoningConfig({
                  reasoning_effort: effort.value as ReasoningConfig['reasoning_effort'],
                })
              }
            />
          ))}

        {reasoningEnabled && !reasoningSupported && (
          <Typography
            variant="caption"
            sx={{ display: 'block', px: 2, pb: 1, fontSize: '0.65rem', color: 'text.secondary' }}
          >
            {reasoningUnsupportedReason(agentModelNames)}
          </Typography>
        )}
      </Menu>

      <Menu
        anchorEl={managerAnchorEl}
        open={Boolean(managerAnchorEl)}
        onClose={() => setManagerAnchorEl(null)}
        anchorOrigin={{ vertical: 'top', horizontal: 'right' }}
        transformOrigin={{ vertical: 'top', horizontal: 'left' }}
        slotProps={{ paper: { sx: { maxHeight: 320, minWidth: 220 } } }}
      >
        <ChoiceRow
          label="Default"
          hint="The crew's own model"
          selected={!managerLLM}
          onClick={() => {
            setManagerLLM('');
            setManagerAnchorEl(null);
          }}
        />
        {Object.entries(models).map(([key, model]) => (
          <ChoiceRow
            key={key}
            label={model?.name || key}
            hint=""
            selected={managerLLM === key}
            onClick={() => {
              setManagerLLM(key);
              setManagerAnchorEl(null);
            }}
          />
        ))}
      </Menu>
    </>
  );
};

export default ChatInputPlusMenu;
