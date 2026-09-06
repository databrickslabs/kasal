import React, { useEffect, useRef, useState } from 'react';
import { Badge, Box, CircularProgress, Divider, IconButton, Menu, MenuItem, Tooltip, Typography } from '@mui/material';
import { Add as AddIcon, ArrowBack as BackIcon, AttachFile as AttachFileIcon, Check as CheckIcon, ChevronRight as ChevronRightIcon } from '@mui/icons-material';
import { useCrewExecutionStore, ReasoningConfig } from '../../../../store/crewExecution';
import { ReasoningModelCatalogue, reasoningUnsupportedReason, useReasoningSupport } from '../../../../hooks/global/useReasoningSupport';

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
  onModelChange?: (model: string) => void;
  modelLabels?: Record<string, string>;
  loadingModels?: boolean;
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

type Section = 'settings' | 'models' | 'process' | 'manager' | 'reasoning';
const TITLES: Record<Section, string> = {
  settings: 'Files and run settings', models: 'Model', process: 'Process type',
  manager: 'Manager model', reasoning: 'Agent reasoning',
};
const ROW_SX = {
  minHeight: 42, px: 1.5, py: 1, gap: 1.5, borderRadius: '10px',
  fontSize: 13, '&.Mui-selected': { bgcolor: 'action.selected' },
};

/** Match Chat: one baseline, label left, current value and chevron right. */
function SettingsRow({ label, value, onClick }: { label: string; value: string; onClick: () => void }) {
  return <MenuItem aria-label={label} onClick={onClick}
    onKeyDown={event => { if (event.key === 'ArrowRight') { event.preventDefault(); onClick(); } }} sx={ROW_SX}>
    <Typography sx={{ fontSize: 13, fontWeight: 500, flexShrink: 0 }}>{label}</Typography>
    <Box sx={{ ml: 'auto', minWidth: 0, display: 'flex', alignItems: 'center', gap: 0.75, color: 'text.secondary' }}>
      <Typography noWrap title={value} sx={{ fontSize: 12, maxWidth: 150 }}>{value}</Typography>
      <ChevronRightIcon sx={{ fontSize: 16, flexShrink: 0 }} />
    </Box>
  </MenuItem>;
}

function ChoiceRow({ label, hint, selected, disabled, onClick }: {
  label: string; hint?: string; selected: boolean; disabled?: boolean; onClick: () => void;
}) {
  return <MenuItem role="menuitemradio" aria-checked={selected} selected={selected} disabled={disabled} onClick={onClick} sx={ROW_SX}>
    <Box sx={{ minWidth: 0, flex: 1 }}>
      <Typography sx={{ fontSize: 13, fontWeight: 500, whiteSpace: 'normal', overflowWrap: 'anywhere' }}>{label}</Typography>
      {hint && <Typography sx={{ fontSize: 12, color: 'text.secondary', whiteSpace: 'normal', mt: 0.25 }}>{hint}</Typography>}
    </Box>
    {selected && <CheckIcon sx={{ fontSize: 16, color: 'text.primary', flexShrink: 0 }} />}
  </MenuItem>;
}

/** Sub-panels replace the root rows, keeping every choice above the composer
 * without opening another popover that could be clipped by the canvas edge. */
const ChatInputPlusMenu: React.FC<ChatInputPlusMenuProps> = ({
  onAddFiles, attachDisabled = false, attachDisabledReason, models, selectedModel,
  onModelChange, modelLabels = {}, loadingModels = false, disabled = false,
}) => {
  const [anchorEl, setAnchorEl] = useState<HTMLElement | null>(null);
  const [section, setSection] = useState<Section>('settings');
  const listRef = useRef<HTMLUListElement>(null);
  const open = Boolean(anchorEl);
  const { processType, setProcessType, managerLLM, setManagerLLM, reasoningEnabled,
    setReasoningEnabled, reasoningConfig, setReasoningConfig } = useCrewExecutionStore();
  const { agentModelNames, supported: reasoningSupported } = useReasoningSupport(models, selectedModel);
  const hasNonDefault = processType !== 'sequential' || reasoningEnabled;
  const effort = reasoningConfig.reasoning_effort ?? 'low';
  const modelName = (key: string) => modelLabels[key] || models[key]?.name || key || 'Default';
  const close = () => { setAnchorEl(null); setSection('settings'); };
  const pick = (apply: () => void) => { apply(); setSection('settings'); };

  // Keep keyboard users in the active panel when the focused root row unmounts.
  useEffect(() => {
    if (open) listRef.current?.querySelector<HTMLElement>('[role="menuitem"]')?.focus();
  }, [section, open]);
  useEffect(() => { if (disabled) setAnchorEl(null); }, [disabled]);

  const modelRows = (manager: boolean) => loadingModels
    ? [<MenuItem key="loading" disabled sx={ROW_SX}><CircularProgress size={14} />Loading models…</MenuItem>]
    : [
      ...(manager ? [<ChoiceRow key="default" label="Default" hint="Use the crew’s model" selected={!managerLLM} onClick={() => pick(() => setManagerLLM(''))} />] : []),
      ...Object.keys(models).map(key => <ChoiceRow key={key} label={modelName(key)} selected={(manager ? managerLLM : selectedModel) === key}
        onClick={() => pick(() => manager ? setManagerLLM(key) : onModelChange?.(key))} />),
      ...(Object.keys(models).length ? [] : [<MenuItem key="empty" disabled sx={ROW_SX}>No models available</MenuItem>]),
    ];

  const rootRows = [
    ...(onModelChange ? [<SettingsRow key="models" label="Model" value={loadingModels ? 'Loading models…' : modelName(selectedModel || '')} onClick={() => setSection('models')} />] : []),
    <SettingsRow key="process" label="Process type" value={PROCESSES.find(p => p.value === processType)?.label || 'Sequential'} onClick={() => setSection('process')} />,
    ...(processType === 'hierarchical' ? [<SettingsRow key="manager" label="Manager model" value={modelName(managerLLM || '')} onClick={() => setSection('manager')} />] : []),
    <SettingsRow key="reasoning" label="Agent reasoning" value={reasoningEnabled ? EFFORTS.find(e => e.value === effort)?.label || 'Low' : 'Off'} onClick={() => setSection('reasoning')} />,
    <Divider key="divider" sx={{ mx: 1.5, my: 0.5, opacity: 0.5 }} />,
    <Tooltip key="files" describeChild title={attachDisabled ? attachDisabledReason || '' : ''} placement="top">
      <MenuItem aria-disabled={attachDisabled} onClick={() => { if (!attachDisabled) { close(); onAddFiles(); } }}
        sx={{ ...ROW_SX, ...(attachDisabled ? { color: 'text.disabled', cursor: 'default' } : {}) }}>
        <Typography sx={{ fontSize: 13, fontWeight: 500 }}>Add files</Typography>
        <AttachFileIcon sx={{ ml: 'auto', fontSize: 16, color: 'text.secondary' }} />
      </MenuItem>
    </Tooltip>,
  ];
  const optionRows = section === 'models' ? modelRows(false)
    : section === 'manager' ? modelRows(true)
    : section === 'process' ? PROCESSES.map(process => <ChoiceRow key={process.value} label={process.label} hint={process.hint}
      selected={processType === process.value} onClick={() => pick(() => setProcessType(process.value))} />)
    : [
      <ChoiceRow key="off" label="Off" hint="Use the model’s default thinking behavior" selected={!reasoningEnabled} onClick={() => pick(() => setReasoningEnabled(false))} />,
      ...EFFORTS.map(option => <ChoiceRow key={option.value} label={option.label} hint={option.hint}
        selected={reasoningEnabled && effort === option.value} disabled={!reasoningSupported}
        onClick={() => pick(() => { setReasoningConfig({ reasoning_effort: option.value }); setReasoningEnabled(true); })} />),
      ...(!reasoningSupported ? [<Typography key="unsupported" role="note" sx={{ px: 1.5, py: 1, fontSize: 12, color: 'text.secondary' }}>{reasoningUnsupportedReason(agentModelNames)}</Typography>] : []),
    ];

  return <>
    <Tooltip title="Files and run settings"><span>
      <IconButton aria-label="Files and run settings" aria-haspopup="menu" aria-expanded={open} size="small" disabled={disabled}
        onClick={event => { setSection('settings'); setAnchorEl(event.currentTarget); }}
        sx={{ width: 32, height: 32, borderRadius: '12px', bgcolor: 'background.subtle', color: 'text.secondary',
          '&:hover': { bgcolor: 'action.hover', color: 'text.primary' } }}>
        <Badge color="primary" variant="dot" invisible={!hasNonDefault}><AddIcon sx={{ fontSize: 18 }} /></Badge>
      </IconButton>
    </span></Tooltip>
    <Menu anchorEl={anchorEl} open={open} onClose={close}
      anchorOrigin={{ vertical: 'top', horizontal: 'left' }} transformOrigin={{ vertical: 'bottom', horizontal: 'left' }}
      MenuListProps={{ ref: listRef, 'aria-label': TITLES[section], onKeyDown: event => {
        if (event.key === 'ArrowLeft' && section !== 'settings') { event.preventDefault(); setSection('settings'); }
      } }}
      slotProps={{ paper: { sx: { width: 352, maxWidth: 'calc(100vw - 24px)', maxHeight: 'min(480px, calc(100vh - 32px))',
        p: 0.75, borderRadius: '16px', backgroundImage: 'none', bgcolor: 'background.paper', '& .MuiMenu-list': { py: 0 } } } }}>
      {section === 'settings' ? rootRows : [
        <MenuItem key="back" aria-label="Back" onClick={() => setSection('settings')} sx={{ ...ROW_SX, mb: 0.5 }}>
          <BackIcon sx={{ fontSize: 16, color: 'text.secondary' }} /><Typography sx={{ fontSize: 13, fontWeight: 600 }}>{TITLES[section]}</Typography>
        </MenuItem>,
        ...optionRows,
      ]}
    </Menu>
  </>;
};

export default ChatInputPlusMenu;
