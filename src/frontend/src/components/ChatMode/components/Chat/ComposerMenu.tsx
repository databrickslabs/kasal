/**
 * The composer's "+" menu — one home for every input-bar setting.
 *
 * The bar used to line up five separate controls (source, answer mode, memory,
 * model, attach) beside the MCP picker; on narrow panes they wrapped and read
 * as clutter. They all live here now as UNIFORM rows — label left, current
 * value right — and picking one SWAPS the panel to that section (back header +
 * options) instead of expanding inline. The accordion version grew past the
 * anchored menu's height when a long section (MCP) opened, shoving the top
 * rows out of view; a two-level panel keeps the menu height stable. Nothing
 * opens a nested popover: a popover inside this overflow-hidden menu gets
 * clipped (the invisible-MCP bug). The MCP list embeds via McpPicker's
 * `inline` variant.
 *
 * Extracted from ChatInput.tsx (which is past the size target) rather than
 * grown inside it.
 */
import React, { useEffect, useRef, useState } from 'react';
import { ModelConfigResponse } from '../../types/dispatcher';
import {
  isAnswerModeDisabled,
  modelLacksReasoning,
} from '../../utils/answerModes';
import { SOURCE_MODES } from '../../utils/sourceModes';
import McpPicker from './McpPicker';
import SkillsPicker from './SkillsPicker';
import { useAnchoredFixedStyle } from '../../hooks/useAnchoredFixedStyle';
import { useExecutionStore } from '../../store/executionStore';

export const MODES = [
  { id: 'chat', label: 'Chat', desc: 'Quick answer from a single agent' },
  { id: 'research', label: 'Research', desc: 'Full multi-agent crew' },
  { id: 'deep', label: 'Deep Research', desc: 'Maximum reasoning effort' },
] as const;

export type MemoryModeId = 'workspace' | 'session';
export const MEMORY_MODES: { id: MemoryModeId; label: string; hint: string }[] = [
  { id: 'workspace', label: 'Teamspace memory', hint: 'Recall context across the whole teamspace' },
  { id: 'session', label: 'Session memory', hint: "Recall only this chat's history — no teamspace memory" },
];

type SectionId = '' | 'source' | 'mode' | 'memory' | 'model' | 'tools' | 'skills';

/** Model lists at or under this length render without a search box. */
const MODEL_SEARCH_THRESHOLD = 6;

const SECTION_TITLES: Record<Exclude<SectionId, ''>, string> = {
  model: 'Model',
  source: 'Source',
  mode: 'Answer mode',
  memory: 'Memory',
  tools: 'Tools & MCP',
  skills: 'Skills',
};

interface ComposerMenuProps {
  disabled?: boolean;
  menuPlacement: 'up' | 'down';
  menuAnimClass: string;
  /** Focus the input after a pick inside the menu. */
  onPicked: () => void;
  chatModeType: string;
  setChatModeType: (id: 'chat' | 'research' | 'deep') => void;
  models: ModelConfigResponse[];
  selectedModel: string;
  onModelChange: (key: string) => void;
  memoryEnabled: boolean;
  onToggleMemory: () => void;
  attachmentCount: number;
  onAttachFiles: () => void;
  onOpenMcpConfig?: () => void;
  /** Capture this conversation into a skill draft (sends the /skill command). */
  onCreateSkill?: () => void;
}

/** One uniform menu row: label left, value right, a nav chevron (points right —
 *  the row navigates to a sub-panel, it doesn't expand in place). */
const Row: React.FC<{
  label: string;
  value?: React.ReactNode;
  chevron?: boolean;
  onClick: () => void;
  'aria-label'?: string;
}> = ({ label, value, chevron = true, onClick, ...aria }) => (
  <button
    type="button"
    aria-label={aria['aria-label'] ?? label}
    onClick={onClick}
    className="w-full flex items-center justify-between gap-3 !px-3 !py-2.5 rounded-lg text-[13px] transition-colors hover:bg-[var(--bg-rail-hover)]"
    style={{ backgroundColor: 'transparent', border: 'none' }}
  >
    <span className="font-medium" style={{ color: 'var(--text-primary)' }}>
      {label}
    </span>
    <span className="flex min-w-0 items-center gap-1.5 text-xs" style={{ color: 'var(--text-muted)' }}>
      {value}
      {chevron && (
        <svg className="w-3 h-3 flex-shrink-0" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2.5}>
          <path strokeLinecap="round" strokeLinejoin="round" d="M8.25 4.5l7.5 7.5-7.5 7.5" />
        </svg>
      )}
    </span>
  </button>
);

/** An option inside a section panel: name + description, check when active. */
const Option: React.FC<{
  label: string;
  desc?: string;
  active?: boolean;
  disabled?: boolean;
  onClick: () => void;
}> = ({ label, desc, active, disabled, onClick }) => (
  <button
    type="button"
    disabled={disabled}
    role="menuitemradio"
    aria-checked={!!active}
    onClick={onClick}
    className={`w-full text-left !px-3 !py-2 rounded-lg transition-colors ${
      disabled ? 'opacity-45 cursor-not-allowed' : active ? 'bg-[var(--bg-active-chip)]' : 'hover:bg-[var(--bg-rail-hover)]'
    }`}
    style={{ backgroundColor: undefined, border: 'none' }}
  >
    <span className="flex items-center justify-between gap-2">
      <span className="text-[13px] font-medium" style={{ color: 'var(--text-primary)' }}>
        {label}
      </span>
      {active && (
        <svg className="w-3.5 h-3.5 flex-shrink-0" style={{ color: 'var(--accent)' }} fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2.5}>
          <path strokeLinecap="round" strokeLinejoin="round" d="M4.5 12.75l6 6 9-13.5" />
        </svg>
      )}
    </span>
    {desc && (
      <span className="block text-xs mt-0.5" style={{ color: 'var(--text-muted)' }}>
        {desc}
      </span>
    )}
  </button>
);

const Divider: React.FC = () => (
  <div className="mx-3 my-1 border-t" style={{ borderColor: 'var(--border-color)' }} />
);

const ComposerMenu: React.FC<ComposerMenuProps> = ({
  disabled,
  menuPlacement,
  menuAnimClass,
  onPicked,
  chatModeType,
  setChatModeType,
  models,
  selectedModel,
  onModelChange,
  memoryEnabled,
  onToggleMemory,
  attachmentCount,
  onAttachFiles,
  onOpenMcpConfig,
  onCreateSkill,
}) => {
  const [open, setOpen] = useState(false);
  const [section, setSectionRaw] = useState<SectionId>('');
  const [modelFilter, setModelFilter] = useState('');
  // Entering/leaving a section always starts it fresh — a stale model search
  // from the last visit would silently hide models.
  const setSection = (id: SectionId) => {
    setModelFilter('');
    setSectionRaw(id);
  };
  const rootRef = useRef<HTMLDivElement>(null);
  const menuStyle = useAnchoredFixedStyle(open, rootRef, menuPlacement);

  const preferExisting = useExecutionStore((s) => s.preferExisting);
  const setPreferExisting = useExecutionStore((s) => s.setPreferExisting);
  const selectedMcp = useExecutionStore((s) => s.selectedMcpServers);
  const selectedBricks = useExecutionStore((s) => s.selectedAgentBricksEndpoints) ?? [];
  const selectedSkills = useExecutionStore((s) => s.selectedSkills) ?? [];
  const toolCount = (selectedMcp?.length ?? 0) + selectedBricks.length;
  const skillCount = selectedSkills.length;

  useEffect(() => {
    if (!open) return;
    const onDown = (e: MouseEvent) => {
      if (rootRef.current && !rootRef.current.contains(e.target as Node)) setOpen(false);
    };
    document.addEventListener('mousedown', onDown);
    return () => document.removeEventListener('mousedown', onDown);
  }, [open]);

  const activeSource = preferExisting ? SOURCE_MODES[1] : SOURCE_MODES[0];
  const activeMode = MODES.find((m) => m.id === chatModeType) ?? MODES[0];
  const lacksReasoning = modelLacksReasoning(models, selectedModel);
  const modelName = models.find((m) => m.key === selectedModel)?.name || selectedModel || 'Default';
  const activeMemory = MEMORY_MODES[memoryEnabled ? 0 : 1];

  const badgeCount = attachmentCount + toolCount + skillCount;

  const modelQuery = modelFilter.trim().toLowerCase();
  const visibleModels = modelQuery
    ? models.filter((m) => (m.name || m.key).toLowerCase().includes(modelQuery) || m.key.toLowerCase().includes(modelQuery))
    : models;

  /** Pick handler shared by every radio-style option: apply, back to the main
   *  level (so the updated value is visible), refocus the input. */
  const pick = (apply: () => void) => {
    apply();
    setSection('');
    onPicked();
  };

  const sectionPanel = (id: Exclude<SectionId, ''>) => (
    <>
      {/* Back header — the sub-panel replaces the row list, it never pushes it
          out of view. */}
      <button
        type="button"
        aria-label="Back"
        onClick={() => setSection('')}
        className="w-full flex items-center gap-2 !px-3 !py-2.5 rounded-lg text-[13px] font-medium transition-colors hover:bg-[var(--bg-rail-hover)]"
        style={{ backgroundColor: 'transparent', border: 'none', color: 'var(--text-primary)' }}
      >
        <svg className="w-3.5 h-3.5 flex-shrink-0" style={{ color: 'var(--text-secondary)' }} fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2.5}>
          <path strokeLinecap="round" strokeLinejoin="round" d="M15.75 19.5l-7.5-7.5 7.5-7.5" />
        </svg>
        {SECTION_TITLES[id]}
      </button>
      <Divider />

      {id === 'model' && (
        <>
          {/* A short list is faster to scan than to search; the box appears
              once the list is long enough to need it. */}
          {models.length > MODEL_SEARCH_THRESHOLD && (
            <div className="px-1.5 pb-1.5">
              <input
                value={modelFilter}
                onChange={(e) => setModelFilter(e.target.value)}
                placeholder="Search models…"
                aria-label="Search models"
                autoFocus
                className="w-full rounded-md px-2 py-1.5 text-xs outline-none"
                style={{
                  backgroundColor: 'var(--bg-secondary)',
                  color: 'var(--text-primary)',
                  border: '1px solid var(--border-color)',
                }}
              />
            </div>
          )}
          <div className="max-h-72 overflow-y-auto">
            {visibleModels.length === 0 ? (
              <div className="px-3 py-2 text-xs" style={{ color: 'var(--text-muted)' }}>
                No matching models
              </div>
            ) : (
              visibleModels.map((m) => (
                <Option
                  key={m.key}
                  label={m.name || m.key}
                  active={m.key === selectedModel}
                  onClick={() => pick(() => onModelChange(m.key))}
                />
              ))
            )}
          </div>
        </>
      )}

      {id === 'source' &&
        SOURCE_MODES.map((m) => (
          <Option
            key={m.id}
            label={m.label}
            desc={m.hint}
            active={m.id === activeSource.id}
            onClick={() => pick(() => setPreferExisting(m.id === 'existing'))}
          />
        ))}

      {id === 'mode' &&
        MODES.map((m) => {
          const modeDisabled = isAnswerModeDisabled(m.id, lacksReasoning);
          return (
            <Option
              key={m.id}
              label={m.label}
              desc={m.desc}
              active={m.id === chatModeType}
              disabled={modeDisabled}
              onClick={() => {
                if (modeDisabled) return;
                pick(() => setChatModeType(m.id));
              }}
            />
          );
        })}

      {id === 'memory' && (
        <div role="radiogroup" aria-label="Memory mode">
          {MEMORY_MODES.map((m) => (
            <Option
              key={m.id}
              label={m.label}
              desc={m.hint}
              active={m.id === activeMemory.id}
              onClick={() =>
                pick(() => {
                  if (m.id !== activeMemory.id) onToggleMemory();
                })
              }
            />
          ))}
        </div>
      )}

      {/* Multi-select: stays open — back out with the header when done. */}
      {id === 'tools' && (
        <McpPicker variant="inline" disabled={disabled} onOpenMcpConfig={onOpenMcpConfig} />
      )}

      {/* Skills — same multi-select pattern; back out with the header. */}
      {id === 'skills' && <SkillsPicker disabled={disabled} />}
    </>
  );

  return (
    <div className="relative" ref={rootRef}>
      <button
        type="button"
        disabled={disabled}
        onClick={() =>
          setOpen((o) => {
            if (!o) setSection(''); // always reopen at the main level
            return !o;
          })
        }
        aria-label="Composer settings and tools"
        aria-expanded={open}
        className="relative flex-shrink-0 w-8 h-8 rounded-xl flex items-center justify-center transition-colors hover:opacity-80 disabled:opacity-40 disabled:cursor-not-allowed"
        style={{ color: 'var(--text-secondary)', backgroundColor: 'var(--bg-secondary)', border: '1px solid var(--border-color)' }}
      >
        <svg
          className={`w-4 h-4 transition-transform ${open ? 'rotate-45' : ''}`}
          fill="none"
          viewBox="0 0 24 24"
          stroke="currentColor"
          strokeWidth={2}
        >
          <path strokeLinecap="round" strokeLinejoin="round" d="M12 4.5v15m7.5-7.5h-15" />
        </svg>
        {badgeCount > 0 && (
          <span
            className="absolute -top-1 -right-1 text-[9px] tabular-nums rounded-full min-w-[14px] h-[14px] flex items-center justify-center px-0.5"
            style={{ backgroundColor: 'var(--accent)', color: '#fff' }}
          >
            {badgeCount}
          </span>
        )}
      </button>

      {open && (
        <div
          className={`kasal-popover ${menuAnimClass} w-[22rem] rounded-xl overflow-hidden z-50`}
          style={{ ...menuStyle, backgroundColor: 'var(--bg-input)', border: '1px solid var(--border-color)' }}
        >
          <div className="max-h-[70vh] overflow-y-auto py-1.5 px-1.5 flex flex-col">
            {section !== '' ? (
              sectionPanel(section)
            ) : (
              <>
                {models.length > 0 && (
                  <Row
                    label="Model"
                    value={<span className="max-w-[150px] truncate">{modelName}</span>}
                    onClick={() => setSection('model')}
                  />
                )}

                <Row label="Source" value={activeSource.short} onClick={() => setSection('source')} />

                {/* Answer mode — hidden, not greyed, while "Use existing" is on:
                    a saved crew carries its own agents/process/model. */}
                {!preferExisting && (
                  <Row label="Answer mode" value={activeMode.label} onClick={() => setSection('mode')} />
                )}

                <Row label="Memory" value={activeMemory.label} onClick={() => setSection('memory')} />

                {/* Attach — an action, not a section: chevron-less. */}
                <Row
                  label="Attach files"
                  chevron={false}
                  value={
                    attachmentCount > 0 ? (
                      <span
                        className="text-[10px] tabular-nums rounded-full min-w-[16px] h-[16px] flex items-center justify-center px-1"
                        style={{ backgroundColor: 'var(--accent)', color: '#fff' }}
                      >
                        {attachmentCount}
                      </span>
                    ) : (
                      // Paperclip — clearer than a text hint like "Add knowledge"
                      <svg className="w-3.5 h-3.5 flex-shrink-0" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={1.8} aria-hidden="true">
                        <path strokeLinecap="round" strokeLinejoin="round" d="M18.375 12.739l-7.693 7.693a4.5 4.5 0 01-6.364-6.364l10.94-10.94A3 3 0 1119.5 7.372L8.552 18.32m.009-.01l-.01.01m5.699-9.941l-7.81 7.81a1.5 1.5 0 002.112 2.13" />
                      </svg>
                    )
                  }
                  onClick={() => {
                    setOpen(false);
                    onAttachFiles();
                  }}
                />

                <Divider />

                <Row
                  label="Tools & MCP"
                  value={toolCount > 0 ? `${toolCount} selected` : 'None selected'}
                  onClick={() => setSection('tools')}
                />

                <Row
                  label="Skills"
                  value={skillCount > 0 ? `${skillCount} selected` : 'None selected'}
                  onClick={() => setSection('skills')}
                />

                {onCreateSkill && (
                  // An action, not a section: distil this conversation into a
                  // skill draft (validated on the backend, saved by a click).
                  <Row
                    label="Create a skill from this chat"
                    chevron={false}
                    onClick={() => {
                      onCreateSkill();
                      onPicked();
                    }}
                  />
                )}
              </>
            )}
          </div>
        </div>
      )}
    </div>
  );
};

export default ComposerMenu;
