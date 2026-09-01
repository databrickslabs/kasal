import React, { useEffect, useRef, useState } from 'react';
import { Skill, SkillService } from '../../../../api/tools/SkillService';
import { useExecutionStore } from '../../store/executionStore';

/**
 * The chat input's "+" menu section for picking Agent Skills to attach to the
 * next run's agents.
 *
 * Lists the skills VISIBLE + ENABLED for this workspace (Kasal builtins plus the
 * workspace's own), filtered to enabled — a disabled skill can't be attached, so
 * it's omitted, the same rule McpPicker follows. Selections are stored by NAME
 * (not id): a skill resolves per workspace with an override preferred over the
 * builtin it shadows, and only the name is stable across that. At execution time
 * the backend attaches these to every agent, and the kernel builder injects each
 * skill's <available_skills> block + load_skill/read_skill_file tools.
 *
 * Inline-only (embedded in ComposerMenu's overflow-hidden panel), mirroring
 * McpPicker's `inline` variant — no popover of its own to get clipped.
 */
const SkillsPicker: React.FC<{ disabled?: boolean }> = ({ disabled }) => {
  const [filter, setFilter] = useState('');
  const [skills, setSkills] = useState<Skill[] | null>(null);
  const [error, setError] = useState<string | null>(null);
  const rootRef = useRef<HTMLDivElement>(null);

  const selected = useExecutionStore((s) => s.selectedSkills);
  const toggle = useExecutionStore((s) => s.toggleSkill);

  // Load the workspace's skills once mounted (ComposerMenu mounts this only while
  // its menu is open). Filter to enabled — a disabled skill can't be attached.
  useEffect(() => {
    let cancelled = false;
    setError(null);
    SkillService.list()
      .then((all) => {
        if (cancelled) return;
        const enabled = all.filter((s) => s.enabled);
        setSkills(enabled);
        // Reconcile the persisted selection against reality: a skill deleted or
        // disabled since it was picked leaves a stale name in the store (a phantom
        // "+" count). Drop any selected name no longer available.
        const store = useExecutionStore.getState();
        const available = new Set(enabled.map((s) => s.name));
        const kept = store.selectedSkills.filter((n) => available.has(n));
        if (kept.length !== store.selectedSkills.length) {
          store.setSelectedSkills(kept);
        }
      })
      .catch(() => {
        if (!cancelled) {
          setSkills([]);
          setError('Could not load skills');
        }
      });
    return () => {
      cancelled = true;
    };
  }, []);

  const check = (isSelected: boolean) => (
    <span
      aria-hidden="true"
      className="w-3.5 h-3.5 rounded flex-shrink-0 flex items-center justify-center"
      style={{
        border: `1px solid ${isSelected ? 'var(--accent)' : 'var(--border-color)'}`,
        backgroundColor: isSelected ? 'var(--accent)' : 'transparent',
      }}
    >
      {isSelected && (
        <svg className="w-2.5 h-2.5" fill="none" viewBox="0 0 24 24" stroke="#fff" strokeWidth={3}>
          <path strokeLinecap="round" strokeLinejoin="round" d="M4.5 12.75l6 6 9-13.5" />
        </svg>
      )}
    </span>
  );

  const query = filter.trim().toLowerCase();
  const list = skills ?? [];
  const visible = query
    ? list.filter(
        (s) =>
          s.name.toLowerCase().includes(query) ||
          (s.description || '').toLowerCase().includes(query),
      )
    : list;

  return (
    <div ref={rootRef} role="menu" aria-label="Skills picker" className="w-full">
      <div className="px-3 pb-1.5">
        <input
          value={filter}
          onChange={(e) => setFilter(e.target.value)}
          placeholder="Search skills…"
          aria-label="Search skills"
          className="w-full rounded-md px-2 py-1 text-xs outline-none"
          style={{
            backgroundColor: 'var(--bg-input)',
            color: 'var(--text-primary)',
            border: '1px solid var(--border-color)',
          }}
        />
      </div>

      <div className="max-h-80 overflow-y-auto px-1.5 pb-1.5">
        {skills === null ? (
          <div className="px-3 py-2 text-xs" style={{ color: 'var(--text-muted)' }}>Loading…</div>
        ) : list.length === 0 ? (
          <div className="px-3 py-2 text-xs" style={{ color: 'var(--text-muted)' }}>
            No skills available
          </div>
        ) : visible.length === 0 ? (
          <div className="px-3 py-2 text-xs" style={{ color: 'var(--text-muted)' }}>
            No matching skills
          </div>
        ) : (
          visible.map((skill) => {
            const isSelected = selected.includes(skill.name);
            return (
              <button
                key={String(skill.id)}
                type="button"
                role="menuitemcheckbox"
                aria-checked={isSelected}
                disabled={disabled}
                onClick={() => toggle(skill.name)}
                title={skill.description}
                className="w-full flex items-center gap-2 !px-2.5 !py-1.5 my-0.5 rounded-lg text-left text-xs transition-colors hover:bg-[var(--bg-rail-hover)] disabled:opacity-40 disabled:cursor-not-allowed"
                style={{ color: 'var(--text-primary)' }}
              >
                {check(isSelected)}
                <span className="min-w-0 flex-1">
                  <span className="block truncate font-medium">{skill.name}</span>
                  {skill.description && (
                    <span className="block truncate text-[11px]" style={{ color: 'var(--text-muted)' }}>
                      {skill.description}
                    </span>
                  )}
                </span>
              </button>
            );
          })
        )}
      </div>

      {error && (
        <div className="px-3 py-2 text-[11px]" style={{ color: 'var(--accent)', borderTop: '1px solid var(--border-color)' }}>
          {error}
        </div>
      )}
    </div>
  );
};

export default SkillsPicker;
