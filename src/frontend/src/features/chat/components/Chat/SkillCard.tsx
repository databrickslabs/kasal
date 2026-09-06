import React, { useMemo, useState } from 'react';
import { AlertTriangle, Check, ChevronDown, ChevronRight, Loader2, Save, Sparkles } from 'lucide-react';
import { SkillService } from '../../../../api/tools/SkillService';
import { invalidateSkillsCache } from '../../utils/skillSelection';
import { parseSkillMarkdown } from '../../utils/skillBlock';

/**
 * A SKILL.md draft the assistant proposed (a ```skill block), rendered as a
 * card with a "Save to teamspace" button.
 *
 * The model only ever PROPOSES: saving goes through the existing skills API
 * with a person clicking — validate first (the reference validator's own
 * messages surface inline), then create, or update in place when this
 * teamspace already owns a skill of that name. A name that matches one of
 * Kasal's builtins saves a teamspace copy that overrides it, and the card says
 * so. Operators get the backend's 403 verbatim — the button is not hidden by
 * a client-side guess about roles.
 */
interface SkillCardProps {
  /** The SKILL.md text between the fences. */
  code: string;
  /** True while the draft is still streaming (unclosed fence). */
  streaming?: boolean;
  /** True when the message ended without closing the fence. */
  truncated?: boolean;
}

type Phase = 'idle' | 'saving' | 'saved' | 'error';

function errorDetail(e: unknown): string {
  const err = e as { response?: { data?: { detail?: unknown } }; message?: string };
  const detail = err?.response?.data?.detail;
  if (typeof detail === 'string') return detail;
  if (detail && typeof detail === 'object') return JSON.stringify(detail);
  return err?.message || 'Could not save the skill';
}

const SkillCard: React.FC<SkillCardProps> = ({ code, streaming = false, truncated = false }) => {
  const draft = useMemo(() => parseSkillMarkdown(code), [code]);
  const [phase, setPhase] = useState<Phase>('idle');
  const [errors, setErrors] = useState<string[]>([]);
  const [message, setMessage] = useState<string | null>(null);
  const [bodyOpen, setBodyOpen] = useState(false);

  const canSave = !streaming && phase !== 'saving' && phase !== 'saved' && !!draft.name;
  const status = streaming
    ? 'Drafting…'
    : phase === 'saved'
      ? 'Saved'
      : truncated
        ? 'Draft · incomplete'
        : 'Draft';

  const save = async () => {
    setPhase('saving');
    setErrors([]);
    setMessage(null);
    const input = {
      name: draft.name,
      description: draft.description,
      body: draft.body,
      license: draft.license ?? null,
      compatibility: draft.compatibility ?? null,
    };
    try {
      const verdict = await SkillService.validate(input);
      if (!verdict.valid) {
        setErrors(verdict.errors?.length ? verdict.errors : ['The draft did not pass validation.']);
        setPhase('error');
        return;
      }
      const same = (await SkillService.list()).find((s) => s.name === draft.name);
      const saved =
        same && same.group_id
          ? await SkillService.update(same.id, input)
          : await SkillService.create(input);
      invalidateSkillsCache();
      setPhase('saved');
      setMessage(
        same
          ? same.group_id
            ? `Updated "${saved.name}" in this teamspace.`
            : `Saved "${saved.name}" — it overrides Kasal's builtin of that name for this teamspace.`
          : `Saved "${saved.name}". Pick it in the "+" menu to use it on the next turn.`,
      );
    } catch (e) {
      setPhase('error');
      setMessage(errorDetail(e));
    }
  };

  // Quiet, bordered action in the chat's own button idiom (text-xs, rounded-lg,
  // rail hover) — not a filled pill: --accent is the theme's red alert color.
  const btn =
    'inline-flex items-center gap-1.5 rounded-lg border text-xs font-medium transition-colors ' +
    'hover:bg-[var(--bg-rail-hover)] disabled:opacity-40 disabled:cursor-not-allowed disabled:hover:bg-transparent';

  return (
    <div
      className="my-2 overflow-hidden rounded-lg border"
      style={{ borderColor: 'var(--border-color, rgba(0,0,0,0.12))' }}
      role="group"
      aria-label={`Skill draft ${draft.name || ''}`.trim()}
    >
      <div
        className="flex items-center justify-between gap-2 px-3 py-1.5 text-xs"
        style={{ background: 'var(--bg-secondary, #f5f5f5)', color: 'var(--text-muted, rgba(0,0,0,0.6))' }}
      >
        <span className="inline-flex items-center gap-1.5 font-medium">
          <Sparkles size={13} />
          Skill
          {draft.name && (
            <code
              className="rounded px-1 py-0.5 text-[11px]"
              style={{ background: 'var(--bg-primary, #fff)', color: 'var(--text-primary)' }}
            >
              {draft.name}
            </code>
          )}
        </span>
        <span>{status}</span>
      </div>

      <div className="px-3 py-2 text-[13px] leading-snug">
        {draft.description ? (
          <p className="my-0" style={{ color: 'var(--text-primary, #111)' }}>{draft.description}</p>
        ) : (
          <p className="my-0 italic" style={{ color: 'var(--text-muted, rgba(0,0,0,0.6))' }}>
            {streaming ? 'Writing the description…' : 'No description yet.'}
          </p>
        )}

        <button
          type="button"
          className="mt-2 inline-flex items-center gap-1 text-xs"
          style={{ color: 'var(--text-muted, rgba(0,0,0,0.6))' }}
          onClick={() => setBodyOpen((o) => !o)}
          aria-expanded={bodyOpen}
        >
          {bodyOpen ? <ChevronDown size={13} /> : <ChevronRight size={13} />}
          {bodyOpen ? 'Hide instructions' : 'Show instructions'}
        </button>
        {bodyOpen && (
          <pre
            className="mt-1 max-h-80 overflow-auto whitespace-pre-wrap rounded p-2 text-xs"
            style={{ background: 'var(--bg-secondary, #f5f5f5)', color: 'var(--text-primary, #111)' }}
          >
            {draft.body}
          </pre>
        )}
      </div>

      <div
        className="flex items-center justify-between gap-2 border-t px-3 py-1.5"
        style={{ borderColor: 'var(--border-color, rgba(0,0,0,0.12))' }}
      >
        <div className="min-w-0 flex-1 text-xs">
          {errors.length > 0 && (
            <ul className="my-0 list-disc pl-4" style={{ color: 'var(--accent, #d33)' }}>
              {errors.map((err, i) => (
                <li key={i}>{err}</li>
              ))}
            </ul>
          )}
          {message && (
            <span
              className="inline-flex items-center gap-1"
              style={{ color: phase === 'error' ? 'var(--accent, #d33)' : 'var(--text-muted, rgba(0,0,0,0.6))' }}
            >
              {phase === 'error' ? <AlertTriangle size={13} /> : <Check size={13} />}
              {message}
            </span>
          )}
        </div>
        <button
          type="button"
          className={btn}
          style={{
            padding: '7px 12px',
            borderColor: 'var(--border-color)',
            color: phase === 'saved' ? 'var(--text-muted)' : 'var(--text-primary)',
            backgroundColor: 'var(--bg-primary)',
          }}
          disabled={!canSave}
          onClick={save}
          title={draft.name ? 'Validate and save this skill to your teamspace' : 'The draft needs a name first'}
        >
          {phase === 'saving' ? <Loader2 size={13} className="animate-spin" /> : phase === 'saved' ? <Check size={13} /> : <Save size={13} />}
          {phase === 'saved' ? 'Saved' : 'Save to teamspace'}
        </button>
      </div>
    </div>
  );
};

export default SkillCard;
