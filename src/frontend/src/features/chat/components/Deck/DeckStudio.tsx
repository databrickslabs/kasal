import React, { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { createPortal } from 'react-dom';
import { Download, Loader2, Maximize2, Undo2, Upload, X } from 'lucide-react';
import ScaledFrame from '../Chat/ScaledFrame';
import DeckPresentation from '../Chat/DeckPresentation';
import ThumbnailRail from './ThumbnailRail';
import SlideInstructionBar from './SlideInstructionBar';
import DeckModelPicker from './DeckModelPicker';
import { useDeckHistory } from './useDeckHistory';
import RunProgress from '../Chat/RunProgress';
import StepContent from '../Preview/StepContent';
import type { RunStep } from '../Preview/traceEventStep';
import type { TraceEntryData } from '../Chat/ChatMessage';
import { DeckService } from '../../../../api/chat/DeckService';
import { useAppStore } from '../../store/appStore';
import { useThemeStore } from '../../../../store/theme';
import { useSessionStore } from '../../../../app/sessions/sessionStore';
import { planSlideEdit, type SlideEdit } from '../../utils/slideRefine';
import { useResolvedAssetHtml } from '../../hooks/useResolvedAssetHtml';
import { hasPendingAssets } from '../../utils/assetRefs';
import { SLIDE_W, clearRefined, replaceDeckInContent, splitSlides, stageFor } from '../../utils/htmlDeck';
import { downloadDeckHtml, downloadDeckPdf, downloadDeckPptx, sanitizeDeckDocument } from '../../utils/deckExport';

/**
 * The deck studio: independent slide edits, merged into the latest deck.
 *
 * Thumbnails on the left (select, drag to reorder, duplicate, delete, add
 * between), the selected slide large on a themed stage, and under it the
 * instruction bar. An instruction sends ONLY that slide through the one-slide
 * generation call; the thumbnail pulses while it works, the slide swaps in
 * place, and Undo puts the previous deck back. Structural edits are instant.
 *
 * The message the deck lives in is the source of truth: every change is
 * written straight back into it, and the studio follows it — so the chat's
 * deck card is always current, closing the studio loses nothing, and an edit
 * that lands after a remount still shows.
 */

interface DeckStudioProps {
  /** The deck's HTML (without the ``` fences). */
  code: string;
  /** The chat message the deck lives in — where edits are written back. */
  messageId?: string;
  initialIndex?: number;
  onDeckChange?: (next: string, previous: string) => Promise<void>;
  model?: string;
  onClose: () => void;
}

interface SlideActivity { startedAt: number; jobId?: string; step: TraceEntryData }

const readTextFile = (file: File): Promise<string> => {
  if (typeof file.text === 'function') return file.text();
  return new Promise((resolve, reject) => {
    const reader = new FileReader();
    reader.onload = () => resolve(typeof reader.result === 'string' ? reader.result : '');
    reader.onerror = () => reject(reader.error ?? new Error('File read failed'));
    reader.readAsText(file);
  });
};

const DeckStudio: React.FC<DeckStudioProps> = ({ code, messageId, initialIndex = 0, onClose, onDeckChange, model }) => {
  const dark = useThemeStore(s => s.isDarkMode);
  const writeBack = useCallback((next: string, previous: string) => {
    if (onDeckChange) return onDeckChange(next, previous);
    if (!messageId) return;
    const store = useSessionStore.getState();
    const msg = store.messages.find((m) => m.id === messageId);
    if (msg) store.updateMessage(messageId, { content: replaceDeckInContent(msg.content, next) });
  }, [messageId, onDeckChange]);
  const { deck, current, history, saving, pending: savingRef, commit, undo: undoDeck } = useDeckHistory(code, writeBack);
  const slides = useMemo(() => splitSlides(deck), [deck]);
  const count = slides.length;
  // What the rail, the stage and the exports SHOW: the deck with its
  // `asset:<id>` image references resolved to bytes. Edits and write-backs
  // use `slides`/`deck` — the references stay in what is stored and sent.
  const resolvedDeck = useResolvedAssetHtml(deck);
  const viewSlides = useMemo(() => splitSlides(resolvedDeck), [resolvedDeck]);
  const [selected, setSelected] = useState(() => Math.max(0, Math.min(initialIndex, count - 1)));
  const active = useRef(new Set<number>());
  const [working, setWorking] = useState<ReadonlySet<number>>(new Set());
  const [error, setError] = useState<string | null>(null);
  const [errors, setErrors] = useState<Record<number, string | null>>({});
  const [activities, setActivities] = useState<Record<number, SlideActivity>>({});
  const [activitySteps, setActivitySteps] = useState<Record<number, RunStep | null>>({});
  // The bar revises the selected slide, or writes a just-inserted blank one.
  const [barMode, setBarMode] = useState<{ kind: 'refine' } | { kind: 'fill'; at: number }>({ kind: 'refine' });
  const [present, setPresent] = useState(false);
  const [menu, setMenu] = useState(false);
  const [busy, setBusy] = useState<'' | 'html' | 'pdf' | 'pptx'>('');
  const menuRef = useRef<HTMLDivElement>(null);
  const importRef = useRef<HTMLInputElement>(null);
  const selectedModel = useAppStore((s) => s.selectedModel);
  const [editModel, setEditModel] = useState(() => model || selectedModel || '');

  const shown = Math.min(selected, Math.max(0, count - 1));
  const stage = useMemo(() => stageFor(viewSlides[shown] ?? ''), [viewSlides, shown]);

  const structureSaving = saving && working.size === 0;
  const activity = activities[shown];
  const activityStep = activitySteps[shown];
  const setActivityStep = (step: RunStep | null) => setActivitySteps(all => ({ ...all, [shown]: step }));
  const saveError = () => setError('The deck could not be saved. Your previous version is still shown. Try again.');
  const clearSlideActivity = () => {
    setActivities({});
    setActivitySteps({});
    setErrors({});
  };
  const undo = () => {
    if (active.current.size || savingRef.current) return;
    setError(null);
    void undoDeck().then(clearSlideActivity).catch(saveError);
  };

  const instant = (edit: SlideEdit) => {
    if (active.current.size || savingRef.current) return;
    const plan = planSlideEdit(edit, current.current);
    if (plan.kind !== 'instant') return;
    setError(null);
    void commit(plan.deck, plan.done, () => {
      clearSlideActivity();
      setSelected(plan.focus);
      if (edit.kind === 'blank') setBarMode({ kind: 'fill', at: edit.index });
    }).catch(saveError);
  };

  const apply = async (instruction: string) => {
    const target = barMode.kind === 'fill' ? barMode.at : shown;
    if (active.current.has(target) || (savingRef.current && !active.current.size)) return;
    const edit: SlideEdit =
      barMode.kind === 'fill'
        ? { kind: 'fill', index: barMode.at, instruction }
        : { kind: 'refine', index: shown, instruction };
    const base = current.current;
    const original = splitSlides(clearRefined(base));
    const plan = planSlideEdit(edit, base);
    if (plan.kind !== 'call') return;
    setError(null);
    setErrors(all => ({ ...all, [target]: null }));
    active.current.add(target);
    setWorking(new Set(active.current));
    setActivitySteps(all => ({ ...all, [target]: null }));
    if (barMode.kind === 'fill') setBarMode({ kind: 'refine' });
    // Pin the transcript to the session that initiated this edit. The studio
    // may close or the user may switch sessions before the response arrives.
    const store = useSessionStore.getState();
    const owner = messageId ? store.currentSessionId : null;
    const post = (role: 'user' | 'assistant', content: string, extra?: Parameters<typeof store.addMessage>[2]) =>
      owner ? store.addMessageToTargetSession(owner, role, content, extra) : undefined;
    post('user', `Slide ${target + 1}: ${instruction}`);
    const startedAt = Date.now();
    const pending: TraceEntryData = {
      kind: 'tool_call', label: plan.summary, sublabel: instruction,
      source: 'refine', timestamp: startedAt,
    };
    const stepId = post('assistant', '', { resultType: 'trace', resultData: pending });
    let jobId: string | undefined;
    const updateActivity = (step: TraceEntryData) => {
      const updates = { resultType: 'trace', resultData: step, ...(jobId ? { executionId: jobId } : {}) };
      if (owner && stepId) store.updateMessageInTargetSession(owner, stepId, updates);
      setActivities(all => ({ ...all, [target]: { startedAt, jobId, step } }));
    };
    setActivities(all => ({ ...all, [target]: { startedAt, step: pending } }));
    try {
      const res = await DeckService.refineSlide({ ...plan.request, model: editModel || null }, (id) => {
        jobId = id;
        updateActivity(pending);
      });
      jobId = res.job_id || undefined;
      if (!res.section) throw new Error(res.error || 'The model did not return a slide.');
      if (clearRefined(res.section).trim() === (original[target] || '').trim()) {
        throw new Error('The model returned the slide unchanged. Try a more specific instruction.');
      }
      await commit(latest => {
        const now = splitSlides(clearRefined(latest));
        if (now.length !== original.length || now[target] !== original[target]) {
          throw new Error('This slide changed while the edit was running. Please retry on the current slide.');
        }
        const rebased = planSlideEdit(edit, latest);
        if (rebased.kind !== 'call') throw new Error('The slide edit could not be applied.');
        return rebased.apply(res.section!);
      }, plan.done);
      updateActivity({
        kind: 'tool_result', label: plan.done,
        sublabel: [res.model, 'Agent run'].filter(Boolean).join(' · '),
        source: 'refine', timestamp: Date.now(), durationMs: Date.now() - startedAt,
      });
    } catch (e) {
      const detail = (e as { response?: { data?: { detail?: unknown } } })?.response?.data?.detail;
      const message = typeof detail === 'string' ? detail : e instanceof Error ? e.message : 'The edit failed.';
      setErrors(all => ({ ...all, [target]: message }));
      updateActivity({ kind: 'event', label: 'Slide edit failed', detail: message,
        source: 'refine', timestamp: Date.now(), durationMs: Date.now() - startedAt });
    } finally {
      active.current.delete(target);
      setWorking(new Set(active.current));
    }
  };

  // Keys are handled on the studio's own container (onKeyDown below), NOT a
  // window listener: the container calls stopPropagation to stay modal, which
  // would also stop a window listener from ever seeing the event. Escape closes;
  // arrows page slides — except while the instruction bar (which autofocuses)
  // HAS text to edit, when the field keeps them. While the presentation overlay
  // is open it owns the keys through its own window listener, so we don't touch
  // them.
  const onDeckKey = useCallback(
    (e: React.KeyboardEvent) => {
      if (present) return; // the presentation overlay owns the keys
      const el = e.target as HTMLInputElement | HTMLTextAreaElement | null;
      const editing =
        !!el && (el.tagName === 'TEXTAREA' || el.tagName === 'INPUT') && el.value.length > 0;
      if (e.key === 'Escape') {
        e.preventDefault();
        onClose();
      } else if (!editing) {
        const k = e.key;
        let next: number | null = null;
        if (k === 'ArrowLeft' || k === 'ArrowUp' || k === 'PageUp') next = Math.max(0, shown - 1);
        else if (k === 'ArrowRight' || k === 'ArrowDown' || k === 'PageDown') next = Math.min(count - 1, shown + 1);
        else if (k === 'Home') next = 0;
        else if (k === 'End') next = count - 1;
        if (next !== null) {
          e.preventDefault();
          setSelected(next);
        }
      }
      // Modal: keep keys from reaching the chat behind (a portal still bubbles
      // React events up the component tree).
      e.stopPropagation();
    },
    [present, shown, count, onClose],
  );

  useEffect(() => {
    if (!menu) return;
    const onDoc = (e: MouseEvent) => {
      if (menuRef.current && !menuRef.current.contains(e.target as Node)) setMenu(false);
    };
    document.addEventListener('mousedown', onDoc);
    return () => document.removeEventListener('mousedown', onDoc);
  }, [menu]);

  const runExport = async (kind: 'html' | 'pdf' | 'pptx') => {
    setMenu(false);
    setBusy(kind);
    try {
      if (kind === 'html') downloadDeckHtml(resolvedDeck);
      else if (kind === 'pdf') await downloadDeckPdf(viewSlides);
      else await downloadDeckPptx(viewSlides);
    } catch (err) {
      console.error('[deck] export failed', err);
    } finally {
      setBusy('');
    }
  };

  const importHtml = async (event: React.ChangeEvent<HTMLInputElement>) => {
    const file = event.target.files?.[0];
    event.target.value = '';
    if (!file || active.current.size || savingRef.current) return;
    setError(null);
    try {
      const documentHtml = await readTextFile(file);
      if (active.current.size || savingRef.current) return;
      const parsed = new DOMParser().parseFromString(documentHtml, 'text/html');
      // Our own standalone export keeps the authored deck inside #deck-stage.
      // For another HTML document, carry its head styles along with its body.
      const exportedStage = parsed.querySelector<HTMLElement>('#deck-stage');
      const headStyles = Array.from(parsed.head.querySelectorAll('style')).map((style) => style.outerHTML).join('\n');
      const candidate = exportedStage?.innerHTML ?? [headStyles, parsed.body.innerHTML].filter(Boolean).join('\n');
      const imported = sanitizeDeckDocument(candidate).trim();
      if (splitSlides(imported).length === 0) {
        setError('That HTML file does not contain any presentation slides.');
        return;
      }
      void commit(imported, 'Imported HTML deck', () => {
        clearSlideActivity();
        setSelected(0);
        setBarMode({ kind: 'refine' });
      }).catch(saveError);
    } catch {
      setError('The HTML presentation could not be imported.');
    }
  };

  const btn =
    'inline-flex items-center gap-1.5 rounded-md !px-2.5 !py-1.5 text-xs font-medium transition-colors hover:bg-[var(--bg-rail-hover)] disabled:opacity-40 disabled:hover:bg-transparent';
  const lastEdit = history[history.length - 1];

  // Portaled to <body> so its z-index counts in the ROOT stacking context —
  // inside the chat column, the app bar (z 1001, at the root) paints over it
  // whatever z-index the studio has. The chat's utility classes are scoped to
  // `.kasal-chat-root` (tailwind.config `important`), so the portal carries
  // that class on a wrapper; without it `fixed inset-0` would not apply and
  // the studio would land as a giant static block at the end of the page.
  return createPortal(
    <div className="kasal-chat-root" data-theme={dark ? 'dark' : 'light'} style={{ colorScheme: dark ? 'dark' : 'light' }}>
      <div
        role="dialog"
        aria-label="Deck studio"
        aria-modal="true"
        className="kasal-deck-studio fixed inset-0 z-[1200] flex flex-col"
        // A portal still bubbles React events up the COMPONENT tree: a click in
        // the instruction bar reached the deck card's onClick, which focuses the
        // card — and the textarea lost focus the moment it got it. The studio is
        // modal; nothing behind it should hear its clicks. Keys are handled here
        // too (onDeckKey), which is why they can't just be stopped: a blanket
        // key-stop is what kept arrow navigation — and the presentation's own
        // key handler — from ever running.
        onClick={(e) => e.stopPropagation()}
        onKeyDown={onDeckKey}
        style={{ background: 'var(--bg-primary)', color: 'var(--text-primary)' }}
      >
        <div className="flex items-center gap-2 px-4 py-2" style={{ borderBottom: '1px solid var(--border-color)' }}>
          <span className="text-sm font-medium">Deck</span>
          <span className="text-xs" style={{ color: 'var(--text-secondary)' }}>
            {count} slide{count === 1 ? '' : 's'}
            {history.length > 0 ? ` · ${history.length} edit${history.length === 1 ? '' : 's'}` : ''}
          </span>
          {(working.size > 0 || saving) && (
            <span role="status" className="text-xs" style={{ color: 'var(--text-secondary)' }}>
              Reordering available after slide edits finish saving
            </span>
          )}
          <div className="ml-auto flex items-center gap-1">
            <input
              ref={importRef}
              type="file"
              accept=".html,text/html"
              hidden
              data-testid="deck-html-input"
              onChange={importHtml}
            />
            <button
              type="button"
              className={btn}
              disabled={!lastEdit || working.size > 0 || saving}
              onClick={undo}
              title={lastEdit ? `Undo: ${lastEdit.label}` : 'Undo'}
            >
              <Undo2 size={14} /> Undo
            </button>
            <button type="button" className={btn} onClick={() => setPresent(true)} title="Present">
              <Maximize2 size={14} /> Present
            </button>
            <button
              type="button"
              className={btn}
              disabled={working.size > 0 || saving}
              onClick={() => importRef.current?.click()}
              title="Import HTML"
            >
              <Upload size={14} /> Import HTML
            </button>
            <div ref={menuRef} className="relative">
              <button type="button" className={btn} disabled={!!busy} onClick={() => setMenu((m) => !m)} title="Download">
                {busy ? <Loader2 size={14} className="animate-spin" /> : <Download size={14} />} Download
              </button>
              {menu && (
                <div
                  className="absolute right-0 z-10 mt-1 min-w-[10rem] overflow-hidden rounded-md border py-1 shadow-lg"
                  style={{ background: 'var(--bg-primary)', borderColor: 'var(--border-color)' }}
                >
                  <button type="button" className="block w-full !px-3 !py-1.5 text-left text-xs hover:bg-[var(--bg-rail-hover)]" onClick={() => runExport('html')}>
                    Download HTML
                  </button>
                  <button type="button" className="block w-full !px-3 !py-1.5 text-left text-xs hover:bg-[var(--bg-rail-hover)]" onClick={() => runExport('pdf')}>
                    Download PDF
                  </button>
                  <button type="button" className="block w-full !px-3 !py-1.5 text-left text-xs hover:bg-[var(--bg-rail-hover)]" onClick={() => runExport('pptx')}>
                    Download PowerPoint
                  </button>
                </div>
              )}
            </div>
            <button type="button" className={btn} onClick={onClose} title="Done (Esc)" style={{ background: 'var(--bg-active-chip)' }}>
              <X size={14} /> Done
            </button>
          </div>
        </div>
        <div className="flex min-h-0 flex-1">
          <ThumbnailRail
            slides={viewSlides}
            selected={shown}
            working={working}
            locked={working.size > 0 || saving}
            onSelect={(i) => {
              setSelected(i);
              setBarMode({ kind: 'refine' });
            }}
            onMove={(from, to) => instant({ kind: 'move', from, to })}
            onDuplicate={(i) => instant({ kind: 'duplicate', index: i })}
            onRemove={(i) => instant({ kind: 'remove', index: i })}
            // "+" adds a blank slide in the neighbour's design RIGHT AWAY (the
            // click has to do something visible), then the bar offers to write it.
            onAddAt={(at) => {
              instant({ kind: 'blank', index: at });
            }}
          />
          <div className="flex min-w-0 flex-1 flex-col">
            <div className="min-h-0 flex-1 p-6">
              <div className="h-full w-full">
                <ScaledFrame
                  // Remount once the slide's images are in (see hasPendingAssets).
                  key={hasPendingAssets(stage) ? 'pending' : 'ready'}
                  html={stage}
                  baseWidth={SLIDE_W}
                  contain
                  upscale
                  pad={0}
                  background="transparent"
                  title={`Slide ${shown + 1}`}
                />
              </div>
            </div>
            {activity && <div className="overflow-auto px-3"
              style={{ maxHeight: '30vh', flexShrink: 0 }} aria-label="Slide edit activity">
              {activityStep ? <>
                <button type="button" className={btn} onClick={() => setActivityStep(null)}>Back to run activity</button>
                <StepContent step={activityStep} />
              </> : <RunProgress key={`${shown}-${activity.startedAt}`} inline autoExpand={false} running={working.has(shown)} generating={working.has(shown)}
                latestStep={activity.step} jobId={activity.jobId} onSelectStep={setActivityStep} />}
            </div>}
            <SlideInstructionBar
              controls={<DeckModelPicker value={editModel} onChange={setEditModel} disabled={working.has(shown) || structureSaving} />}
              slideNumber={barMode.kind === 'fill' ? barMode.at + 1 : shown + 1}
              mode={barMode.kind}
              working={working.has(shown) || structureSaving}
              error={errors[shown] || error}
              onApply={apply}
              onCancelFill={() => setBarMode({ kind: 'refine' })}
            />
          </div>
        </div>
        {present && (
          <DeckPresentation
            stage={stage}
            index={shown}
            count={count}
            onPrev={() => setSelected((i) => Math.max(0, i - 1))}
            onNext={() => setSelected((i) => Math.min(count - 1, i + 1))}
            onFirst={() => setSelected(0)}
            onLast={() => setSelected(Math.max(0, count - 1))}
            onClose={() => setPresent(false)}
          />
        )}
      </div>
    </div>,
    document.body,
  );
};

export default DeckStudio;
