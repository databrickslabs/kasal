import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { clearRefined, markRefined, replaceSlide, splitSlides } from '../../utils/htmlDeck';
import { SLIDE_EDIT_MSG, applySlideOps, editableStage, type SlideOp } from '../../utils/slideEdit';

type Commit = (update: (latest: string) => string, label: string) => Promise<void>;

interface Session {
  index: number;
  /** The stored slide when the session began (marker cleared): what the frame's numbers refer to. */
  base: string;
  /** The slide as the deck holds it now, after this session's saves (marker cleared). */
  written: string;
  /** What the frame renders; fixed for the session so a save never reloads it. */
  view: string;
  /** Tells this session's frame messages from any other frame's. */
  key: string;
}

/** Thrown when the slide changed under an edit; its message is shown as is. */
class SlideEditConflict extends Error {}

const OPS = new Set(['html', 'remove', 'pick', 'commit']);
const ELEMENT_ID = /^n?\d{1,6}$/;
// One element's content; anything larger is not something a person typed.
const MAX_HTML = 200_000;

const newKey = (): string =>
  globalThis.crypto?.randomUUID?.() ?? `${Date.now()}-${Math.random().toString(36).slice(2)}`;

/**
 * The selected slide, editable right on the studio's stage (see utils/slideEdit).
 * There is no edit mode: the stage always renders the slide marked for
 * editing, the frame reports what the person does as operations, and they are
 * saved when the person leaves the text they were in, moves to another slide,
 * or does anything else in the studio (`flush`). Each save is one edit in the
 * history, so Undo takes it back.
 *
 * The frame keeps its document across saves — reloading it would drop the
 * caret — and is rebuilt only when the slide changes some other way (Undo, a
 * model edit, a replaced image, another slide selected).
 */
export function useSlideEditing({ slides, viewSlides, shown, editable, commit, onError, onPickImage }: {
  /** The stored slides (asset references intact). */
  slides: string[];
  /** The same slides as the stage renders them (images resolved). */
  viewSlides: string[];
  shown: number;
  /** False while the slide is being rewritten by the model or its images are loading. */
  editable: boolean;
  commit: Commit;
  onError: (message: string) => void;
  /** The person asked to replace an image on the slide: choose one, then call `replaceImage`. */
  onPickImage: () => void;
}) {
  const [session, setSession] = useState<Session | null>(null);
  const sessionRef = useRef<Session | null>(null);
  const ops = useRef<SlideOp[]>([]);
  // The image a pick is for, and the session that asked: an upload that ends
  // after the person moved on must not land on another slide.
  const picked = useRef<{ key: string; id: string } | null>(null);
  // Bumped to rebuild the frame when nothing else about the slide says so.
  const [epoch, setEpoch] = useState(0);

  const open = useCallback((next: Session | null) => {
    ops.current = [];
    sessionRef.current = next;
    setSession(next);
  }, []);

  /** Save what was done on the slide. Null when there is nothing new; the promise never rejects. */
  const flush = useCallback((): Promise<void> | null => {
    const s = sessionRef.current;
    if (!s || ops.current.length === 0) return null;
    const edited = applySlideOps(s.base, ops.current);
    // Nothing new since the last save (or typed and put back).
    if (edited === applySlideOps(s.written, [])) return null;
    return commit((latest) => {
      const now = splitSlides(latest)[s.index];
      if (now === undefined || clearRefined(now) !== s.written) {
        throw new SlideEditConflict('This slide changed while you were editing it, so your change was not saved.');
      }
      s.written = edited;
      return replaceSlide(clearRefined(latest), s.index, markRefined(edited));
    }, `Edited slide ${s.index + 1}`).catch((e) => {
      onError(e instanceof SlideEditConflict ? e.message : 'Your change could not be saved. Try again.');
    });
  }, [commit, onError]);

  /**
   * The picked image now shows `src` (an `asset:<id>` reference). Saved at
   * once, and the frame is rebuilt so it shows the new image.
   */
  const replaceImage = useCallback((src: string) => {
    const s = sessionRef.current;
    const pick = picked.current;
    picked.current = null;
    // No image was picked (or this pick was already answered): nothing to replace.
    if (!pick) return;
    if (!s || pick.key !== s.key) {
      onError('The slide changed before the image was uploaded, so it was not replaced.');
      return;
    }
    ops.current.push({ op: 'src', id: pick.id, src });
    void flush();
    open(null);
    setEpoch((n) => n + 1);
  }, [flush, open, onError]);

  const slide = slides[shown];
  const view = viewSlides[shown];
  useEffect(() => {
    const s = sessionRef.current;
    if (!editable || slide === undefined || view === undefined) {
      if (s) {
        void flush();
        open(null);
      }
      return;
    }
    // Our own save (or nothing at all) changed this slide: keep the frame.
    if (s && s.index === shown && clearRefined(slide) === s.written) return;
    // Moving to another slide saves this one first. When the slide itself
    // changed some other way (Undo, a model edit), what was not saved yet is
    // dropped with the frame that showed it.
    if (s && s.index !== shown) void flush();
    const base = clearRefined(slide);
    open({ index: shown, base, written: base, view: clearRefined(view), key: newKey() });
  }, [editable, shown, slide, view, flush, open, epoch]);

  // Closing the studio unmounts it: this is what saves what was done then.
  const flushRef = useRef(flush);
  useEffect(() => {
    flushRef.current = flush;
  }, [flush]);
  useEffect(() => () => {
    void flushRef.current();
  }, []);

  const pickRef = useRef(onPickImage);
  useEffect(() => {
    pickRef.current = onPickImage;
  }, [onPickImage]);

  // Listening from mount and checking the CURRENT session leaves no gap: a
  // listener attached per session (after its frame rendered) could miss the
  // frame's first message.
  useEffect(() => {
    const onMessage = (e: MessageEvent) => {
      const s = sessionRef.current;
      const d = e.data as { t?: unknown; id?: unknown; op?: unknown; k?: unknown; html?: unknown } | null;
      if (!s || !d || d.t !== SLIDE_EDIT_MSG || d.id !== s.key || typeof d.op !== 'string' || !OPS.has(d.op)) return;
      if (d.op === 'commit') {
        void flush();
        return;
      }
      if (typeof d.k !== 'string' || !ELEMENT_ID.test(d.k)) return;
      if (d.op === 'pick') {
        picked.current = { key: s.key, id: d.k };
        pickRef.current();
      } else if (d.op === 'remove') {
        ops.current.push({ op: 'remove', id: d.k });
      } else if (typeof d.html === 'string' && d.html.length <= MAX_HTML) {
        // Only an element's latest content matters; keep it in the order it arrived.
        const id = d.k;
        ops.current = ops.current.filter((o) => !(o.op === 'html' && o.id === id));
        ops.current.push({ op: 'html', id, html: d.html });
      }
    };
    window.addEventListener('message', onMessage);
    return () => window.removeEventListener('message', onMessage);
  }, [flush]);

  const stage = useMemo(() => (session ? editableStage(session.view, session.key) : null), [session]);

  return { stage, frameKey: session?.key, flush, replaceImage };
}
