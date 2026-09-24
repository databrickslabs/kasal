import { useCallback, useEffect, useRef, useState } from 'react';

interface HistoryEntry { label: string; prev: string }
type DeckUpdate = string | ((current: string) => string);

/** Generate slides concurrently, but serialize whole-deck writes and rebase each edit. */
export function useDeckHistory(code: string, writeBack: (next: string, previous: string) => void | Promise<void>) {
  const [deck, setDeck] = useState(code);
  const current = useRef(code);
  const [history, setHistory] = useState<HistoryEntry[]>([]);
  const [saving, setSaving] = useState(false);
  const pending = useRef<Promise<void> | null>(null);

  useEffect(() => { current.current = code; setDeck(code); }, [code]);

  const save = useCallback((update: DeckUpdate, done: (next: string, previous: string) => void): Promise<void> => {
    const run = () => {
      const previous = current.current;
      const next = typeof update === 'function' ? update(previous) : update;
      const finish = () => {
        current.current = next;
        setDeck(next);
        done(next, previous);
      };
      const result = writeBack(next, previous);
      if (result) return result.then(finish);
      finish();
    };
    try {
      const result = pending.current ? pending.current.catch(() => {}).then(run) : run();
      if (!result) return Promise.resolve();
      setSaving(true);
      const tracked = result.finally(() => {
        if (pending.current === tracked) { pending.current = null; setSaving(false); }
      });
      pending.current = tracked;
      return tracked;
    } catch (error) {
      return Promise.reject(error);
    }
  }, [writeBack]);

  const commit = useCallback((update: DeckUpdate, label: string, done?: () => void) => save(update, (_, previous) => {
    setHistory(h => [...h, { label, prev: previous }]);
    done?.();
  }), [save]);

  const undo = useCallback(() => {
    const last = history[history.length - 1];
    if (!last || pending.current) return Promise.resolve();
    return save(last.prev, () => setHistory(h => h.slice(0, -1)));
  }, [history, save]);

  return { deck, current, history, saving, pending, commit, undo };
}
