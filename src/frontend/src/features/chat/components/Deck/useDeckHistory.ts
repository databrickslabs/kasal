import { useCallback, useEffect } from 'react';
import { useDeckState, type DeckStudioStore } from './deckStudioStore';

type DeckUpdate = string | ((current: string) => string);

/** Generate slides concurrently, but serialize whole-deck writes and rebase each edit. */
export function useDeckHistory(store: DeckStudioStore, code: string, writeBack: (next: string, previous: string) => void | Promise<void>) {
  const [deck, setDeck] = useDeckState(store, 'deck');
  const [history, setHistory] = useDeckState(store, 'history');
  const [saving, setSaving] = useDeckState(store, 'saving');
  const { current, pending } = store.getState();

  useEffect(() => {
    // Reopening with the same prop must not overwrite work completed while closed.
    if (store.getState().source === code) return;
    current.current = code;
    store.setState({ source: code, deck: code });
  }, [store, code, current]);

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
  }, [writeBack, current, pending, setDeck, setSaving]);

  const commit = useCallback((update: DeckUpdate, label: string, done?: () => void) => save(update, (_, previous) => {
    setHistory(h => [...h, { label, prev: previous }]);
    done?.();
  }), [save, setHistory]);

  const undo = useCallback(() => {
    const last = history[history.length - 1];
    if (!last || pending.current) return Promise.resolve();
    return save(last.prev, () => setHistory(h => h.slice(0, -1)));
  }, [history, save, pending, setHistory]);

  return { deck, current, history, saving, pending, commit, undo };
}
