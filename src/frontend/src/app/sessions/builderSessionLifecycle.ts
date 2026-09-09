import { useBuilderCanvasStore, type BuilderCanvas } from './builderCanvasStore';
import { useSessionPreferences } from './sessionPreferences';
import { builderSessionKey } from './sessionIndex';
import { useSessionStore } from './sessionStore';
import { useFlowStateStore } from '../../store/flowState';
import { useErrorStore } from '../../store/error';
import { useUserStore } from '../../store/user';
import type { ChatSession } from '../../features/chat/types/chat';
import { keepPending, legacyCanvases, legacyActiveCanvas, pendingCanvases, readCanvas, restoreCanvas,
  retireLegacyCanvas, snapshot, writeCanvas, type CanvasSnapshot } from './builderSessionPersistence';

const deletionBarriers = new Map<string, () => Promise<() => void>>();
let canvasOwner: string | null = null;
export async function pauseBuilderSessionWrites(id: string) {
  return await deletionBarriers.get(id)?.() || (() => {});
}

/** One lifecycle above the mode views; writes continue across session navigation. */
export async function connectBuilderSessions(group: string, sessions: ChatSession[], cancelled: () => boolean,
  onError: (cause: unknown) => void = () => {},
) {
  const user = useUserStore.getState().currentUser?.email;
  if (!user || !group) return () => {};
  if (canvasOwner !== user) {
    useBuilderCanvasStore.setState({ canvases: [], activeCanvasId: null, hydrated: false });
    canvasOwner = user;
  }
  const blocked = new Set<string>();
  const revisions = new Map<string, number>();
  const acknowledged = new Map<string, string>();
  const beforeLoad = new Map(useBuilderCanvasStore.getState().canvases.map(canvas =>
    [canvas.id, JSON.stringify(snapshot(canvas, useFlowStateStore.getState().getDeclared(canvas.id)))]));
  const imported: BuilderCanvas[] = [];
  const owned = () => !cancelled() && useUserStore.getState().currentUser?.email === user;
  const legacy = legacyCanvases(group);
  const legacyActive = legacyActiveCanvas();
  const pending = pendingCanvases(user, group);
  const ids = new Set([...sessions.filter(s => s.mode === 'crew' || s.mode === 'flow').map(s => s.id),
    ...legacy.map(canvas => canvas.chatSessionId || canvas.id), ...Object.keys(pending)]);
  // Read sequentially to bound DB/crypto work during large legacy imports.
  for (const id of ids) {
    if (!owned()) return () => {};
    try {
      const saved = await readCanvas(id, group);
      if (!owned()) return () => {};
      const old = legacy.find(canvas => (canvas.chatSessionId || canvas.id) === id);
      let state = saved?.state;
      let revision = saved?.revision || 0;
      if (pending[id]) {
        if (pending[id].revision !== revision) {
          // A refresh immediately after an acknowledged server write is harmless.
          if (JSON.stringify(saved?.state) !== JSON.stringify(pending[id].state))
            throw new Error('This session changed in another browser. Your unsaved canvas is retained locally.');
        } else {
          state = pending[id].state;
          revision = await writeCanvas(id, group, state, revision);
        }
        keepPending(user, group, id);
      } else if (!state && old) {
        state = { ...old, chatSessionId: id, group_id: group,
          declaredFlowState: useFlowStateStore.getState().getDeclared(old.id) };
        revision = await writeCanvas(id, group, state, revision);
      }
      if (!owned()) return () => {};
      if (!state) throw new Error('Saved builder canvas is unavailable.');
      const serverCanvas = restoreCanvas(state, group, id);
      const current = useBuilderCanvasStore.getState().getCanvas(serverCanvas.id);
      const editedDuringLoad = current && JSON.stringify(snapshot(current,
        useFlowStateStore.getState().getDeclared(current.id))) !== beforeLoad.get(current.id);
      const canvas = editedDuringLoad ? current : serverCanvas;
      imported.push(canvas);
      revisions.set(id, revision);
      if (!editedDuringLoad) {
        if (state.declaredFlowState) useFlowStateStore.getState().setDeclared(canvas.id, state.declaredFlowState);
        else useFlowStateStore.getState().clearDeclared(canvas.id);
      }
      acknowledged.set(id, JSON.stringify(snapshot(serverCanvas, state.declaredFlowState)));
      if (old) retireLegacyCanvas(old.id);
      const preferences = useSessionPreferences.getState();
      const key = builderSessionKey(canvas);
      const oldPreference = preferences.entries[`builder:${canvas.id}`];
      if (oldPreference && !preferences.entries[key]) preferences.update(key, oldPreference);
    } catch (cause) {
      if (!owned()) return () => {};
      blocked.add(id);
      onError(cause);
    }
  }
  if (!owned()) return () => {};
  const prior = useBuilderCanvasStore.getState();
  const importedIds = new Set(imported.map(canvas => canvas.id));
  const canvases = [...prior.canvases.filter(canvas => canvas.group_id !== group || !importedIds.has(canvas.id)), ...imported];
  const activeKey = `kasal-active-builder:${user}:${group}`;
  const restoredId = localStorage.getItem(activeKey) || legacyActive;
  const activeId = imported.find(canvas => canvas.id === restoredId)?.id ||
    canvases.find(canvas => canvas.id === prior.activeCanvasId && canvas.group_id === group && !blocked.has(canvas.chatSessionId || canvas.id))?.id || imported[0]?.id || null;
  useBuilderCanvasStore.setState({ canvases, activeCanvasId: activeId, hydrated: true, unavailableSessionIds: [...blocked] });

  const timers = new Map<string, ReturnType<typeof setTimeout>>();
  const writes = new Map<string, Promise<void>>();
  const latest = new Map<string, CanvasSnapshot>();
  const paused = new Set<string>();
  const registered = new Set<string>();
  let stopped = false;
  const publish = (id: string, state: CanvasSnapshot) => useSessionStore.setState(store => ({ sessions: [
    ...store.sessions.filter(session => session.id !== id),
    { id, title: state.name, mode: state.viewMode, groupId: group,
      createdAt: new Date(state.createdAt), updatedAt: new Date(state.lastModified) },
  ] }));
  // Existing metadata owns the timestamp; reading a canvas is not an edit.
  imported.filter(canvas => !useSessionStore.getState().sessions.some(session => session.id === canvas.chatSessionId))
    .forEach(canvas => publish(canvas.chatSessionId!, snapshot(canvas)));

  const flush = (id: string) => {
    if (stopped || !owned() || writes.has(id) || paused.has(id)) return;
    const state = latest.get(id);
    if (!state) return;
    latest.delete(id);
    const work = (async () => {
      try {
        const revision = await writeCanvas(id, group, state, revisions.get(id) || 0);
        revisions.set(id, revision);
        acknowledged.set(id, JSON.stringify(state));
        const next = latest.get(id);
        // Keep subsequent edits with the revision they must compare against.
        if (next) keepPending(user, group, id, { state: next, revision });
        else keepPending(user, group, id);
        if (owned()) publish(id, state);
      } catch {
        if (owned()) useErrorStore.getState().showErrorMessage('Session changes could not be saved. Your canvas is retained locally; reload to reconnect.');
        latest.delete(id); // Do not spin or overwrite a conflicting revision.
      } finally { writes.delete(id); if (latest.has(id)) flush(id); }
    })();
    writes.set(id, work);
  };
  const changed = () => {
    if (!owned() || stopped) return;
    const store = useBuilderCanvasStore.getState();
    if (store.activeCanvasId) localStorage.setItem(activeKey, store.activeCanvasId);
    for (const canvas of store.canvases.filter(c => c.group_id === group)) {
      const id = canvas.chatSessionId || canvas.id;
      if (paused.has(id) || blocked.has(id)) continue;
      deletionBarriers.set(id, async () => {
        paused.add(id);
        clearTimeout(timers.get(id)); timers.delete(id); latest.delete(id);
        await writes.get(id);
        keepPending(user, group, id);
        return () => { paused.delete(id); changed(); };
      });
      registered.add(id);
      if (canvas.isSessionDraft && !canvas.nodes.length && !canvas.flowNodes.length) continue;
      const state = snapshot(canvas, useFlowStateStore.getState().getDeclared(canvas.id));
      if (JSON.stringify(state) === acknowledged.get(id)) continue;
      latest.set(id, state);
      keepPending(user, group, id, { state, revision: revisions.get(id) || 0 });
      if (timers.has(id)) clearTimeout(timers.get(id));
      timers.set(id, setTimeout(() => { timers.delete(id); flush(id); }, 300));
    }
  };
  const unsubscribe = useBuilderCanvasStore.subscribe(changed);
  const unsubscribeFlow = useFlowStateStore.subscribe(changed);
  changed();
  return () => {
    stopped = true; unsubscribe(); unsubscribeFlow();
    timers.forEach(timer => clearTimeout(timer));
    registered.forEach(id => deletionBarriers.delete(id));
  };
}
