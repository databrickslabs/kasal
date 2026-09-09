import { useCallback, useEffect, useRef, useState } from 'react';
import { useSessionStore } from './sessionStore';
import { useExecutionStore } from '../../features/chat/store/executionStore';
import { useAppStore } from '../../features/chat/store/appStore';
import { useBuilderCanvasStore } from './builderCanvasStore';
import { useUILayoutStore } from '../../store/uiLayout';
import { switchWorkspaceMode } from './sessionNavigation';
import { useUserStore } from '../../store/user';
import { usePermissionStore } from '../../store/permissions';
import { connectBuilderSessions } from './builderSessionLifecycle';
import { describeSessionLoadError, SessionLoadError } from './sessionLoadError';

/** Own session initialization once, outside either mode's mount lifecycle. */
export function useWorkspaceSessions() {
  const [groupId, setGroupId] = useState(() => localStorage.getItem('selectedGroupId') || '');
  const [loadError, setLoadError] = useState<string | null>(null);
  const [loading, setLoading] = useState(false);
  const retryRef = useRef(() => {});
  const retry = useCallback(() => retryRef.current(), []);
  const user = useUserStore(state => state.currentUser?.email);
  const canBuild = usePermissionStore(state => state.allowAgentBuilder || state.allowFlowBuilder);
  useEffect(() => {
    let mounted = true;
    let version = 0;
    let disconnect = () => {};
    useAppStore.getState().init();
    const restore = async (initial: boolean) => {
      const current = ++version;
      disconnect();
      disconnect = () => {};
      setLoading(true);
      setLoadError(null);
      useBuilderCanvasStore.setState({ hydrated: false });
      const cancelled = () => !mounted || current !== version;
      const failures: string[] = [];
      try {
        try {
          if (initial) await useSessionStore.getState().init();
          else await useSessionStore.getState().reloadForGroup();
        } catch (cause) {
          if (cancelled()) return;
          failures.push(describeSessionLoadError(cause, 'list'));
          // The list is usable when only the selected conversation failed.
          if (!(cause instanceof SessionLoadError) || cause.stage !== 'messages') return;
        }
        if (cancelled()) return;
        if (canBuild) {
          try {
            const release = await connectBuilderSessions(
              localStorage.getItem('selectedGroupId') || '', useSessionStore.getState().sessions, cancelled,
              cause => { if (!cancelled()) failures.push(describeSessionLoadError(cause, 'builders')); });
            if (cancelled()) { release(); return; }
            disconnect = release;
            useBuilderCanvasStore.setState({ hydrated: true });
          } catch (cause) {
            if (cancelled()) return;
            failures.push(describeSessionLoadError(cause, 'builders'));
          }
        } else useBuilderCanvasStore.setState({ hydrated: true });
        // Chat state restoration is independent of builder hydration.
        const sid = useSessionStore.getState().currentSessionId;
        if (sid) useExecutionStore.getState().restoreSessionState(sid);
        else useExecutionStore.getState().resetForSession();
      } catch (cause) {
        if (!cancelled()) failures.push(describeSessionLoadError(cause, 'restore'));
      } finally {
        if (!cancelled()) {
          setLoadError(failures.length ? [...new Set(failures)].join(' ') : null);
          setLoading(false);
        }
      }
    };
    retryRef.current = () => { if (user) void restore(true); };
    if (user) void restore(true);
    const changeGroup = () => {
      const gid = localStorage.getItem('selectedGroupId') || '';
      setGroupId(gid);
      useBuilderCanvasStore.setState({ activeCanvasId: null, hydrated: false });
      void restore(false);
    };
    window.addEventListener('group-changed', changeGroup);
    return () => { mounted = false; version++; retryRef.current = () => {}; disconnect(); window.removeEventListener('group-changed', changeGroup); };
  }, [user, canBuild]);
  return { groupId, loadError, loading, retry };
}

/** Canvas selection restores its type; viewing Chat never changes the saved canvas type. */
export function useBuilderSessionMode() {
  const activeId = useBuilderCanvasStore(state => state.activeCanvasId);
  const mode = useUILayoutStore(state => state.appMode);
  const hydrated = useBuilderCanvasStore(state => state.hydrated);
  const initialized = useRef(false);
  const previousId = useRef(activeId);
  useEffect(() => {
    if (!hydrated) return;
    const tab = useBuilderCanvasStore.getState().getActiveCanvas();
    if (!initialized.current) {
      initialized.current = true;
      if (mode !== 'chat') switchWorkspaceMode(mode);
    } else if (mode !== 'chat' && activeId !== previousId.current && tab) {
      useUILayoutStore.getState().setAppMode(tab.viewMode);
    } else if (mode !== 'chat' && (!tab || tab.viewMode !== mode)) {
      switchWorkspaceMode(mode);
    }
    previousId.current = activeId;
  }, [activeId, mode, hydrated]);
}
