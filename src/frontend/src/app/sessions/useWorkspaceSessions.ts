import { useEffect, useRef, useState } from 'react';
import { useSessionStore } from '../../features/chat/store/sessionStore';
import { useExecutionStore } from '../../features/chat/store/executionStore';
import { useAppStore } from '../../features/chat/store/appStore';
import { useTabManagerStore } from '../../store/tabManager';
import { useUILayoutStore } from '../../store/uiLayout';
import { switchWorkspaceMode } from './sessionNavigation';

/** Own session initialization once, outside either mode's mount lifecycle. */
export function useWorkspaceSessions() {
  const [groupId, setGroupId] = useState(() => localStorage.getItem('selectedGroupId') || '');
  const [loadError, setLoadError] = useState(false);
  useEffect(() => {
    let mounted = true;
    useAppStore.getState().init();
    const restore = async (initial: boolean) => {
      try {
        if (initial) await useSessionStore.getState().init();
        else await useSessionStore.getState().reloadForGroup();
        const sid = useSessionStore.getState().currentSessionId;
        if (sid) useExecutionStore.getState().restoreSessionState(sid);
        else useExecutionStore.getState().resetForSession();
        if (mounted) setLoadError(false);
      } catch { if (mounted) setLoadError(true); }
    };
    void restore(true);
    const changeGroup = () => {
      const gid = localStorage.getItem('selectedGroupId') || '';
      setGroupId(gid);
      const mode = useUILayoutStore.getState().appMode;
      if (mode !== 'chat') switchWorkspaceMode(mode);
      void restore(false);
    };
    window.addEventListener('group-changed', changeGroup);
    return () => { mounted = false; window.removeEventListener('group-changed', changeGroup); };
  }, []);
  return { groupId, loadError };
}

/** Canvas selection restores its type; viewing Chat never changes the saved canvas type. */
export function useBuilderSessionMode() {
  const activeId = useTabManagerStore(state => state.activeTabId);
  const mode = useUILayoutStore(state => state.appMode);
  const initialized = useRef(false);
  const previousId = useRef(activeId);
  useEffect(() => {
    const tab = useTabManagerStore.getState().getActiveTab();
    if (!initialized.current) {
      initialized.current = true;
      if (mode !== 'chat') switchWorkspaceMode(mode);
    } else if (mode !== 'chat' && activeId !== previousId.current && tab) {
      useUILayoutStore.getState().setAppMode(tab.viewMode);
    } else if (mode !== 'chat' && (!tab || tab.viewMode !== mode)) {
      switchWorkspaceMode(mode);
    }
    previousId.current = activeId;
  }, [activeId, mode]);
}
