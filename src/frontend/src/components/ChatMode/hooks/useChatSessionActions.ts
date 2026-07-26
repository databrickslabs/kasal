/**
 * Session list interactions: new chat, switching, deleting, and inline rename.
 *
 * The rename and context-menu state live here because nothing else reads them —
 * they exist purely to drive the sidebar's own controls.
 */
import { useState, useCallback } from 'react';
import { useSessionStore } from '../store/sessionStore';
import { useExecutionStore } from '../store/executionStore';

interface UseChatSessionActionsArgs {
  setPendingRun: (v: { sessionId: string | null; label: string; run: () => void } | null) => void;
}

export function useChatSessionActions({ setPendingRun }: UseChatSessionActionsArgs) {

  const currentSessionId = useSessionStore((s) => s.currentSessionId);

  // --- Local UI state (sidebar-only concerns) ---
  const [contextMenu, setContextMenu] = useState<{ sessionId: string; x: number; y: number } | null>(null);

  const [renamingSessionId, setRenamingSessionId] = useState<string | null>(null);

  const [renameValue, setRenameValue] = useState('');

  // --- Session switching ---
  const handleNewChat = useCallback(() => {
    if (currentSessionId) {
      useExecutionStore.getState().saveSessionState(currentSessionId);
    }
    // Reset to a blank chat WITHOUT persisting a row — the session is created
    // (and titled) lazily on the first message. Eagerly creating here is what
    // left an empty "New Chat" sitting in the Recent rail beside the button.
    useSessionStore.getState().startNewChat();
    useExecutionStore.getState().resetForSession();
  }, [currentSessionId]);

  const handleSwitchSession = useCallback(async (sessionId: string) => {
    if (currentSessionId) {
      useExecutionStore.getState().saveSessionState(currentSessionId);
    }
    setPendingRun(null);
    await useSessionStore.getState().switchSession(sessionId);
    useExecutionStore.getState().restoreSessionState(sessionId);
  }, [currentSessionId, setPendingRun]);

  const handleDeleteSession = useCallback(async (sessionId: string) => {
    await useSessionStore.getState().deleteSession(sessionId);
    setContextMenu(null);
    useExecutionStore.getState().resetForSession();
  }, []);

  const handleStartRename = useCallback((sessionId: string, currentTitle: string) => {
    setRenamingSessionId(sessionId);
    setRenameValue(currentTitle);
    setContextMenu(null);
  }, []);

  const handleFinishRename = useCallback(async () => {
    if (renamingSessionId && renameValue.trim()) {
      await useSessionStore.getState().renameSession(renamingSessionId, renameValue.trim());
    }
    setRenamingSessionId(null);
    setRenameValue('');
  }, [renamingSessionId, renameValue]);

  return {
    handleNewChat,
    handleSwitchSession,
    handleDeleteSession,
    handleStartRename,
    handleFinishRename,
    renamingSessionId,
    setRenamingSessionId,
    renameValue,
    setRenameValue,
    contextMenu,
    setContextMenu,
  };
}
