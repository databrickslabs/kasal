/**
 * The run-activity step that shows what Auto picked for a message or run.
 *
 * Kept apart from `autoModel.ts` so the chat store can use the pure helpers
 * there without loading the session store.
 */
import { useSessionStore } from '../../../app/sessions/sessionStore';
import type { ModelSelection } from '../types/execution';
import { modelSelectionDetail, modelSelectionLabel } from './autoModel';

export function modelSelectionTrace(selection: ModelSelection) {
  return {
    resultType: 'trace',
    resultData: {
      label: modelSelectionLabel(selection),
      sublabel: modelSelectionDetail(selection),
      source: 'model_selection',
      kind: 'event',
      timestamp: Date.now(),
    },
  };
}

/**
 * Post what Auto picked into the run activity of the session that asked
 * (not the one on screen, which the user may have switched to). A request
 * that named a model carries no selection, and posts nothing.
 */
export function postModelSelection(
  selection: ModelSelection | null | undefined,
  ownerSessionId?: string | null,
): void {
  if (!selection) return;
  const store = useSessionStore.getState();
  const extra = modelSelectionTrace(selection);
  if (ownerSessionId) store.addMessageToTargetSession(ownerSessionId, 'assistant', '', extra);
  else store.addMessage('assistant', '', extra);
}
