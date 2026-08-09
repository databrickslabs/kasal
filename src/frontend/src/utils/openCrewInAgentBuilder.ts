/**
 * Open a saved crew on the Agent Builder canvas.
 *
 * Reuses the `catalogLoadCrew` event the chat `/load` command already
 * dispatches, so WorkflowDesigner routes it through `handleCrewSelectWrapper` —
 * which creates a tab, forces the crew view, and lays the nodes out. Nothing new
 * is wired to open a crew; this only supplies the crew.
 *
 * Deliberately NOT the crew-library dialog's path. That one re-CREATES every
 * agent and task row and loads the copies, which is right for "start from this
 * crew" and wrong here: a flow's node points at a specific crew, and opening it
 * has to show that crew, not a duplicate that edits nowhere. It is also why this
 * does not go anywhere near `CrewFlowDialog.handleCrewSelect`.
 */

import { CrewService } from '../api/workflow/CrewService';
import { useCrewExecutionStore } from '../store/crewExecution';

export async function openCrewInAgentBuilder(
  crewId: string | number,
  fallbackName?: string,
): Promise<void> {
  try {
    const crew = await CrewService.getCrew(String(crewId));
    if (!crew?.nodes?.length) {
      throw new Error('This crew has no agents or tasks saved on it');
    }

    window.dispatchEvent(
      new CustomEvent('catalogLoadCrew', {
        detail: {
          nodes: crew.nodes,
          edges: crew.edges ?? [],
          name: crew.name || fallbackName,
          id: String(crew.id),
        },
      }),
    );
  } catch (error) {
    // Surfaced through the designer's existing snackbar. A node that silently
    // does nothing when clicked reads as a broken canvas, not a missing crew.
    const detail = error instanceof Error ? error.message : String(error);
    useCrewExecutionStore.getState().setErrorMessage(
      `Could not open "${fallbackName || crewId}" in the Agent Builder: ${detail}`,
    );
    useCrewExecutionStore.getState().setShowError(true);
  }
}
