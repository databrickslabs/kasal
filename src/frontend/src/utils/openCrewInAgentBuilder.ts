/**
 * Open a saved crew on the Agent Builder canvas.
 *
 * Reuses the `catalogLoadCrew` event the chat `/load` command already
 * dispatches, so WorkflowDesigner routes it through `handleCrewSelectWrapper` —
 * which creates a tab, forces the crew view, and lays the nodes out. Nothing new
 * is wired to open a crew; this only supplies the crew.
 *
 * Opens the crew IN PLACE: its saved nodes and edges, bound to its id, so the
 * canvas edits the crew's own agent and task rows and Save updates the crew. A
 * flow's node points at a specific crew, and opening it has to show that crew.
 * The catalog card (`CrewFlowDialog.handleCrewSelect`) opens a crew the same
 * way; only its "Open as copy" action re-creates the rows
 * (`cloneCatalogCrew`), and that one binds no crew id.
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
