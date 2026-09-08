import { useBuilderCanvasStore } from '../../../../app/sessions/builderCanvasStore';

export interface CanvasSaveCallbacks {
  onSaved?: (saved: { name: string }) => void;
  onError?: (error: unknown) => void;
}

/** Use the same SaveCrew/SaveFlow handlers as the sidebar, with an automatic name. */
export function saveCanvasToCatalog(flow: boolean, suggestedName: string): Promise<{ name: string }> {
  const tab = useBuilderCanvasStore.getState().getActiveCanvas();
  if (!tab) return Promise.reject(new Error('Open a canvas to save it to the catalog.'));
  const nodes = flow ? tab.flowNodes : tab.nodes;
  if (!nodes?.some(node => node.type === (flow ? 'crewNode' : 'agentNode'))) {
    return Promise.reject(new Error(`Open the ${flow ? 'flow' : 'crew'} on the canvas to save it to the catalog.`));
  }
  const task = nodes.find(node => node.type === 'taskNode');
  const canvasName = tab.name === 'Main Canvas' ? String(task?.data?.name || task?.data?.label || nodes[0]?.data?.label || tab.name) : tab.name;
  return new Promise((resolve, reject) => {
    const id = flow ? tab.savedFlowId : tab.savedCrewId;
    const kind = flow ? 'Flow' : 'Crew';
    window.dispatchEvent(new CustomEvent(id ? `updateExisting${kind}` : `openSave${kind}Dialog`, {
      detail: {
        tabId: tab.id,
        ...(flow ? { flowId: id } : { crewId: id }),
        suggestedName: (flow ? tab.savedFlowName : tab.savedCrewName) || suggestedName.trim() || canvasName,
        onSaved: resolve,
        onError: reject,
      },
    }));
  });
}
