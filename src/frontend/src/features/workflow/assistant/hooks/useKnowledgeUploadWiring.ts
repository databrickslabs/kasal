import { useMemo } from 'react';
import { toast } from 'react-hot-toast';
import { useBuilderCanvasStore } from '../../../../app/sessions/builderCanvasStore';
import type { Agent } from '../../../../api/workflow/AgentService';
import {
  showUploadedAgentsOnCanvas,
  unwireKnowledgeFileFromTasks,
  wireKnowledgeFileIntoTasks,
} from '../utils/knowledgeUploadWiring';

/**
 * The knowledge-upload callbacks, bound to the canvas on screen now.
 *
 * KnowledgeFileUpload keeps the callbacks of the render in which the files
 * were picked for its whole upload loop, so the canvas captured here is the
 * one the upload started on. If the user switches canvas before a file has
 * embedded, the workflow store holds another crew by then, and wiring the file
 * would attach it to the wrong tasks: skip it and say so.
 *
 * Detaching runs on the click, on the canvas on screen, and only touches tasks
 * that search that exact path.
 */
export function useKnowledgeUploadWiring() {
  const canvasId = useBuilderCanvasStore(state => state.activeCanvasId);
  return useMemo(() => {
    const stillOnCanvas = (path?: string) => {
      if (useBuilderCanvasStore.getState().activeCanvasId === canvasId) return true;
      if (path) {
        toast.error(`${path.split('/').pop()} finished uploading after you left its canvas. Attach it again there to search it.`);
      }
      return false;
    };
    return {
      onTasksUpdated: (path: string) => { if (stillOnCanvas(path)) wireKnowledgeFileIntoTasks(path); },
      onAgentsUpdated: (agents: Agent[]) => { if (stillOnCanvas()) showUploadedAgentsOnCanvas(agents); },
      onTaskFileRemoved: (path: string) => unwireKnowledgeFileFromTasks(path),
    };
  }, [canvasId]);
}
