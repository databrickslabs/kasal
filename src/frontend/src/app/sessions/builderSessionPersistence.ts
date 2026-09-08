import { isAxiosError } from 'axios';
import { apiClient } from '../../shared/api/client';
import type { BuilderCanvas } from './builderCanvasStore';
import type { DeclaredFlowState } from '../../store/flowState';

export interface CanvasSnapshot extends Omit<BuilderCanvas, 'createdAt' | 'lastModified' | 'lastSavedAt' | 'lastExecutionTime'> {
  createdAt: string;
  lastModified: string;
  lastSavedAt?: string;
  lastExecutionTime?: string;
  declaredFlowState?: DeclaredFlowState;
}
export interface SavedCanvas { state: CanvasSnapshot | null; revision: number }
export interface PendingCanvas { state: CanvasSnapshot; revision: number }
export const legacyKey = 'tab-manager-storage'; // Migration source; entries retire after server success.
const key = (user: string, group: string) => `kasal-session-outbox:${user}:${group}`;

export function pendingCanvases(user: string, group: string): Record<string, PendingCanvas> {
  try { return JSON.parse(localStorage.getItem(key(user, group)) || '{}'); }
  catch { return {}; }
}
export function keepPending(user: string, group: string, id: string, value?: PendingCanvas) {
  const pending = pendingCanvases(user, group);
  if (value) pending[id] = value; else delete pending[id];
  if (Object.keys(pending).length) localStorage.setItem(key(user, group), JSON.stringify(pending));
  else localStorage.removeItem(key(user, group));
}

export function snapshot(canvas: BuilderCanvas, declaredFlowState?: DeclaredFlowState): CanvasSnapshot {
  // JSON removes ReactFlow callbacks; the canvas hooks restore them on load.
  return JSON.parse(JSON.stringify({ ...canvas, isActive: false, declaredFlowState }));
}
export function restoreCanvas(state: CanvasSnapshot, group: string, sessionId: string): BuilderCanvas {
  return { ...state, group_id: group, chatSessionId: sessionId,
    createdAt: new Date(state.createdAt), lastModified: new Date(state.lastModified),
    lastSavedAt: state.lastSavedAt ? new Date(state.lastSavedAt) : undefined,
    lastExecutionTime: state.lastExecutionTime ? new Date(state.lastExecutionTime) : undefined,
    nodes: state.nodes || [], edges: state.edges || [], flowNodes: state.flowNodes || [], flowEdges: state.flowEdges || [],
  };
}

export async function readCanvas(id: string, group: string): Promise<SavedCanvas | null> {
  try { return (await apiClient.get<SavedCanvas>(`/chat-history/sessions/${id}/canvas`, { headers: { group_id: group } })).data; }
  catch (error) { if (isAxiosError(error) && error.response?.status === 404) return null; throw error; }
}
export async function writeCanvas(id: string, group: string, state: CanvasSnapshot, revision: number) {
  const { data } = await apiClient.put<SavedCanvas>(`/chat-history/sessions/${id}/canvas`,
    { title: state.name, mode: state.viewMode, state, revision }, { headers: { group_id: group } });
  return data.revision;
}

export function legacyCanvases(group: string): CanvasSnapshot[] {
  const data = localStorage.getItem(legacyKey);
  if (!data) return [];
  const parsed = JSON.parse(data);
  return (parsed.state?.tabs || []).filter((canvas: CanvasSnapshot) => !canvas.group_id || canvas.group_id === group)
    .map((canvas: CanvasSnapshot) => ({ ...canvas, group_id: group, viewMode: canvas.viewMode || 'crew',
      nodes: canvas.nodes || [], edges: canvas.edges || [], flowNodes: canvas.flowNodes || [], flowEdges: canvas.flowEdges || [],
    }));
}
export function legacyActiveCanvas(): string | null {
  const data = localStorage.getItem(legacyKey);
  return data ? JSON.parse(data).state?.activeTabId || null : null;
}
export function retireLegacyCanvas(id: string) {
  const data = localStorage.getItem(legacyKey);
  if (!data) return;
  const parsed = JSON.parse(data);
  parsed.state.tabs = (parsed.state.tabs || []).filter((canvas: CanvasSnapshot) => canvas.id !== id);
  if (parsed.state.tabs.length) localStorage.setItem(legacyKey, JSON.stringify(parsed));
  else localStorage.removeItem(legacyKey);
}
