import { Node, Edge } from 'reactflow';
import { AgentNodeData, TaskNodeData } from './crew';
import { Schedule, ScheduleCreate } from '../api/ScheduleService';

export type { Schedule, ScheduleCreate };

export interface ScheduleDialogProps {
  open: boolean;
  onClose: () => void;
  nodes: Node<AgentNodeData | TaskNodeData>[];
  edges: Edge[];
  planningEnabled: boolean;
  selectedModel: string;
}

export interface ConfigViewerDialogProps {
  open: boolean;
  onClose: () => void;
  schedule: Schedule | null;
} 