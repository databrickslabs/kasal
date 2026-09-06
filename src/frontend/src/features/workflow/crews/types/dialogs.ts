import type { Node, Edge } from 'reactflow';
import type React from 'react';

export interface CrewSelectionDialogProps {
  open: boolean;
  onClose: () => void;
  onCrewSelect: (nodes: Node[], edges: Edge[]) => void;
}

export interface SaveCrewProps {
  nodes: Node[];
  edges: Edge[];
  trigger: React.ReactElement;
}
