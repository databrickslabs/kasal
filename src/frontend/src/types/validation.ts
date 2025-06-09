import { Node, Edge } from 'reactflow';

export interface ValidationResult {
  isValid: boolean;
  errors: string[];
}

export interface NodeValidation {
  validateConnections: (nodes: Node[], edges: Edge[]) => ValidationResult;
} 