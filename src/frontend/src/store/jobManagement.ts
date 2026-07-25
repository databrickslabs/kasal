import { create } from 'zustand';
import { Tool } from '../types/tool';

// Simplified Tool type for store
interface SimplifiedTool {
  id?: string;
  title: string;
  description: string;
  icon: string;
  category?: 'PreBuilt' | 'Custom';
  enabled?: boolean;
}

interface JobManagementState {
  jobId: string | null;
  isRunning: boolean;
  selectedModel: string;
  schemaDetectionEnabled: boolean;
  tools: SimplifiedTool[];
  selectedTools: string[];
  
  // Actions
  setJobId: (id: string | null) => void;
  setIsRunning: (running: boolean) => void;
  setSelectedModel: (model: string) => void;
  setSchemaDetectionEnabled: (enabled: boolean) => void;
  setTools: (tools: Tool[]) => void;
  setSelectedTools: (tools: string[]) => void;
  resetJobManagement: () => void;
}

// Helper functions for tool conversion
const convertToSimplifiedTool = (tool: Tool): SimplifiedTool => ({
  id: tool.id,
  title: tool.title,
  description: tool.description,
  icon: tool.icon,
  category: tool.category,
  enabled: tool.enabled,
});

const initialState = {
  jobId: null,
  isRunning: false,
  selectedModel: '',
  schemaDetectionEnabled: true,
  tools: [],
  selectedTools: [],
};

export const useJobManagementStore = create<JobManagementState>((set) => ({
  ...initialState,

  setJobId: (id: string | null) => 
    set(() => ({ jobId: id })),

  setIsRunning: (running: boolean) => 
    set(() => ({ isRunning: running })),

  setSelectedModel: (model: string) => 
    set(() => ({ selectedModel: model })),

  setSchemaDetectionEnabled: (enabled: boolean) => 
    set(() => ({ schemaDetectionEnabled: enabled })),

  setTools: (tools: Tool[]) => 
    set(() => ({ 
      tools: tools.map(convertToSimplifiedTool) 
    })),

  setSelectedTools: (tools: string[]) => 
    set(() => ({ selectedTools: tools })),

  resetJobManagement: () => 
    set(() => ({ ...initialState })),
})); 