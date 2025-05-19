import { Tool } from '../../types/tool';
import { useJobManagementStore } from '../../store/jobManagement';

interface SimplifiedTool {
  id?: string;
  title: string;
  description: string;
  icon: string;
  category?: 'PreBuilt' | 'Custom' | 'UnityCatalog';
  enabled?: boolean;
}

// Convert SimplifiedTool type back to full Tool type
const convertToFullTool = (simplifiedTool: SimplifiedTool): Tool => ({
  ...simplifiedTool,
  config: {},
});

export const useJobManagement = () => {
  const {
    jobId,
    isRunning,
    selectedModel,
    planningEnabled,
    schemaDetectionEnabled,
    tools: simplifiedTools,
    selectedTools,
    setJobId,
    setIsRunning,
    setSelectedModel,
    setPlanningEnabled,
    setSchemaDetectionEnabled,
    setTools,
    setSelectedTools,
  } = useJobManagementStore();

  // Convert simplified tools back to full tools for component use
  const tools = simplifiedTools.map(convertToFullTool);

  return {
    jobId,
    isRunning,
    selectedModel,
    planningEnabled,
    schemaDetectionEnabled,
    tools,
    selectedTools,
    setJobId,
    setIsRunning,
    setSelectedModel,
    setPlanningEnabled,
    setSchemaDetectionEnabled,
    setTools,
    setSelectedTools,
  };
}; 