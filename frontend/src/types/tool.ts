// Define possible config value types
import { UCTool } from '../api/UCToolsService';

export type ConfigValue = string | number | boolean | null | ConfigValue[] | { [key: string]: ConfigValue };

export interface Tool {
  id?: string;
  title: string;
  description: string;
  icon: string;
  config?: Record<string, ConfigValue>;
  category?: 'PreBuilt' | 'Custom' | 'UnityCatalog';
  enabled?: boolean;
}

export interface SavedToolsProps {
  refreshTrigger?: number;
}

export interface ToolIcon {
  value: string;
  label: string;
}

export interface UCToolsProps {
  tools: UCTool[];
  loading: boolean;
  error: string | null;
} 