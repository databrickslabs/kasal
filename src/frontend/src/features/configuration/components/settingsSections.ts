export type SettingsScope = 'personal' | 'workspace' | 'system';
export type SettingsGroup = 'general' | 'ai' | 'connections' | 'administration';
export interface SettingsAccess {
  isSystemAdmin: boolean;
  isWorkspaceAdmin: boolean;
  isEditor: boolean;
}

/** Stable IDs own navigation; translated labels are presentation only. */
export const settingsSections = [
  { id: 'general', title: 'General', description: 'Make Kasal feel right for you.', group: 'general', scopes: ['personal'], key: 'general.title' },
  { id: 'overview', title: 'Overview', description: 'Your teamspace, connected services, and available capabilities.', group: 'general', scopes: ['workspace'], key: 'workspaceOverview.tab', admin: true },
  { id: 'models', title: 'Models', description: 'Choose the models available to your agents.', group: 'ai', scopes: ['workspace', 'system'], key: 'settings.models', admin: true },
  { id: 'tools', title: 'Tools', description: 'Manage the actions your agents can perform.', group: 'ai', scopes: ['workspace', 'system'], key: 'settings.tools', admin: true },
  { id: 'skills', title: 'Skills', description: 'Give agents reusable instructions and know-how.', group: 'ai', scopes: ['workspace'], key: 'skills.tab', admin: true },
  { id: 'prompts', title: 'Prompts', description: 'Review, refine, and optimize agent instructions.', group: 'ai', scopes: ['workspace'], key: 'prompts.tab' },
  { id: 'memory', title: 'Memory', description: 'Configure how agents store and retrieve knowledge.', group: 'ai', scopes: ['workspace'], key: 'memoryBackend.tab', admin: true },
  { id: 'ui', title: 'Output design', description: 'Create reusable layouts for your crew results.', group: 'ai', scopes: ['workspace'], key: 'settings.outputDesign', admin: true },
  { id: 'databricks', title: 'Databricks', description: 'Connect your workspace, data, and storage.', group: 'connections', scopes: ['workspace'], key: 'databricks.tab', admin: true },
  { id: 'mcp', title: 'MCP servers', description: 'Connect external tools and resources through MCP.', group: 'connections', scopes: ['workspace', 'system'], key: 'settings.mcp', admin: true },
  { id: 'remote-agents', title: 'Remote agents', description: 'Connect agents hosted outside this Kasal instance.', group: 'connections', scopes: ['workspace', 'system'], key: 'settings.remoteAgents', admin: true },
  { id: 'mlflow', title: 'MLflow', description: 'Connect tracing, experiments, and evaluation.', group: 'connections', scopes: ['workspace'], key: 'mlflow.tab', admin: true },
  { id: 'api-keys', title: 'API Keys', description: 'Manage credentials for your connected services.', group: 'connections', scopes: ['workspace'], key: 'apiKeys.tab' },
  { id: 'access', title: 'Access', description: 'Manage people, teamspaces, and their permissions.', group: 'administration', scopes: ['system'], key: 'access.tab' },
  { id: 'engines', title: 'Engines', description: 'Manage execution engines and available features.', group: 'administration', scopes: ['system'], key: 'engines.tab' },
  { id: 'database', title: 'Database', description: 'Manage system data and database operations.', group: 'administration', scopes: ['system'], key: 'settings.database' },
  { id: 'objects', title: 'Objects', description: 'Define reusable output schemas for consistent, structured task results.', group: 'administration', scopes: ['workspace'], key: 'settings.objects' },
] as const;
export type SettingsSection = typeof settingsSections[number];
export type SettingsSectionId = SettingsSection['id'];

export function getSettingsSections(scope: SettingsScope, access: SettingsAccess): SettingsSection[] {
  if (scope === 'system' && !access.isSystemAdmin) return [];
  if (scope === 'workspace' && !access.isWorkspaceAdmin && !access.isEditor) return [];
  return settingsSections.filter(section =>
    (section.scopes as readonly string[]).includes(scope) &&
    (scope !== 'workspace' || !('admin' in section) || access.isWorkspaceAdmin),
  );
}
