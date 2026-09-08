import { beforeEach, describe, expect, it, vi } from 'vitest';
import { render, screen, waitFor, within } from '@testing-library/react';
import WorkspaceOverview from './WorkspaceOverview';
import { apiClient } from '../../../shared/api/client';
import { DatabricksService } from '../../../api/databricks/DatabricksService';

vi.mock('../../../shared/api/client', () => ({ apiClient: { get: vi.fn() } }));
vi.mock('../../../api/databricks/DatabricksService', () => ({ DatabricksService: { getInstance: vi.fn() } }));
vi.mock('../../../api/groups/GroupService', () => ({ GroupService: { getInstance: () => ({ getMyGroups: async () => [] }) } }));
vi.mock('../../../api/memory/MemoryBackendService', () => ({ MemoryBackendService: { getConfig: async () => null } }));
vi.mock('../../../api/tools/ToolService', () => ({ ToolService: { listEnabledTools: async () => [] } }));
vi.mock('../../../api/tools/MCPService', () => ({ MCPService: { getInstance: () => ({ getMcpServers: async () => ({ servers: [] }) }) } }));
vi.mock('../../../store/permissions', () => ({ usePermissionStore: (select: (state: { userRole: string }) => unknown) => select({ userRole: 'admin' }) }));

beforeEach(() => {
  vi.clearAllMocks();
  localStorage.setItem('selectedGroupId', 'user_personal');
  vi.mocked(DatabricksService.getInstance).mockReturnValue({
    getDatabricksConfig: async () => ({ enabled: true, catalog: 'catalog', mlflow_enabled: false, evaluation_enabled: false }),
  } as unknown as DatabricksService);
});

const row = (label: string) => within(screen.getByText(label).closest('li')!);

describe('Overview MLflow settings', () => {
  it('shows installation defaults from the same settings endpoint as the MLflow page', async () => {
    vi.mocked(apiClient.get).mockResolvedValue({ data: { enabled: true, evaluation_enabled: false } });
    render(<WorkspaceOverview />);
    await screen.findByText('MLflow Tracing');
    expect(row('MLflow Tracing').getByText('Enabled')).toBeInTheDocument();
    expect(row('MLflow Evaluation').getByText('Disabled · opt-in')).toBeInTheDocument();
    expect(apiClient.get).toHaveBeenCalledWith('/mlflow/settings');
  });

  it('respects explicit tracing opt-out and evaluation opt-in despite stale Databricks flags', async () => {
    vi.mocked(DatabricksService.getInstance).mockReturnValue({
      getDatabricksConfig: async () => ({ enabled: true, mlflow_enabled: true, evaluation_enabled: false }),
    } as unknown as DatabricksService);
    vi.mocked(apiClient.get).mockResolvedValue({ data: { enabled: false, evaluation_enabled: true } });
    render(<WorkspaceOverview />);
    await screen.findByText('MLflow Tracing');
    expect(row('MLflow Tracing').getByText('Disabled')).toBeInTheDocument();
    expect(row('MLflow Evaluation').getByText('Enabled')).toBeInTheDocument();
  });

  it('reports an unavailable status instead of falsely saying disabled on request failure', async () => {
    vi.mocked(apiClient.get).mockRejectedValue(new Error('unavailable'));
    render(<WorkspaceOverview />);
    await screen.findByText('MLflow Tracing');
    expect(row('MLflow Tracing').getByText('Unavailable')).toBeInTheDocument();
  });

  it('ignores an older personal-space response after switching teamspace', async () => {
    let resolveOld!: (value: unknown) => void;
    vi.mocked(apiClient.get).mockReturnValueOnce(new Promise(resolve => { resolveOld = resolve; }))
      .mockResolvedValueOnce({ data: { enabled: false, evaluation_enabled: false } });
    const view = render(<WorkspaceOverview />);
    await screen.findByText('MLflow Tracing');
    localStorage.setItem('selectedGroupId', 'user_other');
    view.rerender(<WorkspaceOverview />);
    await waitFor(() => expect(row('MLflow Tracing').getByText('Disabled')).toBeInTheDocument());
    resolveOld({ data: { enabled: true, evaluation_enabled: true } });
    await waitFor(() => expect(row('MLflow Tracing').queryByText('Enabled')).not.toBeInTheDocument());
  });
});
