import { beforeEach, describe, expect, it, vi } from 'vitest';
import { act, fireEvent, render, screen, waitFor } from '@testing-library/react';
import { CanvasAssistantLayout } from '../../../assistant/components/CanvasAssistantLayout';
import { useUILayoutStore } from '../../../../../store/uiLayout';
import { useAppStore } from '../../../../chat/store/appStore';
import { PublicationService } from '../../../../../api/workflow/PublicationService';
import { CrewExportService } from '../../../../../api/workflow/CrewExportService';
import { FlowService } from '../../../../../api/workflow/FlowService';
import PublishButton from './PublishButton';
import CrewCatalogActions from './CrewCatalogActions';
import CatalogSurface from './CatalogSurface';
import { CatalogNavigation } from './CatalogNavigation';
import type { CrewResponse } from '../../../../../types/workflow/crew';

vi.mock('../../../../../api/workflow/PublicationService', () => ({ PublicationService: {
  get: vi.fn(), publish: vi.fn(), unpublish: vi.fn(),
} }));
vi.mock('../../../../../api/workflow/FlowService', () => ({ FlowService: {
  getFlow: vi.fn(), updateFlowOutcomes: vi.fn(),
} }));
vi.mock('../../../../../api/workflow/CrewExportService', () => ({ CrewExportService: {
  listLakebaseInstances: vi.fn(async () => []), deployApp: vi.fn(), getAppDeploymentStatus: vi.fn(),
} }));
vi.mock('../../../../../api/config/ModelService', () => ({ ModelService: {
  getInstance: () => ({ getActiveModels: async () => ({}) }),
} }));
vi.stubGlobal('ResizeObserver', class { observe() {} unobserve() {} disconnect() {} });

const crew = { id: 'saved-crew', name: 'Research crew', nodes: [
  { type: 'taskNode', data: { description: 'Research {topic}', expected_output: 'A report' } },
] } as unknown as CrewResponse;
const layout = {
  composer: <input aria-label="Conversation draft" defaultValue="Keep my message" />,
  response: <p>Review the saved crew</p>, hasMessages: true, responseKey: 'reply', busy: false,
  dark: false, historyOpen: false, onHistory: vi.fn(), onNewChat: vi.fn(), sessionKey: 'crew:one',
};
function Workspace({ children }: { children: React.ReactNode }) {
  return <><div id="builder-assistant-response-host" /><div id="builder-assistant-composer-host" />
    <div><div>Canvas nodes</div><div id="builder-assistant-preview-host" /></div>
    <CanvasAssistantLayout {...layout} />
    <CatalogNavigation open><CatalogSurface open embedded tab={0} titleId="catalog-title" onClose={() => undefined} onEntered={() => undefined}>
      {children}
    </CatalogSurface></CatalogNavigation></>;
}
beforeEach(() => {
  vi.clearAllMocks();
  vi.mocked(PublicationService.get).mockResolvedValue(null);
  vi.mocked(PublicationService.publish).mockResolvedValue({} as never);
  useAppStore.setState({ loadCatalog: vi.fn(async () => undefined) });
  useUILayoutStore.setState({ assistantResponseFocused: false, assistantPanelSide: 'right', assistantPanelVisible: false, executionHistoryVisible: true });
});

describe('Catalog action panes', () => {
  it('keeps the publication draft across tabs and submits the selected destinations and inputs without loading the card', async () => {
    const loadCard = vi.fn();
    const cardKey = vi.fn();
    const changed = vi.fn();
    render(<Workspace><div onClick={loadCard} onKeyDown={cardKey}>
      <PublishButton entityType="crew" entityId={String(crew.id)} entityName={crew.name} nodes={crew.nodes} onChanged={changed} />
    </div></Workspace>);
    fireEvent.click(screen.getByRole('button', { name: 'Publish', exact: true }));
    const description = await screen.findByRole('textbox', { name: 'Description', exact: true });
    expect(screen.queryByRole('dialog')).toBeNull();
    expect(description.closest('#builder-assistant-preview-host')).not.toBeNull();
    fireEvent.click(description);
    fireEvent.keyDown(description, { key: 'ArrowRight' });
    fireEvent.change(description, { target: { value: 'Research a topic and deliver a sourced report.' } });
    fireEvent.click(screen.getByRole('checkbox', { name: 'MCP clients' }));
    fireEvent.click(screen.getByRole('checkbox', { name: 'Agent platforms' }));
    fireEvent.click(screen.getByRole('checkbox', { name: 'topic is required' }));
    fireEvent.click(screen.getByRole('checkbox', { name: 'Holds a conversation' }));
    fireEvent.click(screen.getByRole('tab', { name: 'Canvas', exact: true }));
    act(() => useUILayoutStore.getState().setAssistantPanelSide('left'));
    fireEvent.click(screen.getByRole('tab', { name: 'Crew catalog' }));
    expect(screen.getByRole('textbox', { name: 'Description', exact: true })).toBe(description);
    expect(description).toHaveValue('Research a topic and deliver a sourced report.');
    fireEvent.click(screen.getByRole('button', { name: 'Publish', exact: true }));
    await waitFor(() => expect(PublicationService.publish).toHaveBeenCalledWith('crew', 'saved-crew', expect.objectContaining({
      external_name: 'research_crew', description: 'Research a topic and deliver a sourced report.', protocols: ['chat'], conversational: true,
      input_schema: expect.objectContaining({ properties: expect.objectContaining({ topic: expect.anything() }) }),
    })));
    expect(vi.mocked(PublicationService.publish).mock.calls[0][2].input_schema?.required ?? []).not.toContain('topic');
    expect(changed).toHaveBeenCalledWith(true);
    expect(screen.queryByRole('button', { name: 'Back to Catalog' })).toBeNull();
    expect(screen.getAllByRole('tab')).toHaveLength(2);
    expect(loadCard).not.toHaveBeenCalled();
    expect(cardKey).not.toHaveBeenCalled();
  });

  it('opens deployment from the clicked catalog crew, retains configuration, and keeps the pane during an active request', async () => {
    let rejectDeployment!: (reason: Error) => void;
    vi.mocked(CrewExportService.deployApp).mockImplementation(() => new Promise((_, reject) => { rejectDeployment = reject; }));
    const loadCard = vi.fn();
    render(<Workspace><div onClick={loadCard}><CrewCatalogActions crew={crew} canEdit canDelete mlflowEnabled={false}
      published={false} onPublished={vi.fn()} onOptimize={vi.fn()} onExport={vi.fn()} onDelete={vi.fn()} /></div></Workspace>);
    fireEvent.click(screen.getByRole('button', { name: 'Deploy to Databricks Apps', exact: true }));
    const catalog = await screen.findByRole('textbox', { name: 'Catalog', exact: true });
    expect(catalog.closest('#builder-assistant-preview-host')).not.toBeNull();
    expect(screen.queryByRole('dialog')).toBeNull();
    fireEvent.change(catalog, { target: { value: 'research_data' } });
    fireEvent.click(screen.getByRole('tab', { name: 'Canvas', exact: true }));
    fireEvent.click(screen.getByRole('tab', { name: 'Crew catalog' }));
    expect(catalog).toHaveValue('research_data');
    fireEvent.click(screen.getByRole('button', { name: 'Deploy to Databricks Apps', exact: true }));
    await waitFor(() => expect(CrewExportService.deployApp).toHaveBeenCalledWith('saved-crew', expect.objectContaining({
      config: expect.objectContaining({ catalog: 'research_data', app_name: 'research-crew' }),
    })));
    fireEvent.click(screen.getByRole('button', { name: 'Back to Catalog' }));
    expect(screen.getByRole('region', { name: 'Deploy · Research crew' })).toBeInTheDocument();
    await act(async () => rejectDeployment(new Error('Deployment unavailable')));
    expect(await screen.findByText('Deployment unavailable')).toBeVisible();
    fireEvent.click(screen.getByRole('button', { name: 'Back to Catalog' }));
    await waitFor(() => expect(screen.queryByRole('region', { name: 'Deploy · Research crew' })).toBeNull());
    expect(loadCard).not.toHaveBeenCalled();
  });

  it('returns to Catalog and closes the child form instead of keeping an extra tab or draft', async () => {
    render(<Workspace><PublishButton entityType="crew" entityId="saved-crew" entityName="Research crew" nodes={crew.nodes} /></Workspace>);
    fireEvent.click(screen.getByRole('button', { name: 'Publish', exact: true }));
    fireEvent.change(await screen.findByRole('textbox', { name: 'Description', exact: true }), { target: { value: 'Discard this draft' } });
    expect(screen.getAllByRole('tab').map(tab => tab.textContent)).toEqual(['Canvas', 'Crew catalog']);
    fireEvent.click(screen.getByRole('button', { name: 'Back to Catalog' }));
    expect(screen.queryByRole('textbox', { name: 'Description', exact: true })).toBeNull();
    fireEvent.click(screen.getByRole('button', { name: 'Publish', exact: true }));
    expect(await screen.findByRole('textbox', { name: 'Description', exact: true })).toHaveValue('');
    expect(screen.getAllByRole('tab')).toHaveLength(2);
  });

  it('edits a published flow with its saved outcomes and unpublishes from the pane', async () => {
    vi.mocked(PublicationService.get).mockResolvedValue({ external_name: 'daily_brief', description: 'Daily briefing', protocols: ['chat'], input_schema: {} } as never);
    vi.mocked(FlowService.getFlow).mockResolvedValue({ flow_config: { outcomes: { Research: 'Sources and findings' } } } as never);
    const changed = vi.fn();
    render(<Workspace><PublishButton published entityType="flow" entityId="saved-flow" entityName="Daily brief"
      nodes={[{ type: 'crewNode', data: { crewName: 'Research' } }]} onChanged={changed} /></Workspace>);
    fireEvent.click(screen.getByRole('button', { name: 'Edit publication' }));
    expect(await screen.findByDisplayValue('Sources and findings')).toBeVisible();
    expect(screen.queryByRole('checkbox', { name: 'Holds a conversation' })).toBeNull();
    expect(screen.queryByRole('dialog')).toBeNull();
    fireEvent.click(screen.getByRole('button', { name: 'Unpublish' }));
    await waitFor(() => expect(PublicationService.unpublish).toHaveBeenCalledWith('flow', 'saved-flow'));
    expect(changed).toHaveBeenCalledWith(false);
    expect(screen.queryByRole('button', { name: 'Back to Catalog' })).toBeNull();
    expect(screen.getAllByRole('tab')).toHaveLength(2);
  });
});
