/**
 * Full-mount regression: "Load from Catalog" on the flow canvas opens the FLOWS
 * tab (not crews). Mounts WorkflowDesigner, fires the left sidebar's catalog action, and asserts
 * the CrewFlowSelectionDialog opens pinned to the Flows tab. This exercises the
 * onLoadCrew JSX closure end to end (the only part not covered by the helper test).
 *
 * WorkflowDesigner is a 65-import monolith, so every heavy child and side-effecting
 * hook is stubbed; the real zustand stores and the real WorkflowEventHandlers
 * (openCatalogForCanvas, useCrewFlowDialogHandler) drive the behavior under test.
 */
import React from 'react';
import { render, screen, fireEvent } from '@testing-library/react';
import { describe, it, expect, beforeEach, vi } from 'vitest';
import { CATALOG_FLOWS_TAB, CATALOG_CREWS_TAB } from './WorkflowEventHandlers';

// ── Heavy child components → inert stubs ────────────────────────────────────
vi.mock('./WorkflowPanels', () => ({ default: () => null }));
vi.mock('../../features/workflow/assistant/ChatPanel', () => ({ default: () => null }));
vi.mock('../sessions/SessionSidebar', () => ({ default: ({ onOpenCatalog }: { onOpenCatalog: () => void }) => <button onClick={onOpenCatalog}>load-from-catalog</button> }));
vi.mock('../../features/groups/components/GroupSelector', () => ({ default: () => null }));
vi.mock('../../features/chat/ChatWorkspace', () => ({ default: () => null }));
vi.mock('../../features/workflow/scheduling/components/ScheduleDialog', () => ({ default: () => null }));
vi.mock('../../features/executions/components/InputVariablesDialog', () => ({ InputVariablesDialog: () => null }));
vi.mock('../../features/help/tutorial/InteractiveTutorial', () => ({ default: () => null }));
vi.mock('../../features/configuration/components/APIKeys/APIKeys', () => ({ default: () => null }));
vi.mock('../../features/executions/components/LLMLogs', () => ({ default: () => null }));
vi.mock('../../features/executions/components/ShowLogs', () => ({ default: () => null }));
vi.mock('../../features/configuration/components/Configuration', () => ({ default: () => null }));
vi.mock('../../features/tools/components/ToolForm', () => ({ default: () => null }));
vi.mock('../../features/workflow/crews/components/SaveCrew', () => ({ default: () => null }));
vi.mock('../../features/workflow/flows/components/SaveFlow', () => ({ default: () => null }));
vi.mock('../Flow/CheckpointResumeDialog', () => ({ default: () => null }));
vi.mock('../../features/workflow/crews/components/TrifectaWarningDialog', () => ({ default: () => null }));

vi.mock('../sessions/SessionLibrary', () => ({ default: () => null }));

// CrewFlowSelectionDialog → surface the props the closure sets.
vi.mock('../../features/workflow/crews/components/CrewFlowDialog/index', () => ({
  CrewFlowSelectionDialog: (props: { isOpen?: boolean; open?: boolean; embedded?: boolean; showOnlyTab?: number; initialTab?: number }) => (
    <div
      data-testid="catalog-dialog"
      data-embedded={String(props.embedded)}
      data-open={String(props.isOpen ?? props.open ?? false)}
      data-showonlytab={String(props.showOnlyTab)}
      data-initialtab={String(props.initialTab)}
    />
  ),
}));

// ── Side-effecting / network hooks → minimal shapes ─────────────────────────
vi.mock('../../hooks/workflow/useThemeManager', () => ({ useThemeManager: () => ({ isDarkMode: false }) }));
vi.mock('../../hooks/workflow/useErrorManager', () => ({
  useErrorManager: () => ({ showError: false, errorMessage: '', handleCloseError: vi.fn(), showErrorMessage: vi.fn() }),
}));
vi.mock('../../hooks/workflow/useFlowManager', () => ({
  useFlowManager: () => ({
    nodes: [], edges: [], setNodes: vi.fn(), setEdges: vi.fn(),
    onNodesChange: vi.fn(), onEdgesChange: vi.fn(), onConnect: vi.fn(),
    handleEdgeContextMenu: vi.fn(), selectedEdges: [], setSelectedEdges: vi.fn(),
    manuallyPositionedNodes: new Set(),
  }),
}));
vi.mock('../../hooks/workflow/useBuilderCanvasSync', () => ({ useBuilderCanvasSync: () => ({ activeCanvasId: null }) }));
vi.mock('../../hooks/workflow/useBuilderExecutionSync', () => ({ useBuilderExecutionSync: () => undefined }));
vi.mock('../../hooks/workflow/useResponsiveLayout', () => ({ useResponsiveLayout: () => ({ isCompact: false, isMobile: false }) }));
vi.mock('../../hooks/workflow/useUIFitView', () => ({ useUIFitView: () => ({ handleUIAwareFitView: vi.fn(), handleFitViewToNodesInternal: vi.fn() }) }));
vi.mock('../../hooks/workflow/useWorkflowLayoutEvents', () => ({ useWorkflowLayoutEvents: () => undefined }));
vi.mock('../../hooks/workflow/useAgentManager', () => ({
  useAgentManager: () => ({ handleAgentSelect: vi.fn() }),
}));
vi.mock('../../hooks/workflow/useTaskManager', () => ({
  useTaskManager: () => ({ handleTaskSelect: vi.fn() }),
}));
vi.mock('./WorkflowPanelManager', () => ({
  PANEL_STATE: { LEFT: 'left', CENTER: 'center', RIGHT: 'right' },
  useNodePositioning: () => undefined,
  usePanelManager: () => ({
    isDraggingPanel: false, setIsDraggingPanel: vi.fn(), panelState: 'center',
    setPanelState: vi.fn(), handlePanelDragStart: vi.fn(),
    handleSnapToLeft: vi.fn(), handleSnapToRight: vi.fn(), handleResetPanel: vi.fn(),
  }),
}));
// dialogManager is read all over the JSX: action-like keys → no-op fns, flags → undefined (falsy).
vi.mock('./WorkflowDialogManager', () => ({
  useDialogManager: () =>
    new Proxy({}, {
      get: (_t, p) =>
        typeof p === 'string' && /^(set|open|close|handle|toggle|show|hide|on|fetch|add|create|update|delete|save|load|run|select|clear)/.test(p)
          ? vi.fn()
          : undefined,
    }),
}));
vi.mock('../../features/workflow/assistant/utils/chatHelpers', () => ({ handleNodesGenerated: vi.fn() }));
vi.mock('./WorkflowUtils', () => ({ setupResizeObserverErrorHandling: vi.fn() }));
vi.mock('../../api/execution/ExecutionLogs', () => ({ executionLogService: { getHistoricalLogs: vi.fn(), connectToLogs: vi.fn() } }));

import WorkflowDesigner from './WorkflowDesigner';
import { useUILayoutStore } from '../../store/uiLayout';
import { useBuilderCanvasStore } from '../sessions/builderCanvasStore';

describe('WorkflowDesigner - Load from Catalog matches the active canvas', () => {
  beforeEach(() => {
    useBuilderCanvasStore.setState({ canvases: [], activeCanvasId: null });
    useBuilderCanvasStore.getState().createCanvas('Canvas 1', 'flow');
  });

  it('opens the Flows tab when invoked on the flow canvas', () => {
    useUILayoutStore.setState({ areFlowsVisible: true, appMode: 'flow' });
    render(<WorkflowDesigner />);

    fireEvent.click(screen.getByText('load-from-catalog'));

    const dialog = screen.getAllByTestId('catalog-dialog').find(d => d.getAttribute('data-open') === 'true');
    expect(dialog).toBeTruthy();
    expect(dialog?.getAttribute('data-embedded')).toBe('true');
    expect(dialog?.getAttribute('data-showonlytab')).toBe(String(CATALOG_FLOWS_TAB));
  });

  it('opens the Crews tab when invoked on the crew canvas', () => {
    useUILayoutStore.setState({ areFlowsVisible: false, appMode: 'crew' });
    render(<WorkflowDesigner />);

    fireEvent.click(screen.getByText('load-from-catalog'));

    const dialog = screen.getAllByTestId('catalog-dialog').find(d => d.getAttribute('data-open') === 'true');
    expect(dialog).toBeTruthy();
    expect(dialog?.getAttribute('data-embedded')).toBe('true');
    expect(dialog?.getAttribute('data-initialtab')).toBe(String(CATALOG_CREWS_TAB));
  });
});
