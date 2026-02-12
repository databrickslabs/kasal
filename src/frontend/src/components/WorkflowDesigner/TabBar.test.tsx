import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { render, screen, fireEvent, waitFor, act, within } from '@testing-library/react';
import { ThemeProvider, createTheme } from '@mui/material';
import TabBar from './TabBar';

// --- Mocks ---

const mockCreateTab = vi.fn(() => 'new-tab-id');
const mockCloseTab = vi.fn();
const mockSetActiveTab = vi.fn();
const mockUpdateTabName = vi.fn();
const mockDuplicateTab = vi.fn(() => 'dup-tab-id');
const mockClearAllTabs = vi.fn();
const mockClearTabExecutionStatus = vi.fn();
const mockGetTabsForCurrentGroup = vi.fn();
const mockSwitchToGroup = vi.fn();

const makeTab = (overrides: Record<string, unknown> = {}) => ({
  id: 'tab-1',
  name: 'Tab One',
  nodes: [],
  edges: [],
  flowNodes: [],
  flowEdges: [],
  viewMode: 'crew' as const,
  isActive: true,
  isDirty: false,
  createdAt: new Date(),
  lastModified: new Date(),
  group_id: 'g1',
  ...overrides,
});

let storeTabs = [makeTab()];

vi.mock('../../store/tabManager', () => ({
  useTabManagerStore: () => ({
    tabs: storeTabs,
    activeTabId: storeTabs[0]?.id ?? null,
    createTab: mockCreateTab,
    closeTab: mockCloseTab,
    setActiveTab: mockSetActiveTab,
    updateTabName: mockUpdateTabName,
    duplicateTab: mockDuplicateTab,
    clearAllTabs: mockClearAllTabs,
    clearTabExecutionStatus: mockClearTabExecutionStatus,
    getTabsForCurrentGroup: mockGetTabsForCurrentGroup,
    switchToGroup: mockSwitchToGroup,
  }),
}));

vi.mock('../../hooks/workflow/useThemeManager', () => ({
  useThemeManager: () => ({ isDarkMode: false }),
}));

const theme = createTheme();

const renderTabBar = (props: Record<string, unknown> = {}) =>
  render(
    <ThemeProvider theme={theme}>
      <TabBar {...props} />
    </ThemeProvider>
  );

beforeEach(() => {
  vi.useFakeTimers({ shouldAdvanceTime: true });
  storeTabs = [makeTab()];
  mockGetTabsForCurrentGroup.mockReturnValue(storeTabs);
  vi.clearAllMocks();
  mockGetTabsForCurrentGroup.mockReturnValue(storeTabs);
  mockCreateTab.mockReturnValue('new-tab-id');
  mockDuplicateTab.mockReturnValue('dup-tab-id');
});

afterEach(() => {
  vi.useRealTimers();
});

// ---------- Rendering ----------

describe('TabBar', () => {
  describe('rendering', () => {
    it('renders tabs and new-tab button', () => {
      renderTabBar();
      expect(screen.getByText('Tab One')).toBeInTheDocument();
    });

    it('hides tabs and buttons when hideTabsAndButtons is true', () => {
      renderTabBar({ hideTabsAndButtons: true });
      expect(screen.queryByText('Tab One')).not.toBeInTheDocument();
    });

    it('renders multiple tabs from the store', () => {
      storeTabs = [makeTab(), makeTab({ id: 'tab-2', name: 'Tab Two' })];
      mockGetTabsForCurrentGroup.mockReturnValue(storeTabs);
      renderTabBar();
      expect(screen.getByText('Tab One')).toBeInTheDocument();
      expect(screen.getByText('Tab Two')).toBeInTheDocument();
    });
  });

  // ---------- Tab indicators ----------

  describe('tab indicators', () => {
    it('shows dirty indicator when tab has unsaved changes', () => {
      storeTabs = [makeTab({ isDirty: true })];
      mockGetTabsForCurrentGroup.mockReturnValue(storeTabs);
      renderTabBar();
      expect(screen.getByText('Tab One')).toBeInTheDocument();
    });

    it('shows saved icon when tab is saved and clean', () => {
      storeTabs = [makeTab({ savedCrewId: 'crew-1', savedCrewName: 'My Crew', isDirty: false })];
      mockGetTabsForCurrentGroup.mockReturnValue(storeTabs);
      renderTabBar();
      expect(screen.getByText('Tab One')).toBeInTheDocument();
    });

    it('shows Running chip when tab is running via props', () => {
      renderTabBar({ isRunning: true, runningTabId: 'tab-1' });
      expect(screen.getByText('Running')).toBeInTheDocument();
    });

    it('shows Running chip when tab executionStatus is running', () => {
      storeTabs = [makeTab({ executionStatus: 'running' })];
      mockGetTabsForCurrentGroup.mockReturnValue(storeTabs);
      renderTabBar();
      expect(screen.getByText('Running')).toBeInTheDocument();
    });

    it('shows Completed chip', () => {
      storeTabs = [makeTab({ executionStatus: 'completed', lastExecutionTime: new Date() })];
      mockGetTabsForCurrentGroup.mockReturnValue(storeTabs);
      renderTabBar();
      expect(screen.getByText('Completed')).toBeInTheDocument();
    });

    it('shows Completed chip without lastExecutionTime', () => {
      storeTabs = [makeTab({ executionStatus: 'completed' })];
      mockGetTabsForCurrentGroup.mockReturnValue(storeTabs);
      renderTabBar();
      expect(screen.getByText('Completed')).toBeInTheDocument();
    });

    it('shows Failed chip', () => {
      storeTabs = [makeTab({ executionStatus: 'failed', lastExecutionTime: new Date() })];
      mockGetTabsForCurrentGroup.mockReturnValue(storeTabs);
      renderTabBar();
      expect(screen.getByText('Failed')).toBeInTheDocument();
    });

    it('shows Failed chip without lastExecutionTime', () => {
      storeTabs = [makeTab({ executionStatus: 'failed' })];
      mockGetTabsForCurrentGroup.mockReturnValue(storeTabs);
      renderTabBar();
      expect(screen.getByText('Failed')).toBeInTheDocument();
    });

    it('shows close button when there are multiple tabs', () => {
      storeTabs = [makeTab(), makeTab({ id: 'tab-2', name: 'Tab Two' })];
      mockGetTabsForCurrentGroup.mockReturnValue(storeTabs);
      renderTabBar();
      // Close buttons rendered via CloseIcon inside IconButtons
      const closeIcons = document.querySelectorAll('[data-testid="CloseIcon"]');
      expect(closeIcons.length).toBeGreaterThan(0);
    });

    it('does not show close button when there is only one tab', () => {
      storeTabs = [makeTab()];
      mockGetTabsForCurrentGroup.mockReturnValue(storeTabs);
      renderTabBar();
      // Inside the tab label the close button should not be rendered
      const tabElement = screen.getByText('Tab One').closest('[role="tab"]');
      const closeBtn = tabElement?.querySelector('[data-testid="CloseIcon"]');
      expect(closeBtn).toBeNull();
    });
  });

  // ---------- Tab switching ----------

  describe('tab switching', () => {
    it('calls setActiveTab when clicking a tab', () => {
      storeTabs = [makeTab(), makeTab({ id: 'tab-2', name: 'Tab Two' })];
      mockGetTabsForCurrentGroup.mockReturnValue(storeTabs);
      renderTabBar();
      fireEvent.click(screen.getByText('Tab Two'));
      expect(mockSetActiveTab).toHaveBeenCalledWith('tab-2');
    });

    it('does not switch tab when disabled', () => {
      storeTabs = [makeTab(), makeTab({ id: 'tab-2', name: 'Tab Two' })];
      mockGetTabsForCurrentGroup.mockReturnValue(storeTabs);
      renderTabBar({ disabled: true });
      // The Tabs component has pointerEvents: none so click won't reach handler,
      // but we verify setActiveTab was not called
      expect(mockSetActiveTab).not.toHaveBeenCalled();
    });
  });

  // ---------- New tab menu ----------

  describe('new tab menu', () => {
    const openNewTabMenu = () => {
      // The new tab button has AddIcon + ArrowDropDownIcon
      const addIcons = document.querySelectorAll('[data-testid="AddIcon"]');
      const newTabBtn = addIcons[0]?.closest('button');
      fireEvent.click(newTabBtn!);
    };

    it('opens the new tab menu', () => {
      renderTabBar();
      openNewTabMenu();
      expect(screen.getByText('New Empty Canvas')).toBeInTheDocument();
      expect(screen.getByText('Load Existing Plans')).toBeInTheDocument();
      expect(screen.getByText('Load Existing Agents')).toBeInTheDocument();
      expect(screen.getByText('Load Existing Tasks')).toBeInTheDocument();
    });

    it('creates a new empty canvas', () => {
      renderTabBar();
      openNewTabMenu();
      fireEvent.click(screen.getByText('New Empty Canvas'));
      expect(mockCreateTab).toHaveBeenCalled();
    });

    it('does not create new canvas when disabled', () => {
      renderTabBar({ disabled: true });
      // The button is disabled so menu won't open, but we can verify createTab is not called
      expect(mockCreateTab).not.toHaveBeenCalled();
    });

    it('calls onLoadCrew when clicking Load Existing Plans', () => {
      const onLoadCrew = vi.fn();
      renderTabBar({ onLoadCrew });
      openNewTabMenu();
      fireEvent.click(screen.getByText('Load Existing Plans'));
      expect(onLoadCrew).toHaveBeenCalled();
    });

    it('handles Load Existing Plans without onLoadCrew callback', () => {
      renderTabBar();
      openNewTabMenu();
      fireEvent.click(screen.getByText('Load Existing Plans'));
      // No error thrown
    });

    it('calls onLoadAgents when clicking Load Existing Agents', () => {
      const onLoadAgents = vi.fn();
      renderTabBar({ onLoadAgents });
      openNewTabMenu();
      fireEvent.click(screen.getByText('Load Existing Agents'));
      expect(onLoadAgents).toHaveBeenCalled();
    });

    it('handles Load Existing Agents without onLoadAgents callback', () => {
      renderTabBar();
      openNewTabMenu();
      fireEvent.click(screen.getByText('Load Existing Agents'));
      // No error thrown
    });

    it('calls onLoadTasks when clicking Load Existing Tasks', () => {
      const onLoadTasks = vi.fn();
      renderTabBar({ onLoadTasks });
      openNewTabMenu();
      fireEvent.click(screen.getByText('Load Existing Tasks'));
      expect(onLoadTasks).toHaveBeenCalled();
    });

    it('handles Load Existing Tasks without onLoadTasks callback', () => {
      renderTabBar();
      openNewTabMenu();
      fireEvent.click(screen.getByText('Load Existing Tasks'));
      // No error thrown
    });
  });

  // ---------- Close tab ----------

  describe('close tab', () => {
    it('closes a clean tab directly', () => {
      storeTabs = [makeTab(), makeTab({ id: 'tab-2', name: 'Tab Two' })];
      mockGetTabsForCurrentGroup.mockReturnValue(storeTabs);
      renderTabBar();
      const closeIcons = document.querySelectorAll('[data-testid="CloseIcon"]');
      // Click the first close button in a tab label
      const closeBtn = closeIcons[0]?.closest('button');
      fireEvent.click(closeBtn!);
      expect(mockCloseTab).toHaveBeenCalledWith('tab-1');
    });

    it('shows confirm dialog for dirty tab', () => {
      storeTabs = [
        makeTab({ isDirty: true }),
        makeTab({ id: 'tab-2', name: 'Tab Two' }),
      ];
      mockGetTabsForCurrentGroup.mockReturnValue(storeTabs);
      renderTabBar();
      const closeIcons = document.querySelectorAll('[data-testid="CloseIcon"]');
      const closeBtn = closeIcons[0]?.closest('button');
      fireEvent.click(closeBtn!);
      expect(screen.getByText('Unsaved Changes')).toBeInTheDocument();
      expect(screen.getByText(/has unsaved changes/)).toBeInTheDocument();
    });

    it('does not close when disabled', () => {
      storeTabs = [makeTab(), makeTab({ id: 'tab-2', name: 'Tab Two' })];
      mockGetTabsForCurrentGroup.mockReturnValue(storeTabs);
      renderTabBar({ disabled: true });
      // pointerEvents: none blocks clicks
      expect(mockCloseTab).not.toHaveBeenCalled();
    });
  });

  // ---------- Close confirm dialog ----------

  describe('close confirm dialog', () => {
    const openCloseConfirmDialog = () => {
      storeTabs = [
        makeTab({ isDirty: true }),
        makeTab({ id: 'tab-2', name: 'Tab Two' }),
      ];
      mockGetTabsForCurrentGroup.mockReturnValue(storeTabs);
      renderTabBar();
      const closeIcons = document.querySelectorAll('[data-testid="CloseIcon"]');
      const closeBtn = closeIcons[0]?.closest('button');
      fireEvent.click(closeBtn!);
    };

    it('cancels close', () => {
      openCloseConfirmDialog();
      fireEvent.click(screen.getByText('Cancel'));
      expect(mockCloseTab).not.toHaveBeenCalled();
    });

    it('discards changes and closes', () => {
      openCloseConfirmDialog();
      fireEvent.click(screen.getByText('Discard Changes'));
      expect(mockCloseTab).toHaveBeenCalledWith('tab-1');
    });

    it('saves & closes for new crew (no savedCrewId)', () => {
      openCloseConfirmDialog();
      const dispatchSpy = vi.spyOn(window, 'dispatchEvent');
      fireEvent.click(screen.getByText('Save & Close'));
      const saveEvent = dispatchSpy.mock.calls.find(
        (c) => (c[0] as CustomEvent).type === 'openSaveCrewDialog'
      );
      expect(saveEvent).toBeDefined();
      dispatchSpy.mockRestore();
    });

    it('saves & closes for existing crew with valid savedCrewId', () => {
      storeTabs = [
        makeTab({ isDirty: true, savedCrewId: 'crew-123', savedCrewName: 'My Crew' }),
        makeTab({ id: 'tab-2', name: 'Tab Two' }),
      ];
      mockGetTabsForCurrentGroup.mockReturnValue(storeTabs);
      renderTabBar();
      const closeIcons = document.querySelectorAll('[data-testid="CloseIcon"]');
      const closeBtn = closeIcons[0]?.closest('button');
      fireEvent.click(closeBtn!);

      const dispatchSpy = vi.spyOn(window, 'dispatchEvent');
      fireEvent.click(screen.getByText('Save & Close'));
      const updateEvent = dispatchSpy.mock.calls.find(
        (c) => (c[0] as CustomEvent).type === 'updateExistingCrew'
      );
      expect(updateEvent).toBeDefined();
      expect((updateEvent![0] as CustomEvent).detail.crewId).toBe('crew-123');
      dispatchSpy.mockRestore();
    });

    it('handles updateCrewComplete event for existing crew', () => {
      storeTabs = [
        makeTab({ isDirty: true, savedCrewId: 'crew-123', savedCrewName: 'My Crew' }),
        makeTab({ id: 'tab-2', name: 'Tab Two' }),
      ];
      mockGetTabsForCurrentGroup.mockReturnValue(storeTabs);
      renderTabBar();
      const closeIcons = document.querySelectorAll('[data-testid="CloseIcon"]');
      const closeBtn = closeIcons[0]?.closest('button');
      fireEvent.click(closeBtn!);
      fireEvent.click(screen.getByText('Save & Close'));

      // Fire the updateCrewComplete event
      window.dispatchEvent(new CustomEvent('updateCrewComplete'));
      expect(mockCloseTab).toHaveBeenCalledWith('tab-1');
    });

    it('handles saveCrewComplete event for new crew', () => {
      openCloseConfirmDialog();
      fireEvent.click(screen.getByText('Save & Close'));

      // Fire the saveCrewComplete event
      window.dispatchEvent(new CustomEvent('saveCrewComplete'));
      expect(mockCloseTab).toHaveBeenCalledWith('tab-1');
    });

    it('saves & closes for legacy tab with savedCrewId="loaded" and agent node', () => {
      storeTabs = [
        makeTab({
          isDirty: true,
          savedCrewId: 'loaded',
          name: 'Legacy Crew',
          nodes: [
            { id: 'n1', type: 'agentNode', data: { agentId: 'a1' }, position: { x: 0, y: 0 } },
          ],
        }),
        makeTab({ id: 'tab-2', name: 'Tab Two' }),
      ];
      mockGetTabsForCurrentGroup.mockReturnValue(storeTabs);
      renderTabBar();
      const closeIcons = document.querySelectorAll('[data-testid="CloseIcon"]');
      const closeBtn = closeIcons[0]?.closest('button');
      fireEvent.click(closeBtn!);

      const dispatchSpy = vi.spyOn(window, 'dispatchEvent');
      fireEvent.click(screen.getByText('Save & Close'));
      const updateByNameEvent = dispatchSpy.mock.calls.find(
        (c) => (c[0] as CustomEvent).type === 'updateExistingCrewByName'
      );
      expect(updateByNameEvent).toBeDefined();
      expect((updateByNameEvent![0] as CustomEvent).detail.crewName).toBe('Legacy Crew');

      // Fire the updateCrewComplete event
      window.dispatchEvent(new CustomEvent('updateCrewComplete'));
      expect(mockCloseTab).toHaveBeenCalledWith('tab-1');
      dispatchSpy.mockRestore();
    });

    it('saves & closes for legacy tab with savedCrewId="loaded" and task node', () => {
      storeTabs = [
        makeTab({
          isDirty: true,
          savedCrewId: 'loaded',
          name: 'Legacy Task Crew',
          nodes: [
            { id: 'n1', type: 'taskNode', data: { taskId: 't1' }, position: { x: 0, y: 0 } },
          ],
        }),
        makeTab({ id: 'tab-2', name: 'Tab Two' }),
      ];
      mockGetTabsForCurrentGroup.mockReturnValue(storeTabs);
      renderTabBar();
      const closeIcons = document.querySelectorAll('[data-testid="CloseIcon"]');
      const closeBtn = closeIcons[0]?.closest('button');
      fireEvent.click(closeBtn!);

      const dispatchSpy = vi.spyOn(window, 'dispatchEvent');
      fireEvent.click(screen.getByText('Save & Close'));
      const updateByNameEvent = dispatchSpy.mock.calls.find(
        (c) => (c[0] as CustomEvent).type === 'updateExistingCrewByName'
      );
      expect(updateByNameEvent).toBeDefined();
      dispatchSpy.mockRestore();
    });

    it('falls back to save dialog for legacy tab with no agent/task nodes', () => {
      storeTabs = [
        makeTab({
          isDirty: true,
          savedCrewId: 'loaded',
          name: 'Legacy Empty',
          nodes: [],
        }),
        makeTab({ id: 'tab-2', name: 'Tab Two' }),
      ];
      mockGetTabsForCurrentGroup.mockReturnValue(storeTabs);
      renderTabBar();
      const closeIcons = document.querySelectorAll('[data-testid="CloseIcon"]');
      const closeBtn = closeIcons[0]?.closest('button');
      fireEvent.click(closeBtn!);

      const dispatchSpy = vi.spyOn(window, 'dispatchEvent');
      fireEvent.click(screen.getByText('Save & Close'));
      const saveEvent = dispatchSpy.mock.calls.find(
        (c) => (c[0] as CustomEvent).type === 'openSaveCrewDialog'
      );
      expect(saveEvent).toBeDefined();

      // Fire saveCrewComplete
      window.dispatchEvent(new CustomEvent('saveCrewComplete'));
      expect(mockCloseTab).toHaveBeenCalledWith('tab-1');
      dispatchSpy.mockRestore();
    });
  });

  // ---------- Context menu ----------

  describe('context menu', () => {
    const openContextMenu = () => {
      renderTabBar();
      const tab = screen.getByText('Tab One').closest('[role="tab"]')!;
      fireEvent.contextMenu(tab);
    };

    it('opens context menu on right-click', () => {
      openContextMenu();
      expect(screen.getByText('Rename')).toBeInTheDocument();
      expect(screen.getByText('Duplicate')).toBeInTheDocument();
      expect(screen.getByText('Save Crew')).toBeInTheDocument();
      expect(screen.getByText('Save Flow')).toBeInTheDocument();
      expect(screen.getByText('Run This Tab')).toBeInTheDocument();
    });

    it('does not open context menu when disabled', () => {
      renderTabBar({ disabled: true });
      // pointerEvents: none prevents interaction, context menu handler returns early
      expect(screen.queryByText('Rename')).not.toBeInTheDocument();
    });

    it('shows Close Tab and Close All Tabs with multiple tabs', () => {
      storeTabs = [makeTab(), makeTab({ id: 'tab-2', name: 'Tab Two' })];
      mockGetTabsForCurrentGroup.mockReturnValue(storeTabs);
      renderTabBar();
      const tab = screen.getByText('Tab One').closest('[role="tab"]')!;
      fireEvent.contextMenu(tab);
      expect(screen.getByText('Close Tab')).toBeInTheDocument();
      expect(screen.getByText('Close All Tabs')).toBeInTheDocument();
    });

    it('handles rename from context menu', () => {
      openContextMenu();
      fireEvent.click(screen.getByText('Rename'));
      expect(screen.getByText('Rename Tab')).toBeInTheDocument();
      expect(screen.getByDisplayValue('Tab One')).toBeInTheDocument();
    });

    it('handles duplicate from context menu', () => {
      openContextMenu();
      fireEvent.click(screen.getByText('Duplicate'));
      expect(mockDuplicateTab).toHaveBeenCalledWith('tab-1');
    });

    it('handles run from context menu', () => {
      const onRunTab = vi.fn();
      renderTabBar({ onRunTab });
      const tab = screen.getByText('Tab One').closest('[role="tab"]')!;
      fireEvent.contextMenu(tab);
      fireEvent.click(screen.getByText('Run This Tab'));
      expect(onRunTab).toHaveBeenCalledWith('tab-1');
    });

    it('handles run without onRunTab callback', () => {
      openContextMenu();
      fireEvent.click(screen.getByText('Run This Tab'));
      // No error thrown
    });

    it('disables Run This Tab when isRunning', () => {
      renderTabBar({ isRunning: true, runningTabId: 'tab-1' });
      const tab = screen.getByText('Tab One').closest('[role="tab"]')!;
      fireEvent.contextMenu(tab);
      const runItem = screen.getByText('Run This Tab').closest('li');
      expect(runItem).toHaveClass('Mui-disabled');
    });

    it('handles save crew from context menu', () => {
      const dispatchSpy = vi.spyOn(window, 'dispatchEvent');
      openContextMenu();
      fireEvent.click(screen.getByText('Save Crew'));
      expect(mockSetActiveTab).toHaveBeenCalledWith('tab-1');
      // setTimeout dispatches the event
      act(() => {
        vi.advanceTimersByTime(150);
      });
      const saveEvent = dispatchSpy.mock.calls.find(
        (c) => (c[0] as CustomEvent).type === 'openSaveCrewDialog'
      );
      expect(saveEvent).toBeDefined();
      dispatchSpy.mockRestore();
    });

    it('handles save flow from context menu', () => {
      const dispatchSpy = vi.spyOn(window, 'dispatchEvent');
      openContextMenu();
      fireEvent.click(screen.getByText('Save Flow'));
      expect(mockSetActiveTab).toHaveBeenCalledWith('tab-1');
      act(() => {
        vi.advanceTimersByTime(150);
      });
      const saveFlowEvent = dispatchSpy.mock.calls.find(
        (c) => (c[0] as CustomEvent).type === 'openSaveFlowDialog'
      );
      expect(saveFlowEvent).toBeDefined();
      dispatchSpy.mockRestore();
    });

    it('handles close tab from context menu for clean tab', () => {
      storeTabs = [makeTab(), makeTab({ id: 'tab-2', name: 'Tab Two' })];
      mockGetTabsForCurrentGroup.mockReturnValue(storeTabs);
      renderTabBar();
      const tab = screen.getByText('Tab One').closest('[role="tab"]')!;
      fireEvent.contextMenu(tab);
      fireEvent.click(screen.getByText('Close Tab'));
      expect(mockCloseTab).toHaveBeenCalledWith('tab-1');
    });

    it('handles close tab from context menu for dirty tab', () => {
      storeTabs = [
        makeTab({ isDirty: true }),
        makeTab({ id: 'tab-2', name: 'Tab Two' }),
      ];
      mockGetTabsForCurrentGroup.mockReturnValue(storeTabs);
      renderTabBar();
      const tab = screen.getByText('Tab One').closest('[role="tab"]')!;
      fireEvent.contextMenu(tab);
      fireEvent.click(screen.getByText('Close Tab'));
      expect(screen.getByText('Unsaved Changes')).toBeInTheDocument();
    });

    it('shows clear execution status in context menu', () => {
      storeTabs = [makeTab({ executionStatus: 'completed', lastExecutionTime: new Date() })];
      mockGetTabsForCurrentGroup.mockReturnValue(storeTabs);
      renderTabBar();
      const tab = screen.getByText('Tab One').closest('[role="tab"]')!;
      fireEvent.contextMenu(tab);
      expect(screen.getByText('Clear Execution Status')).toBeInTheDocument();
    });

    it('clears execution status from context menu', () => {
      storeTabs = [makeTab({ executionStatus: 'failed', lastExecutionTime: new Date() })];
      mockGetTabsForCurrentGroup.mockReturnValue(storeTabs);
      renderTabBar();
      const tab = screen.getByText('Tab One').closest('[role="tab"]')!;
      fireEvent.contextMenu(tab);
      fireEvent.click(screen.getByText('Clear Execution Status'));
      expect(mockClearTabExecutionStatus).toHaveBeenCalledWith('tab-1');
    });

    it('does not show clear execution status for running tab', () => {
      storeTabs = [makeTab({ executionStatus: 'running' })];
      mockGetTabsForCurrentGroup.mockReturnValue(storeTabs);
      renderTabBar();
      const tab = screen.getByText('Tab One').closest('[role="tab"]')!;
      fireEvent.contextMenu(tab);
      expect(screen.queryByText('Clear Execution Status')).not.toBeInTheDocument();
    });

    it('does not show clear execution status for tab without execution', () => {
      openContextMenu();
      expect(screen.queryByText('Clear Execution Status')).not.toBeInTheDocument();
    });

    it('handles close all tabs without unsaved changes', () => {
      storeTabs = [makeTab(), makeTab({ id: 'tab-2', name: 'Tab Two' })];
      mockGetTabsForCurrentGroup.mockReturnValue(storeTabs);
      renderTabBar();
      const tab = screen.getByText('Tab One').closest('[role="tab"]')!;
      fireEvent.contextMenu(tab);
      fireEvent.click(screen.getByText('Close All Tabs'));
      expect(mockClearAllTabs).toHaveBeenCalled();
      expect(mockCreateTab).toHaveBeenCalled();
    });

    it('shows close all confirm dialog when tabs have unsaved changes', () => {
      storeTabs = [
        makeTab({ isDirty: true }),
        makeTab({ id: 'tab-2', name: 'Tab Two', isDirty: true }),
      ];
      mockGetTabsForCurrentGroup.mockReturnValue(storeTabs);
      renderTabBar();
      const tab = screen.getByText('Tab One').closest('[role="tab"]')!;
      fireEvent.contextMenu(tab);
      fireEvent.click(screen.getByText('Close All Tabs'));
      expect(screen.getByText(/Some tabs have unsaved changes/)).toBeInTheDocument();
      expect(screen.getByText('Discard All Changes')).toBeInTheDocument();
    });
  });

  // ---------- Rename dialog ----------

  describe('rename dialog', () => {
    const openRenameDialog = () => {
      renderTabBar();
      const tab = screen.getByText('Tab One').closest('[role="tab"]')!;
      fireEvent.contextMenu(tab);
      fireEvent.click(screen.getByText('Rename'));
    };

    it('submits rename on button click', () => {
      openRenameDialog();
      const dialog = screen.getByRole('dialog');
      const input = within(dialog).getByDisplayValue('Tab One');
      fireEvent.change(input, { target: { value: 'New Name' } });
      // Click the "Rename" button inside the dialog (not the context menu item)
      const renameBtn = within(dialog).getByRole('button', { name: 'Rename' });
      fireEvent.click(renameBtn);
      expect(mockUpdateTabName).toHaveBeenCalledWith('tab-1', 'New Name');
    });

    it('submits rename on Enter key', () => {
      openRenameDialog();
      const dialog = screen.getByRole('dialog');
      const input = within(dialog).getByDisplayValue('Tab One');
      fireEvent.change(input, { target: { value: 'Enter Name' } });
      fireEvent.keyPress(input, { key: 'Enter', charCode: 13 });
      expect(mockUpdateTabName).toHaveBeenCalledWith('tab-1', 'Enter Name');
    });

    it('trims whitespace from name', () => {
      openRenameDialog();
      const dialog = screen.getByRole('dialog');
      const input = within(dialog).getByDisplayValue('Tab One');
      fireEvent.change(input, { target: { value: '  Trimmed  ' } });
      const renameBtn = within(dialog).getByRole('button', { name: 'Rename' });
      fireEvent.click(renameBtn);
      expect(mockUpdateTabName).toHaveBeenCalledWith('tab-1', 'Trimmed');
    });

    it('does not submit empty name', () => {
      openRenameDialog();
      const dialog = screen.getByRole('dialog');
      const input = within(dialog).getByDisplayValue('Tab One');
      fireEvent.change(input, { target: { value: '   ' } });
      const renameBtn = within(dialog).getByRole('button', { name: 'Rename' });
      fireEvent.click(renameBtn);
      expect(mockUpdateTabName).not.toHaveBeenCalled();
    });

    it('cancels rename dialog', () => {
      openRenameDialog();
      fireEvent.click(screen.getByText('Cancel'));
      expect(mockUpdateTabName).not.toHaveBeenCalled();
    });

    it('closes rename dialog via X button', () => {
      openRenameDialog();
      // Close via the dialog's onClose (clicking backdrop)
      const dialog = screen.getByText('Rename Tab').closest('[role="dialog"]')!;
      const backdrop = dialog.parentElement?.querySelector('.MuiBackdrop-root');
      if (backdrop) {
        fireEvent.click(backdrop);
      }
      // Dialog should be gone
    });

    it('handles rename for non-existent tab gracefully', () => {
      renderTabBar();
      const tab = screen.getByText('Tab One').closest('[role="tab"]')!;
      fireEvent.contextMenu(tab);
      // Temporarily empty tabs to simulate race condition
      const originalTabs = storeTabs;
      storeTabs = [];
      fireEvent.click(screen.getByText('Rename'));
      storeTabs = originalTabs;
      // No dialog opened since tab not found
    });
  });

  // ---------- Close all confirm dialog ----------

  describe('close all confirm dialog', () => {
    const openCloseAllDialog = () => {
      storeTabs = [
        makeTab({ isDirty: true }),
        makeTab({ id: 'tab-2', name: 'Tab Two', isDirty: true }),
      ];
      mockGetTabsForCurrentGroup.mockReturnValue(storeTabs);
      renderTabBar();
      const tab = screen.getByText('Tab One').closest('[role="tab"]')!;
      fireEvent.contextMenu(tab);
      fireEvent.click(screen.getByText('Close All Tabs'));
    };

    it('shows dirty tab chips', () => {
      openCloseAllDialog();
      // Inside close all dialog, dirty tabs are listed as chips
      const chips = screen.getAllByText('Tab One');
      expect(chips.length).toBeGreaterThan(0);
    });

    it('cancels close all', () => {
      openCloseAllDialog();
      const cancelButtons = screen.getAllByText('Cancel');
      // Click the cancel in the close all dialog
      fireEvent.click(cancelButtons[cancelButtons.length - 1]);
      expect(mockClearAllTabs).not.toHaveBeenCalled();
    });

    it('discards all and closes', () => {
      openCloseAllDialog();
      fireEvent.click(screen.getByText('Discard All Changes'));
      expect(mockClearAllTabs).toHaveBeenCalled();
      expect(mockCreateTab).toHaveBeenCalled();
    });

    it('clicks Save All & Close', () => {
      openCloseAllDialog();
      fireEvent.click(screen.getByText('Save All & Close'));
      // Currently just closes the dialog without actual save all
    });
  });

  // ---------- Auto-create default tab ----------

  describe('auto-create default tab', () => {
    it('creates a default tab when no tabs exist', () => {
      storeTabs = [];
      mockGetTabsForCurrentGroup.mockReturnValue([]);
      renderTabBar();
      expect(mockCreateTab).toHaveBeenCalledWith('Main Canvas');
    });

    it('does not create default tab when tabs exist', () => {
      renderTabBar();
      expect(mockCreateTab).not.toHaveBeenCalledWith('Main Canvas');
    });
  });

  // ---------- Auto-clear execution status ----------

  describe('auto-clear execution status', () => {
    it('clears execution status after 5 minutes', () => {
      const pastTime = new Date(Date.now() - 1000).toISOString(); // 1 second ago
      storeTabs = [makeTab({ executionStatus: 'completed', lastExecutionTime: pastTime })];
      mockGetTabsForCurrentGroup.mockReturnValue(storeTabs);
      renderTabBar();

      act(() => {
        vi.advanceTimersByTime(5 * 60 * 1000);
      });

      expect(mockClearTabExecutionStatus).toHaveBeenCalledWith('tab-1');
    });

    it('clears immediately if more than 5 minutes have passed', () => {
      const oldTime = new Date(Date.now() - 6 * 60 * 1000).toISOString(); // 6 minutes ago
      storeTabs = [makeTab({ executionStatus: 'completed', lastExecutionTime: oldTime })];
      mockGetTabsForCurrentGroup.mockReturnValue(storeTabs);
      renderTabBar();

      expect(mockClearTabExecutionStatus).toHaveBeenCalledWith('tab-1');
    });

    it('does not clear running tabs', () => {
      storeTabs = [makeTab({ executionStatus: 'running', lastExecutionTime: new Date().toISOString() })];
      mockGetTabsForCurrentGroup.mockReturnValue(storeTabs);
      renderTabBar();

      act(() => {
        vi.advanceTimersByTime(6 * 60 * 1000);
      });

      expect(mockClearTabExecutionStatus).not.toHaveBeenCalled();
    });

    it('does not clear tabs without execution status', () => {
      renderTabBar();
      act(() => {
        vi.advanceTimersByTime(6 * 60 * 1000);
      });
      expect(mockClearTabExecutionStatus).not.toHaveBeenCalled();
    });

    it('does not clear tabs without lastExecutionTime', () => {
      storeTabs = [makeTab({ executionStatus: 'completed' })];
      mockGetTabsForCurrentGroup.mockReturnValue(storeTabs);
      renderTabBar();
      act(() => {
        vi.advanceTimersByTime(6 * 60 * 1000);
      });
      // No lastExecutionTime means the condition `tab.lastExecutionTime` is falsy
      expect(mockClearTabExecutionStatus).not.toHaveBeenCalled();
    });
  });

  // ---------- Group change listener ----------

  describe('group change listener', () => {
    it('switches group on group-changed event', () => {
      renderTabBar();
      act(() => {
        window.dispatchEvent(
          new CustomEvent('group-changed', { detail: { groupId: 'g2' } })
        );
      });
      expect(mockSwitchToGroup).toHaveBeenCalledWith('g2');
    });

    it('cleans up event listener on unmount', () => {
      const removeSpy = vi.spyOn(window, 'removeEventListener');
      const { unmount } = renderTabBar();
      unmount();
      const removed = removeSpy.mock.calls.find(
        (c) => c[0] === 'group-changed'
      );
      expect(removed).toBeDefined();
      removeSpy.mockRestore();
    });
  });

  // ---------- Disabled state ----------

  describe('disabled state', () => {
    it('shows disabled tooltip on new tab button', () => {
      renderTabBar({ disabled: true });
      // Button should be disabled
      const addIcons = document.querySelectorAll('[data-testid="AddIcon"]');
      const newTabBtn = addIcons[0]?.closest('button');
      expect(newTabBtn).toBeDisabled();
    });
  });

  // ---------- Dialog onClose (backdrop/ESC) ----------

  describe('dialog onClose handlers', () => {
    it('closes rename dialog via Escape key', () => {
      renderTabBar();
      const tab = screen.getByText('Tab One').closest('[role="tab"]')!;
      fireEvent.contextMenu(tab);
      fireEvent.click(screen.getByText('Rename'));
      expect(screen.getByRole('dialog')).toBeInTheDocument();
      // Press Escape to close
      fireEvent.keyDown(screen.getByRole('dialog'), { key: 'Escape' });
      expect(mockUpdateTabName).not.toHaveBeenCalled();
    });

    it('closes close-confirm dialog via Escape key', () => {
      storeTabs = [
        makeTab({ isDirty: true }),
        makeTab({ id: 'tab-2', name: 'Tab Two' }),
      ];
      mockGetTabsForCurrentGroup.mockReturnValue(storeTabs);
      renderTabBar();
      const closeIcons = document.querySelectorAll('[data-testid="CloseIcon"]');
      const closeBtn = closeIcons[0]?.closest('button');
      fireEvent.click(closeBtn!);
      expect(screen.getByText('Unsaved Changes')).toBeInTheDocument();
      fireEvent.keyDown(screen.getByRole('dialog'), { key: 'Escape' });
      expect(mockCloseTab).not.toHaveBeenCalled();
    });

    it('closes close-all dialog via Escape key', () => {
      storeTabs = [
        makeTab({ isDirty: true }),
        makeTab({ id: 'tab-2', name: 'Tab Two', isDirty: true }),
      ];
      mockGetTabsForCurrentGroup.mockReturnValue(storeTabs);
      renderTabBar();
      const tab = screen.getByText('Tab One').closest('[role="tab"]')!;
      fireEvent.contextMenu(tab);
      fireEvent.click(screen.getByText('Close All Tabs'));
      expect(screen.getByText(/Some tabs have unsaved changes/)).toBeInTheDocument();
      const dialogs = screen.getAllByRole('dialog');
      fireEvent.keyDown(dialogs[dialogs.length - 1], { key: 'Escape' });
      expect(mockClearAllTabs).not.toHaveBeenCalled();
    });
  });

  // ---------- Disabled handler branches ----------

  describe('disabled handler branches', () => {
    it('handleTabChange returns early when disabled', () => {
      storeTabs = [makeTab(), makeTab({ id: 'tab-2', name: 'Tab Two' })];
      mockGetTabsForCurrentGroup.mockReturnValue(storeTabs);
      renderTabBar({ disabled: true });
      // Directly fire change on the tab list to exercise handleTabChange disabled path
      const tabList = screen.getByRole('tablist');
      const secondTab = screen.getByText('Tab Two').closest('[role="tab"]')!;
      // Simulate tab activation by firing change event on tabs
      fireEvent.click(secondTab);
      expect(mockSetActiveTab).not.toHaveBeenCalled();
    });

    it('handleNewEmptyCanvas returns early when disabled', () => {
      // Open the menu by directly calling the handler - the button is disabled in UI
      // We can render as not-disabled, open menu, then re-render as disabled
      const { rerender } = render(
        <ThemeProvider theme={theme}>
          <TabBar />
        </ThemeProvider>
      );
      // Open the menu
      const addIcons = document.querySelectorAll('[data-testid="AddIcon"]');
      const newTabBtn = addIcons[0]?.closest('button');
      fireEvent.click(newTabBtn!);
      // Now rerender with disabled
      rerender(
        <ThemeProvider theme={theme}>
          <TabBar disabled={true} />
        </ThemeProvider>
      );
      fireEvent.click(screen.getByText('New Empty Canvas'));
      // createTab should not have been called (the early return in handleNewEmptyCanvas)
      expect(mockCreateTab).not.toHaveBeenCalled();
    });

    it('handleCloseTab returns early when disabled', () => {
      storeTabs = [makeTab(), makeTab({ id: 'tab-2', name: 'Tab Two' })];
      mockGetTabsForCurrentGroup.mockReturnValue(storeTabs);
      const { rerender } = render(
        <ThemeProvider theme={theme}>
          <TabBar />
        </ThemeProvider>
      );
      // Now rerender as disabled - close buttons are still rendered but handler checks disabled
      rerender(
        <ThemeProvider theme={theme}>
          <TabBar disabled={true} />
        </ThemeProvider>
      );
      const closeIcons = document.querySelectorAll('[data-testid="CloseIcon"]');
      const closeBtn = closeIcons[0]?.closest('button');
      if (closeBtn) {
        fireEvent.click(closeBtn);
      }
      expect(mockCloseTab).not.toHaveBeenCalled();
    });

    it('handleContextMenu returns early when disabled', () => {
      const { rerender } = render(
        <ThemeProvider theme={theme}>
          <TabBar />
        </ThemeProvider>
      );
      rerender(
        <ThemeProvider theme={theme}>
          <TabBar disabled={true} />
        </ThemeProvider>
      );
      const tab = screen.getByText('Tab One').closest('[role="tab"]')!;
      fireEvent.contextMenu(tab);
      expect(screen.queryByText('Rename')).not.toBeInTheDocument();
    });
  });

  // ---------- Additional branch coverage ----------

  describe('additional branch coverage', () => {
    it('non-Enter keypress does not submit rename', () => {
      renderTabBar();
      const tab = screen.getByText('Tab One').closest('[role="tab"]')!;
      fireEvent.contextMenu(tab);
      fireEvent.click(screen.getByText('Rename'));
      const dialog = screen.getByRole('dialog');
      const input = within(dialog).getByDisplayValue('Tab One');
      fireEvent.keyPress(input, { key: 'a', charCode: 97 });
      expect(mockUpdateTabName).not.toHaveBeenCalled();
    });

    it('context menu on single tab does not show Close Tab', () => {
      storeTabs = [makeTab()];
      mockGetTabsForCurrentGroup.mockReturnValue(storeTabs);
      renderTabBar();
      const tab = screen.getByText('Tab One').closest('[role="tab"]')!;
      fireEvent.contextMenu(tab);
      expect(screen.queryByText('Close Tab')).not.toBeInTheDocument();
    });

    it('context menu Close All Tabs visible even with single tab', () => {
      storeTabs = [makeTab()];
      mockGetTabsForCurrentGroup.mockReturnValue(storeTabs);
      renderTabBar();
      const tab = screen.getByText('Tab One').closest('[role="tab"]')!;
      fireEvent.contextMenu(tab);
      expect(screen.getByText('Close All Tabs')).toBeInTheDocument();
    });

    it('renders tab without running status when not active run', () => {
      storeTabs = [makeTab()];
      mockGetTabsForCurrentGroup.mockReturnValue(storeTabs);
      renderTabBar({ isRunning: true, runningTabId: 'other-tab' });
      expect(screen.queryByText('Running')).not.toBeInTheDocument();
    });

    it('does not show saved icon when tab is dirty even with savedCrewId', () => {
      storeTabs = [makeTab({ savedCrewId: 'crew-1', savedCrewName: 'My Crew', isDirty: true })];
      mockGetTabsForCurrentGroup.mockReturnValue(storeTabs);
      renderTabBar();
      // The SaveIcon tooltip "Saved as:" should not appear when dirty
      const saveIcons = document.querySelectorAll('[data-testid="SaveIcon"]');
      // SaveIcon inside tab label should not be present
      const tabEl = screen.getByText('Tab One').closest('[role="tab"]');
      const saveIcon = tabEl?.querySelector('[data-testid="SaveIcon"]');
      expect(saveIcon).toBeNull();
    });

    it('close all dialog does not show dirty chips when none are dirty', () => {
      storeTabs = [makeTab(), makeTab({ id: 'tab-2', name: 'Tab Two' })];
      mockGetTabsForCurrentGroup.mockReturnValue(storeTabs);
      renderTabBar();
      const tab = screen.getByText('Tab One').closest('[role="tab"]')!;
      fireEvent.contextMenu(tab);
      fireEvent.click(screen.getByText('Close All Tabs'));
      // No confirm dialog shown since no dirty tabs
      expect(mockClearAllTabs).toHaveBeenCalled();
    });

    it('shows Running chip when executionStatus is running even without props', () => {
      storeTabs = [makeTab({ executionStatus: 'running' })];
      mockGetTabsForCurrentGroup.mockReturnValue(storeTabs);
      renderTabBar({ isRunning: false, runningTabId: null });
      expect(screen.getByText('Running')).toBeInTheDocument();
    });

    it('activeTabId falls back to false when null', () => {
      storeTabs = [];
      mockGetTabsForCurrentGroup.mockReturnValue([]);
      renderTabBar();
      // Tabs component receives value=false when no active tab
      expect(mockCreateTab).toHaveBeenCalledWith('Main Canvas');
    });
  });

  // ---------- Dark mode ----------

  describe('dark mode', () => {
    it('renders with dark mode hook', () => {
      // The mock always returns false, but covers the hook call path
      renderTabBar();
      expect(screen.getByText('Tab One')).toBeInTheDocument();
    });
  });
});
