import { create } from 'zustand';
import { UILayoutState } from '../utils/CanvasLayoutManager';

interface UILayoutStore extends UILayoutState {
  // Actions to update the UI state
  updateScreenDimensions: (width: number, height: number) => void;
  setChatPanelWidth: (width: number) => void;
  setChatPanelCollapsed: (collapsed: boolean) => void;
  setChatPanelVisible: (visible: boolean) => void;
  setExecutionHistoryHeight: (height: number) => void;
  setExecutionHistoryVisible: (visible: boolean) => void;
  setLeftSidebarExpanded: (expanded: boolean) => void;
  setPanelPosition: (position: number) => void;
  setAreFlowsVisible: (visible: boolean) => void;
  
  // Computed getters
  getUILayoutState: () => UILayoutState;
}

export const useUILayoutStore = create<UILayoutStore>((set, get) => ({
  // Default UI state
  screenWidth: typeof window !== 'undefined' ? window.innerWidth : 1200,
  screenHeight: typeof window !== 'undefined' ? window.innerHeight : 800,
  tabBarHeight: 48,
  leftSidebarVisible: true,
  leftSidebarExpanded: false,
  leftSidebarBaseWidth: 48,
  leftSidebarExpandedWidth: 280,
  rightSidebarVisible: true,
  rightSidebarWidth: 48,
  chatPanelVisible: true,
  chatPanelCollapsed: false,
  chatPanelWidth: 450,
  chatPanelCollapsedWidth: 60,
  executionHistoryVisible: false,
  executionHistoryHeight: 60,
  panelPosition: 50,
  areFlowsVisible: true,

  // Actions
  updateScreenDimensions: (width: number, height: number) =>
    set({ screenWidth: width, screenHeight: height }),

  setChatPanelWidth: (width: number) =>
    set({ chatPanelWidth: width }),

  setChatPanelCollapsed: (collapsed: boolean) =>
    set({ chatPanelCollapsed: collapsed }),

  setChatPanelVisible: (visible: boolean) =>
    set({ chatPanelVisible: visible }),

  setExecutionHistoryHeight: (height: number) =>
    set({ executionHistoryHeight: height }),

  setExecutionHistoryVisible: (visible: boolean) =>
    set({ executionHistoryVisible: visible }),

  setLeftSidebarExpanded: (expanded: boolean) =>
    set({ leftSidebarExpanded: expanded }),

  setPanelPosition: (position: number) =>
    set({ panelPosition: position }),

  setAreFlowsVisible: (visible: boolean) =>
    set({ areFlowsVisible: visible }),

  // Computed getter that returns the current UI layout state
  getUILayoutState: (): UILayoutState => {
    const state = get();
    return {
      screenWidth: state.screenWidth,
      screenHeight: state.screenHeight,
      tabBarHeight: state.tabBarHeight,
      leftSidebarVisible: state.leftSidebarVisible,
      leftSidebarExpanded: state.leftSidebarExpanded,
      leftSidebarBaseWidth: state.leftSidebarBaseWidth,
      leftSidebarExpandedWidth: state.leftSidebarExpandedWidth,
      rightSidebarVisible: state.rightSidebarVisible,
      rightSidebarWidth: state.rightSidebarWidth,
      chatPanelVisible: state.chatPanelVisible,
      chatPanelCollapsed: state.chatPanelCollapsed,
      chatPanelWidth: state.chatPanelWidth,
      chatPanelCollapsedWidth: state.chatPanelCollapsedWidth,
      executionHistoryVisible: state.executionHistoryVisible,
      executionHistoryHeight: state.executionHistoryHeight,
      panelPosition: state.panelPosition,
      areFlowsVisible: state.areFlowsVisible,
    };
  },
}));

// Helper hook to get just the UI layout state for the canvas layout manager
export const useUILayoutState = (): UILayoutState => {
  return useUILayoutStore(state => state.getUILayoutState());
};