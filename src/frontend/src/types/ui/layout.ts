/** Workspace chrome and panel state shared by the app and workflow canvas. */
export interface UILayoutState {
  // Screen dimensions
  screenWidth: number;
  screenHeight: number;

  // Fixed UI elements
  tabBarHeight: number;

  // Left sidebar
  leftSidebarVisible: boolean;
  leftSidebarExpanded: boolean;
  leftSidebarBaseWidth: number;    // Activity bar width (48px)
  leftSidebarExpandedWidth: number; // Full expanded width (280px)

  // Right sidebar
  rightSidebarVisible: boolean;
  rightSidebarWidth: number;       // Fixed width (48px)

  // Chat panel
  assistantPanelVisible?: boolean;
  assistantResponseFocused?: boolean;
  assistantPanelSide?: 'left' | 'right';
  assistantPanelRatio?: number;
  assistantDockHeight?: number;
  chatPanelVisible: boolean;
  chatPanelCollapsed: boolean;
  chatPanelWidth: number;          // Dynamic width when expanded
  chatPanelCollapsedWidth: number; // Width when collapsed (60px)
  chatPanelSide: 'left' | 'right'; // Which side the chat panel is docked to

  // Execution history
  executionHistoryVisible: boolean;
  executionHistoryHeight: number;  // Dynamic height

  // Panel splits (for dual canvas mode)
  panelPosition: number;           // 0-100% split between crew/flow panels
  areFlowsVisible: boolean;        // Whether flows panel is shown

  // Layout orientation for crew canvas
  layoutOrientation?: 'vertical' | 'horizontal';
}
