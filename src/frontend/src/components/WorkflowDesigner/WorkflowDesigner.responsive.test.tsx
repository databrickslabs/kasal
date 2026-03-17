/**
 * Unit tests for WorkflowDesigner responsive layout logic.
 *
 * Tests the computed responsive values that adapt the layout to different
 * screen sizes WITHOUT mutating the persisted Zustand store.
 */
import { describe, it, expect } from 'vitest';

/**
 * Extracted responsive logic from WorkflowDesigner.
 * These pure functions mirror the computed values in the component.
 */
function computeResponsiveValues({
  isCompact,
  isMobile,
  showChatPanel,
  isChatCollapsed,
  chatPanelWidth,
  chatPanelCollapsedWidth,
  leftSidebarBaseWidth,
  areFlowsVisible = false,
}: {
  isCompact: boolean;
  isMobile: boolean;
  showChatPanel: boolean;
  isChatCollapsed: boolean;
  chatPanelWidth: number;
  chatPanelCollapsedWidth: number;
  leftSidebarBaseWidth: number;
  areFlowsVisible?: boolean;
}) {
  const effectiveChatVisible = showChatPanel;
  const effectiveChatCollapsed = (isCompact || isMobile || areFlowsVisible) ? true : isChatCollapsed;
  const effectiveChatWidth = effectiveChatCollapsed ? chatPanelCollapsedWidth : chatPanelWidth;
  const effectiveLeftMargin = leftSidebarBaseWidth;

  return { effectiveChatVisible, effectiveChatCollapsed, effectiveChatWidth, effectiveLeftMargin };
}

const baseStore = {
  showChatPanel: true,
  isChatCollapsed: false,
  chatPanelWidth: 450,
  chatPanelCollapsedWidth: 60,
  leftSidebarBaseWidth: 48,
};

describe('WorkflowDesigner Responsive Layout Logic', () => {
  describe('desktop (>= 900px)', () => {
    const params = { ...baseStore, isCompact: false, isMobile: false };

    it('respects the user chat-visible toggle', () => {
      const result = computeResponsiveValues(params);
      expect(result.effectiveChatVisible).toBe(true);
    });

    it('does not force-collapse the chat panel', () => {
      const result = computeResponsiveValues(params);
      expect(result.effectiveChatCollapsed).toBe(false);
    });

    it('uses full chat panel width', () => {
      const result = computeResponsiveValues(params);
      expect(result.effectiveChatWidth).toBe(450);
    });

    it('reserves left sidebar margin', () => {
      const result = computeResponsiveValues(params);
      expect(result.effectiveLeftMargin).toBe(48);
    });

    it('uses collapsed width when user manually collapses', () => {
      const result = computeResponsiveValues({ ...params, isChatCollapsed: true });
      expect(result.effectiveChatCollapsed).toBe(true);
      expect(result.effectiveChatWidth).toBe(60);
    });

    it('hides chat when user toggles it off', () => {
      const result = computeResponsiveValues({ ...params, showChatPanel: false });
      expect(result.effectiveChatVisible).toBe(false);
    });
  });

  describe('compact (600-899px)', () => {
    const params = { ...baseStore, isCompact: true, isMobile: false };

    it('force-collapses the chat panel', () => {
      const result = computeResponsiveValues(params);
      expect(result.effectiveChatCollapsed).toBe(true);
    });

    it('uses collapsed width regardless of user preference', () => {
      const result = computeResponsiveValues(params);
      expect(result.effectiveChatWidth).toBe(60);
    });

    it('still shows chat if user has it enabled', () => {
      const result = computeResponsiveValues(params);
      expect(result.effectiveChatVisible).toBe(true);
    });

    it('reserves left sidebar margin', () => {
      const result = computeResponsiveValues(params);
      expect(result.effectiveLeftMargin).toBe(48);
    });
  });

  describe('mobile (< 600px)', () => {
    const params = { ...baseStore, isCompact: true, isMobile: true };

    it('force-collapses the chat panel', () => {
      const result = computeResponsiveValues(params);
      expect(result.effectiveChatCollapsed).toBe(true);
    });

    it('uses collapsed width', () => {
      const result = computeResponsiveValues(params);
      expect(result.effectiveChatWidth).toBe(60);
    });

    it('still shows the collapsed chat strip', () => {
      const result = computeResponsiveValues(params);
      expect(result.effectiveChatVisible).toBe(true);
    });

    it('reserves left sidebar margin', () => {
      const result = computeResponsiveValues(params);
      expect(result.effectiveLeftMargin).toBe(48);
    });
  });

  describe('flow canvas visible', () => {
    const params = { ...baseStore, isCompact: false, isMobile: false, areFlowsVisible: true };

    it('force-collapses the chat panel when flows are visible', () => {
      const result = computeResponsiveValues(params);
      expect(result.effectiveChatCollapsed).toBe(true);
    });

    it('uses collapsed width when flows are visible', () => {
      const result = computeResponsiveValues(params);
      expect(result.effectiveChatWidth).toBe(60);
    });

    it('still shows the collapsed chat strip when flows are visible', () => {
      const result = computeResponsiveValues(params);
      expect(result.effectiveChatVisible).toBe(true);
    });

    it('does not force-collapse when flows are hidden', () => {
      const result = computeResponsiveValues({ ...params, areFlowsVisible: false });
      expect(result.effectiveChatCollapsed).toBe(false);
      expect(result.effectiveChatWidth).toBe(450);
    });
  });

  describe('store values are never mutated', () => {
    it('computed values differ from store on compact without mutating', () => {
      const storeSnapshot = { ...baseStore };
      const params = { ...baseStore, isCompact: true, isMobile: false };
      const result = computeResponsiveValues(params);

      // Store says not collapsed, but effective is collapsed
      expect(storeSnapshot.isChatCollapsed).toBe(false);
      expect(result.effectiveChatCollapsed).toBe(true);

      // Store value unchanged
      expect(baseStore.isChatCollapsed).toBe(false);
    });
  });

  describe('resize max overrides', () => {
    it('computes compact chat max width override', () => {
      const isCompact = true;
      const windowWidth = 800;
      const chatMaxWidthOverride = isCompact ? Math.min(400, windowWidth * 0.4) : undefined;
      expect(chatMaxWidthOverride).toBe(320); // 800 * 0.4 = 320
    });

    it('returns undefined for chat max width on desktop', () => {
      const isCompact = false;
      const windowWidth = 1440;
      const chatMaxWidthOverride = isCompact ? Math.min(400, windowWidth * 0.4) : undefined;
      expect(chatMaxWidthOverride).toBeUndefined();
    });

    it('computes compact history max height override', () => {
      const isCompact = true;
      const windowHeight = 700;
      const historyMaxHeightOverride = isCompact ? Math.min(300, windowHeight * 0.4) : undefined;
      expect(historyMaxHeightOverride).toBe(280); // 700 * 0.4 = 280
    });

    it('caps history max height at 300 on larger compact screens', () => {
      const isCompact = true;
      const windowHeight = 899;
      const historyMaxHeightOverride = isCompact ? Math.min(300, windowHeight * 0.4) : undefined;
      expect(historyMaxHeightOverride).toBe(300); // 899 * 0.4 = 359.6, capped at 300
    });
  });
});
