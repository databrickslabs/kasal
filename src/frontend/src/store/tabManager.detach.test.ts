/**
 * Tab/crew detachment (chat generation overwrite bug).
 *
 * When chat generates a NEW crew into a tab that was previously associated
 * with a saved crew, the tab must be detached from that crew — otherwise the
 * next Save silently overwrites the old crew record (new content, old name).
 */
import { describe, it, expect, beforeEach } from 'vitest';
import { useTabManagerStore } from './tabManager';

describe('tabManager - clearTabCrewInfo', () => {
  beforeEach(() => {
    useTabManagerStore.setState({ tabs: [], activeTabId: null });
  });

  it('detaches the tab from its saved crew so the next save creates a new one', () => {
    const store = useTabManagerStore.getState();
    const tabId = store.createTab('Send Email');
    store.updateTabCrewInfo(tabId, 'crew-old-id', 'Send Email');

    let tab = useTabManagerStore.getState().getTab(tabId);
    expect(tab?.savedCrewId).toBe('crew-old-id');
    expect(tab?.savedCrewName).toBe('Send Email');

    useTabManagerStore.getState().clearTabCrewInfo(tabId);

    tab = useTabManagerStore.getState().getTab(tabId);
    expect(tab?.savedCrewId).toBeUndefined();
    expect(tab?.savedCrewName).toBeUndefined();
    expect(tab?.lastSavedAt).toBeUndefined();
    // Tab itself survives — only the crew association is dropped
    expect(tab?.name).toBe('Send Email');
  });
});

describe('WorkflowChat - generation detaches tab from saved crew (wiring)', () => {
  it('calls detachTabFromSavedCrew on both generation paths', async () => {
    const { readFileSync } = await import('fs');
    const { resolve } = await import('path');
    const src = readFileSync(
      resolve(__dirname, '../components/Chat/WorkflowChatRefactored.tsx'),
      'utf-8'
    );
    expect(src).toContain('clearTabCrewInfo(activeTabId)');
    // onPlanReady (streaming) + legacy synchronous fallback
    const calls = src.match(/detachTabFromSavedCrew\(\);/g) || [];
    expect(calls.length).toBe(2);
  });
});
