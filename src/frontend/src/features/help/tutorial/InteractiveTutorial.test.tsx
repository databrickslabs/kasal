import React from 'react';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { fireEvent, render, screen } from '@testing-library/react';
import type { Step } from 'react-joyride';
import InteractiveTutorial from './InteractiveTutorial';
import { useAppStore } from '../../chat/store/appStore';
import { usePermissionStore } from '../../../store/permissions';
import { useFlowConfigStore } from '../../../store/flowConfig';
import { useBuilderCanvasStore } from '../../../app/sessions/builderCanvasStore';
import { useUILayoutStore } from '../../../store/uiLayout';

vi.mock('react-joyride', async importOriginal => ({
  ...await importOriginal<typeof import('react-joyride')>(),
  default: ({ steps, callback }: { steps: Step[]; callback: (event: { status: string }) => void }) => <div>
    {steps.map((step, index) => <div key={index}>{step.title}{step.content}</div>)}
    <button onClick={() => callback({ status: 'finished' })}>Finish test tour</button>
  </div>,
}));

beforeEach(() => {
  localStorage.setItem('selectedGroupId', 'tour-group');
  usePermissionStore.setState({ allowAgentBuilder: true, allowFlowBuilder: true });
  useFlowConfigStore.setState({ kasalFlowEnabled: true });
  useAppStore.setState({ sidebarOpen: false });
  useUILayoutStore.setState({ appMode: 'crew' });
  useBuilderCanvasStore.setState({ canvases: [], activeCanvasId: null });
  useBuilderCanvasStore.getState().createCanvas('Original crew', 'crew');
  vi.spyOn(HTMLElement.prototype, 'getClientRects').mockReturnValue([{ width: 100, height: 40 }] as unknown as DOMRectList);
});
afterEach(() => vi.restoreAllMocks());

function mount(onClose = vi.fn()) {
  return render(<>
    {['new-session', 'session-list', 'session-archive', 'canvas-tools', 'builder-catalog', 'chat-catalog',
      'workspace-panel-tabs', 'workspace-account-actions', 'configuration-button', 'workspace-activity']
      .map(target => <div key={target} data-tour={target} />)}
    <InteractiveTutorial isOpen onClose={onClose} />
  </>);
}

describe('current UI tutorial', () => {
  it('reveals sidebar targets, uses the existing builder session, and restores the sidebar on completion', async () => {
    const original = useBuilderCanvasStore.getState().activeCanvasId;
    const onClose = vi.fn();
    mount(onClose);
    fireEvent.click(screen.getByRole('button', { name: 'Tour Agent Builder' }));
    expect(useAppStore.getState().sidebarOpen).toBe(true);
    expect(await screen.findByText(/Control the canvas here/)).toBeInTheDocument();
    expect(screen.getByText(/without confirmation/)).toBeInTheDocument();
    expect(screen.getByText(/Crew catalog now lives in the left sidebar/)).toBeInTheDocument();
    expect(useBuilderCanvasStore.getState().activeCanvasId).toBe(original);
    fireEvent.click(screen.getByRole('button', { name: 'Finish test tour' }));
    expect(onClose).toHaveBeenCalledOnce();
    expect(useAppStore.getState().sidebarOpen).toBe(false);
  });

  it('does not advertise restricted builders or their Activity sections to a Chat-only user', async () => {
    usePermissionStore.setState({ allowAgentBuilder: false, allowFlowBuilder: false });
    useUILayoutStore.setState({ appMode: 'chat' });
    mount();
    expect(screen.queryByRole('button', { name: /Tour .*Builder/ })).toBeNull();
    fireEvent.click(screen.getByRole('button', { name: 'Tour Chat' }));
    expect(await screen.findByText(/New session starts a fresh Chat/)).toBeInTheDocument();
    expect(screen.queryByText(/Agent Builder|Flow Builder|Assistant logs/)).toBeNull();
    expect(screen.getByText(/only crews and flows published to Chat/)).toBeInTheDocument();
  });

  it('omits Flow Builder when flows are disabled and restores the sidebar on external close', async () => {
    useFlowConfigStore.setState({ kasalFlowEnabled: false });
    const view = mount();
    expect(screen.queryByRole('button', { name: 'Tour Flow Builder' })).toBeNull();
    fireEvent.click(screen.getByRole('button', { name: 'Tour Chat' }));
    expect(await screen.findByText(/Use New session to choose Chat, Agent Builder/)).toBeInTheDocument();
    expect(screen.queryByText(/Flow Builder/)).toBeNull();
    view.rerender(<InteractiveTutorial isOpen={false} onClose={vi.fn()} />);
    expect(useAppStore.getState().sidebarOpen).toBe(false);
  });
});
