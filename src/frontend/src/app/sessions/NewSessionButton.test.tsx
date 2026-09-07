import React from 'react';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import { fireEvent, render, screen } from '@testing-library/react';
import NewSessionButton from './NewSessionButton';
import { usePermissionStore } from '../../store/permissions';
import { useFlowConfigStore } from '../../store/flowConfig';
import { newWorkspaceSession } from './sessionNavigation';

vi.mock('./sessionNavigation', () => ({
  newWorkspaceSession: vi.fn(),
  modeLabels: { chat: 'Chat', crew: 'Agent Builder', flow: 'Flow Builder' },
}));

beforeEach(() => {
  vi.clearAllMocks();
  usePermissionStore.setState({ allowAgentBuilder: true, allowFlowBuilder: true });
  useFlowConfigStore.setState({ kasalFlowEnabled: true });
});

describe('New session mode choices', () => {
  it.each(['chat', 'crew', 'flow'] as const)('starts a fresh %s session from the shared button', mode => {
    const onCreated = vi.fn();
    const label = { chat: 'Chat', crew: 'Agent Builder', flow: 'Flow Builder' }[mode];
    render(<NewSessionButton expanded onCreated={onCreated} />);
    fireEvent.click(screen.getByRole('button', { name: 'New session' }));
    expect(screen.getAllByRole('menuitem')).toHaveLength(3);
    fireEvent.click(screen.getByRole('menuitem', { name: new RegExp(`^${label}`) }));
    expect(newWorkspaceSession).toHaveBeenCalledWith(mode);
    expect(onCreated).toHaveBeenCalledTimes(1);
  });

  it('offers the same choices from the collapsed rail', () => {
    render(<NewSessionButton expanded={false} />);
    fireEvent.click(screen.getByRole('button', { name: 'New session' }));
    expect(screen.getAllByRole('menuitem')).toHaveLength(3);
  });

  it('starts Chat directly for operators who cannot use either builder', () => {
    usePermissionStore.setState({ allowAgentBuilder: false, allowFlowBuilder: false });
    render(<NewSessionButton expanded />);
    fireEvent.click(screen.getByRole('button', { name: 'New session' }));
    expect(screen.queryByRole('menu')).toBeNull();
    expect(newWorkspaceSession).toHaveBeenCalledWith('chat');
  });

  it('hides unavailable builder choices', () => {
    usePermissionStore.setState({ allowAgentBuilder: false });
    render(<NewSessionButton expanded />);
    fireEvent.click(screen.getByRole('button', { name: 'New session' }));
    expect(screen.queryByRole('menuitem', { name: /^Agent Builder/ })).toBeNull();
    expect(screen.getAllByRole('menuitem')).toHaveLength(2);
  });

  it('respects the Flow feature setting', () => {
    useFlowConfigStore.setState({ kasalFlowEnabled: false });
    render(<NewSessionButton expanded />);
    fireEvent.click(screen.getByRole('button', { name: 'New session' }));
    expect(screen.queryByRole('menuitem', { name: /^Flow Builder/ })).toBeNull();
  });
});
