import { act, fireEvent, render, screen, waitFor } from '@testing-library/react';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import Configuration from './Configuration';
import { useEventTriggersStore } from '../../../store/eventTriggers';
import { usePermissionStore } from '../../../store/permissions';
import { useGroupStore } from '../../../store/groups';
import { switchSettingsTeamspace } from '../lib/settingsNavigation';
vi.mock('../lib/settingsNavigation', () => ({ readSettingsNavigation: () => null, clearSettingsNavigation: vi.fn(), switchSettingsTeamspace: vi.fn() }));

vi.mock('react-i18next', () => ({ useTranslation: () => ({ t: (_key: string, options: { defaultValue?: string }) => options?.defaultValue || _key }) }));
vi.mock('./GeneralSettings', () => ({ default: () => <div>Personal preferences content</div> }));
vi.mock('./WorkspaceOverview', () => ({ default: () => <div>Teamspace overview content</div> }));
vi.mock('./Models', () => ({ default: ({ mode }: { mode: string }) => <div>Models scope: {mode}</div> }));
vi.mock('./APIKeys/APIKeys', () => ({ default: () => <div>Credential settings content</div> }));
vi.mock('../../triggers/components/TriggersPanel', () => ({ default: ({ embedded }: { embedded?: boolean }) => <div>Event trigger controls {embedded ? 'embedded' : 'standalone'}</div> }));
vi.mock('./Prompts', () => ({ default: () => <div>Prompt settings content</div> }));

beforeEach(() => {
  useEventTriggersStore.setState({ enabled: false, load: vi.fn() });
  usePermissionStore.setState({ userRole: 'admin', isLoading: false, isSystemAdmin: true, isPersonalWorkspaceManager: false, loadPermissions: vi.fn() });
  useGroupStore.setState({ fetchMyGroups: vi.fn(), currentGroupId: 'team-design', groups: [{ id: 'team-design', name: 'Design team', status: 'active', created_at: '', updated_at: '' }] });
});
describe('Settings workspace navigation', () => {
  it('shows the active teamspace and renders models in the selected scope', async () => {
    render(<Configuration />);
    expect(screen.getByRole('combobox', { name: 'Settings for' })).toHaveTextContent('Design team');
    fireEvent.click(screen.getByRole('button', { name: 'Models', exact: true }));
    expect(await screen.findByText('Models scope: workspace')).toBeVisible();
    fireEvent.mouseDown(screen.getByRole('combobox', { name: 'Settings for' }));
    fireEvent.click(screen.getByRole('option', { name: 'System administration' }));
    expect(await screen.findByText('Models scope: system')).toBeVisible();
  });
  it('filters navigation without unmounting the active settings form', async () => {
    render(<Configuration />);
    await screen.findByText('Teamspace overview content');
    fireEvent.change(screen.getByRole('textbox', { name: 'Search settings' }), { target: { value: 'credentials' } });
    expect(screen.getByRole('button', { name: 'API Keys', exact: true })).toBeVisible();
    expect(screen.queryByRole('button', { name: 'Models', exact: true })).not.toBeInTheDocument();
    expect(screen.getByText('Teamspace overview content')).toBeVisible();
  });
  it('keeps the API Keys shortcut working and rejects inaccessible targets after a role change', async () => {
    render(<Configuration />);
    act(() => window.dispatchEvent(new CustomEvent('kasal:navigate-config', { detail: { section: 'API Keys' } })));
    expect(await screen.findByText('Credential settings content')).toBeVisible();
    act(() => usePermissionStore.setState({ userRole: 'operator', isSystemAdmin: false }));
    expect(await screen.findByText('Personal preferences content')).toBeVisible();
    act(() => window.dispatchEvent(new CustomEvent('kasal:navigate-config', { detail: { section: 'api-keys' } })));
    expect(screen.queryByText('Credential settings content')).not.toBeInTheDocument();
  });
  it('closes through the settings header', () => {
    const onClose = vi.fn();
    render(<Configuration onClose={onClose} />);
    fireEvent.click(screen.getByRole('button', { name: 'Close settings' }));
    expect(onClose).toHaveBeenCalledOnce();
  });
});

it('lists other administered teamspaces and switches from Settings for', () => {
  usePermissionStore.setState({ isSystemAdmin: false });
  const makeGroup = (id: string, user_role: 'ADMIN' | 'EDITOR', status: 'active' | 'suspended' = 'active') => ({ id, name: id, user_role, status, auto_created: false, user_count: 1, created_at: '', updated_at: '' });
  useGroupStore.setState({ groups: [makeGroup('team-design', 'ADMIN'), makeGroup('Research team', 'ADMIN'), makeGroup('Editor team', 'EDITOR'), makeGroup('Suspended team', 'ADMIN', 'suspended')] });
  render(<Configuration />);
  fireEvent.mouseDown(screen.getByRole('combobox', { name: 'Settings for' }));
  expect(screen.queryByRole('option', { name: 'Editor team' })).not.toBeInTheDocument();
  expect(screen.queryByRole('option', { name: 'Suspended team' })).not.toBeInTheDocument();
  fireEvent.click(screen.getByRole('option', { name: 'Research team' }));
  expect(switchSettingsTeamspace).toHaveBeenCalledWith('Research team', 'overview');
  expect(screen.getByText('Switching teamspace…')).toBeVisible();
});

it('shows enabled event triggers as an embedded teamspace settings section', async () => {
  render(<Configuration />);
  expect(screen.queryByRole('button', { name: 'Event triggers', exact: true })).not.toBeInTheDocument();
  act(() => useEventTriggersStore.setState({ enabled: true }));
  fireEvent.click(screen.getByRole('button', { name: 'Event triggers', exact: true }));
  expect(await screen.findByText('Event trigger controls embedded')).toBeVisible();
  act(() => useEventTriggersStore.setState({ enabled: false }));
  await waitFor(() => expect(screen.queryByText('Event trigger controls embedded')).not.toBeInTheDocument());
});
