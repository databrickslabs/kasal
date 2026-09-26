import React from 'react';
import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen, fireEvent, waitFor } from '@testing-library/react';
import '@testing-library/jest-dom';
import LegacyMcpUrlNotice from './LegacyMcpUrlNotice';

vi.mock('react-i18next', () => ({
  useTranslation: () => ({
    t: (key: string, options?: { defaultValue?: string; count?: number }) =>
      (options?.defaultValue || key).replace('{{count}}', String(options?.count ?? '')),
  }),
}));

const mockService = { migrateLegacyExternalUrls: vi.fn() };
vi.mock('../../../../api/tools/MCPService', () => ({
  MCPService: { getInstance: () => mockService },
}));

describe('LegacyMcpUrlNotice', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it('renders nothing, and never migrates, when nothing is pending', () => {
    const { container } = render(<LegacyMcpUrlNotice count={0} />);
    expect(container).toBeEmptyDOMElement();
    expect(mockService.migrateLegacyExternalUrls).not.toHaveBeenCalled();
  });

  it('does not migrate on render; only the admin click migrates', async () => {
    const onMigrated = vi.fn();
    mockService.migrateLegacyExternalUrls.mockResolvedValue(3);
    render(<LegacyMcpUrlNotice count={3} onMigrated={onMigrated} />);

    expect(screen.getByText(/3 MCP registration\(s\) still use the legacy/)).toBeInTheDocument();
    expect(mockService.migrateLegacyExternalUrls).not.toHaveBeenCalled();

    fireEvent.click(screen.getByRole('button', { name: 'Migrate' }));
    await waitFor(() => expect(screen.getByText(/Migrated 3 MCP registration/)).toBeInTheDocument());
    expect(mockService.migrateLegacyExternalUrls).toHaveBeenCalledTimes(1);
    expect(onMigrated).toHaveBeenCalledWith(3);

    // Closing the success message does not bring back the stale prompt.
    fireEvent.click(screen.getByRole('button', { name: /close/i }));
    expect(screen.queryByText(/still use the legacy/)).not.toBeInTheDocument();
  });

  it('shows a failure and lets the admin retry', async () => {
    mockService.migrateLegacyExternalUrls
      .mockRejectedValueOnce(new Error('Only admins can browse Databricks MCP servers'))
      .mockResolvedValueOnce(1);
    render(<LegacyMcpUrlNotice count={1} />);

    fireEvent.click(screen.getByRole('button', { name: 'Migrate' }));
    await waitFor(() => expect(screen.getByText('Only admins can browse Databricks MCP servers')).toBeInTheDocument());

    fireEvent.click(screen.getByRole('button', { name: 'Migrate' }));
    await waitFor(() => expect(screen.getByText(/Migrated 1 MCP registration/)).toBeInTheDocument());
  });

  it('keeps the success message when the parent reload fails', async () => {
    mockService.migrateLegacyExternalUrls.mockResolvedValue(2);
    const onMigrated = vi.fn().mockRejectedValue(new Error('reload failed'));
    render(<LegacyMcpUrlNotice count={2} onMigrated={onMigrated} />);

    fireEvent.click(screen.getByRole('button', { name: 'Migrate' }));
    await waitFor(() => expect(screen.getByText(/Migrated 2 MCP registration/)).toBeInTheDocument());
  });

  it('falls back to a generic message for a non-Error rejection', async () => {
    mockService.migrateLegacyExternalUrls.mockRejectedValue('nope');
    render(<LegacyMcpUrlNotice count={1} />);
    fireEvent.click(screen.getByRole('button', { name: 'Migrate' }));
    await waitFor(() => expect(screen.getByText('Could not migrate the registrations')).toBeInTheDocument());
  });
});
