import React from 'react';
import { fireEvent, render, screen } from '@testing-library/react';
import { describe, expect, it, vi } from 'vitest';
import AddPersonDialog from './AddPersonDialog';

const searchDirectory = vi.fn();
vi.mock('../../../api/groups/UserService', () => ({
  UserService: { getInstance: () => ({ searchDirectory }) },
}));

describe('directory search diagnostics', () => {
  it.each([
    "Databricks denied directory access (HTTP 403). Check the app's service principal. Diagnostic: DIR-v3/123456abcdef; stage=directory_request; HTTP=403; auth=app.",
    "Databricks returned 1 matching directory records, but none provided an active, usable sign-in email. Diagnostic: DIR-v3/123456abcdef; stage=directory_response; HTTP=200; auth=app.",
  ])('shows the backend diagnostic: %s', async (detail) => {
    searchDirectory.mockRejectedValueOnce({ response: { data: { detail } } });
    render(<AddPersonDialog onClose={vi.fn()} onAdded={vi.fn()} />);
    fireEvent.change(screen.getByLabelText('Search Databricks directory'), { target: { value: 'alice' } });
    fireEvent.click(screen.getByRole('button', { name: 'Search' }));
    expect(await screen.findByText(detail)).toBeInTheDocument();
    expect(screen.getByLabelText(/Sign-in email/)).toBeEnabled();
  });

  it('falls back when an error has no readable diagnostic', async () => {
    searchDirectory.mockRejectedValueOnce({ response: { data: { detail: [{ message: 'invalid' }] } } });
    render(<AddPersonDialog onClose={vi.fn()} onAdded={vi.fn()} />);
    fireEvent.change(screen.getByLabelText('Search Databricks directory'), { target: { value: 'alice' } });
    fireEvent.click(screen.getByRole('button', { name: 'Search' }));
    expect(await screen.findByText(/Could not contact the directory search service/)).toBeInTheDocument();
  });
});
