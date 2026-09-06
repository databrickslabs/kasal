import React from 'react';
import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen, fireEvent, waitFor } from '@testing-library/react';
import SkillPublishDialog from './SkillPublishDialog';

beforeEach(() => {
  try {
    localStorage.clear();
  } catch {
    /* ignore */
  }
});

const base = {
  open: true,
  title: 'Publish “databricks-sql” to Unity Catalog',
  defaultCatalog: 'kasal',
  defaultSchema: 'default',
  onClose: () => {},
  onConfirm: async () => {},
};

describe('SkillPublishDialog', () => {
  it('prefills the catalog/schema configured in the Databricks section', () => {
    render(<SkillPublishDialog {...base} fqnName="databricks-sql" />);
    expect((screen.getByLabelText('Catalog') as HTMLInputElement).value).toBe('kasal');
    expect((screen.getByLabelText('Schema') as HTMLInputElement).value).toBe('default');
    // With a single-skill name it shows the resolved fully-qualified target.
    expect(screen.getByText(/kasal\.default\.databricks-sql/)).toBeInTheDocument();
  });

  it('runs onConfirm with the shown catalog.schema and closes on success', async () => {
    const onConfirm = vi.fn().mockResolvedValue(undefined);
    const onClose = vi.fn();
    render(
      <SkillPublishDialog
        {...base}
        onConfirm={onConfirm}
        onClose={onClose}
        fqnName="databricks-sql"
      />,
    );
    fireEvent.click(screen.getByRole('button', { name: /publish/i }));
    await waitFor(() => expect(onConfirm).toHaveBeenCalledWith('kasal', 'default'));
    expect(onClose).toHaveBeenCalled();
  });

  it('uses the Pull labels for a pull action', () => {
    render(
      <SkillPublishDialog
        {...base}
        title="Pull skills from Unity Catalog"
        confirmLabel="Pull"
        busyLabel="Pulling…"
      />,
    );
    expect(screen.getByRole('button', { name: 'Pull' })).toBeInTheDocument();
    // No single-skill preview line for a batch action.
    expect(screen.queryByText(/Target:/)).toBeNull();
  });

  it('disables confirm until both catalog and schema are set', () => {
    render(<SkillPublishDialog {...base} defaultSchema="" />);
    expect(screen.getByRole('button', { name: /publish/i })).toBeDisabled();
  });

  it('surfaces the backend error detail verbatim and stays open', async () => {
    const onConfirm = vi
      .fn()
      .mockRejectedValue({ response: { data: { detail: 'PERMISSION_DENIED: no CREATE SKILL' } } });
    const onClose = vi.fn();
    render(<SkillPublishDialog {...base} onConfirm={onConfirm} onClose={onClose} />);
    fireEvent.click(screen.getByRole('button', { name: /publish/i }));
    await waitFor(() =>
      expect(screen.getByText(/PERMISSION_DENIED: no CREATE SKILL/)).toBeInTheDocument(),
    );
    expect(onClose).not.toHaveBeenCalled();
  });
});
