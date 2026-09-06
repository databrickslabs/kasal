import React from 'react';
import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen, fireEvent, waitFor } from '@testing-library/react';
import SkillCard from './SkillCard';

const validate = vi.fn();
const list = vi.fn();
const create = vi.fn();
const update = vi.fn();
vi.mock('../../../../api/tools/SkillService', () => ({
  SkillService: {
    validate: (...a: unknown[]) => validate(...a),
    list: (...a: unknown[]) => list(...a),
    create: (...a: unknown[]) => create(...a),
    update: (...a: unknown[]) => update(...a),
  },
}));

const MD =
  '---\nname: writing-release-notes\ndescription: Use when drafting release notes.\n---\n\n# Writing release notes\n\nBody here.\n';

beforeEach(() => {
  vi.clearAllMocks();
  validate.mockResolvedValue({ valid: true, errors: [], warnings: [] });
  list.mockResolvedValue([]);
  create.mockImplementation(async (input: { name: string }) => ({ id: 9, name: input.name }));
  update.mockImplementation(async (_id: number, input: { name: string }) => ({ id: 3, name: input.name }));
});

describe('SkillCard', () => {
  it('shows the parsed name and description, body collapsed', () => {
    render(<SkillCard code={MD} />);
    expect(screen.getByText('writing-release-notes')).toBeInTheDocument();
    expect(screen.getByText('Use when drafting release notes.')).toBeInTheDocument();
    expect(screen.queryByText(/Body here/)).toBeNull();
    fireEvent.click(screen.getByText('Show instructions'));
    expect(screen.getByText(/Body here/)).toBeInTheDocument();
  });

  it('cannot save while the draft is still streaming', () => {
    render(<SkillCard code={MD} streaming />);
    expect(screen.getByText('Drafting…')).toBeInTheDocument();
    expect(screen.getByRole('button', { name: /Save to teamspace/ })).toBeDisabled();
  });

  it('validates, then creates, then reports the save', async () => {
    render(<SkillCard code={MD} />);
    fireEvent.click(screen.getByRole('button', { name: /Save to teamspace/ }));
    await waitFor(() => expect(screen.getByText(/Saved "writing-release-notes"/)).toBeInTheDocument());
    expect(validate).toHaveBeenCalledTimes(1);
    expect(create).toHaveBeenCalledWith(
      expect.objectContaining({
        name: 'writing-release-notes',
        description: 'Use when drafting release notes.',
        body: expect.stringContaining('# Writing release notes'),
      }),
    );
    expect(validate.mock.invocationCallOrder[0]).toBeLessThan(create.mock.invocationCallOrder[0]);
  });

  it("surfaces the validator's own errors and does not create", async () => {
    validate.mockResolvedValue({ valid: false, errors: ['name must be kebab-case'] });
    render(<SkillCard code={MD} />);
    fireEvent.click(screen.getByRole('button', { name: /Save to teamspace/ }));
    await waitFor(() => expect(screen.getByText('name must be kebab-case')).toBeInTheDocument());
    expect(create).not.toHaveBeenCalled();
  });

  it('updates in place when this teamspace already owns a skill of that name', async () => {
    list.mockResolvedValue([{ id: 3, name: 'writing-release-notes', group_id: 'g1' }]);
    render(<SkillCard code={MD} />);
    fireEvent.click(screen.getByRole('button', { name: /Save to teamspace/ }));
    await waitFor(() => expect(screen.getByText(/Updated "writing-release-notes"/)).toBeInTheDocument());
    expect(update).toHaveBeenCalledWith(3, expect.objectContaining({ name: 'writing-release-notes' }));
    expect(create).not.toHaveBeenCalled();
  });

  it('says so when the name overrides one of the builtins', async () => {
    list.mockResolvedValue([{ id: 1, name: 'writing-release-notes', group_id: null }]);
    render(<SkillCard code={MD} />);
    fireEvent.click(screen.getByRole('button', { name: /Save to teamspace/ }));
    await waitFor(() => expect(screen.getByText(/overrides Kasal's builtin/)).toBeInTheDocument());
    expect(create).toHaveBeenCalled();
  });

  it('shows the backend detail verbatim when saving is refused', async () => {
    validate.mockRejectedValue({ response: { data: { detail: 'Only editors and admins can author skills' } } });
    render(<SkillCard code={MD} />);
    fireEvent.click(screen.getByRole('button', { name: /Save to teamspace/ }));
    await waitFor(() =>
      expect(screen.getByText('Only editors and admins can author skills')).toBeInTheDocument(),
    );
  });
});
