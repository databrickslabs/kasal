import { fireEvent, render, screen, within } from '@testing-library/react';
import { describe, expect, it, vi } from 'vitest';
import MemoryHygieneFields from './MemoryHygieneFields';

describe('MemoryHygieneFields', () => {
  it('shows the effective defaults and keeps retention disabled until forgetting is on', () => {
    render(<MemoryHygieneFields tuning={{}} update={vi.fn()} />);
    expect(screen.getByRole('checkbox', { name: 'Forget expired memories' })).not.toBeChecked();
    expect(screen.getByLabelText('Keep run and chat memories (days)')).toBeDisabled();
    expect(screen.getByRole('checkbox', { name: 'Retire facts a newer one contradicts' })).toBeChecked();
  });

  it('writes each knob into the tuning config', () => {
    const update = vi.fn();
    render(<MemoryHygieneFields tuning={{ forgetting_enabled: true }} update={update} />);

    fireEvent.change(screen.getByLabelText('Keep run and chat memories (days)'), {
      target: { value: '30' },
    });
    expect(update).toHaveBeenLastCalledWith({ episodic_ttl_days: 30 });

    fireEvent.click(screen.getByRole('checkbox', { name: 'Merge near-duplicate memories between runs' }));
    expect(update).toHaveBeenLastCalledWith({ llm_consolidation_enabled: false });

    fireEvent.mouseDown(screen.getByLabelText('Write screening'));
    fireEvent.click(within(screen.getByRole('listbox')).getByText(/Annotate/));
    expect(update).toHaveBeenLastCalledWith({ write_screening: 'annotate' });
  });
});
