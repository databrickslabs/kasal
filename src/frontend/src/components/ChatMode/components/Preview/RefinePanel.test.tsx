import { describe, it, expect, vi, afterEach } from 'vitest';
import { render, screen, fireEvent, act } from '@testing-library/react';
import RefinePanel from './RefinePanel';

const baseProps = {
  deliverable: 'presentation',
  deliverableLabel: 'Presentation',
  onApplyStyle: vi.fn(),
  onRefine: vi.fn(),
  onClose: vi.fn(),
};

function renderPanel(props: Partial<typeof baseProps> = {}) {
  const merged = { ...baseProps, onApplyStyle: vi.fn(), onRefine: vi.fn(), onClose: vi.fn(), ...props };
  render(<RefinePanel {...merged} />);
  return merged;
}

afterEach(() => {
  vi.useRealTimers();
});

describe('RefinePanel', () => {
  it('titles the panel with the friendly deliverable label', () => {
    renderPanel({ deliverableLabel: 'Photo album', deliverable: 'album' });
    expect(screen.getByText('Photo album')).toBeInTheDocument();
  });

  it('applies a one-click style preset immediately (deterministic, no AI)', () => {
    const p = renderPanel();
    fireEvent.click(screen.getByTitle('Apply the Dark style'));
    expect(p.onApplyStyle).toHaveBeenCalledTimes(1);
    expect(p.onApplyStyle.mock.calls[0][0]).toMatchObject({ accent: '#38BDF8', background: '#0F172A' });
    expect(p.onRefine).not.toHaveBeenCalled();
  });

  it('debounces a fine-tune color edit before applying', () => {
    vi.useFakeTimers();
    const p = renderPanel();
    fireEvent.click(screen.getByText(/fine-tune/i));
    fireEvent.change(screen.getByLabelText('Accent'), { target: { value: '#abcdef' } });
    // not applied yet — debounced
    expect(p.onApplyStyle).not.toHaveBeenCalled();
    act(() => { vi.advanceTimersByTime(200); });
    expect(p.onApplyStyle).toHaveBeenCalledTimes(1);
    expect(p.onApplyStyle.mock.calls[0][0]).toMatchObject({ accent: '#abcdef' });
  });

  it('applies fine-tune font and spacing changes (debounced)', () => {
    vi.useFakeTimers();
    const p = renderPanel();
    fireEvent.click(screen.getByText(/fine-tune/i));
    fireEvent.change(screen.getByLabelText('Font'), { target: { value: 'serif' } });
    fireEvent.change(screen.getByLabelText('Spacing'), { target: { value: 'compact' } });
    act(() => { vi.advanceTimersByTime(200); });
    const last = p.onApplyStyle.mock.calls.at(-1)?.[0];
    expect(last).toMatchObject({ font: 'serif', density: 'compact' });
  });

  it('compiles ONLY changed content settings into the AI directive', () => {
    const p = renderPanel();
    // disabled until something changes
    const updateBtn = screen.getByText('Update with AI');
    expect(updateBtn).toBeDisabled();
    fireEvent.change(screen.getByLabelText('Target slide count'), { target: { value: '12' } });
    expect(updateBtn).not.toBeDisabled();
    fireEvent.click(updateBtn);
    expect(p.onRefine).toHaveBeenCalledWith('Aim for about 12 slides.');
    expect(p.onClose).toHaveBeenCalled();
  });

  it('compiles a toggled switch setting into the directive', () => {
    const p = renderPanel();
    // Turn OFF the "open with a title slide" switch.
    fireEvent.click(screen.getByLabelText('Open with a title slide'));
    fireEvent.click(screen.getByText('Update with AI'));
    expect(p.onRefine).toHaveBeenCalledWith('Skip the title slide.');
  });

  it('compiles a select setting into the directive (album layout)', () => {
    const p = renderPanel({ deliverable: 'album', deliverableLabel: 'Photo album' });
    fireEvent.change(screen.getByLabelText('Layout'), { target: { value: 'carousel' } });
    fireEvent.click(screen.getByText('Update with AI'));
    expect(p.onRefine).toHaveBeenCalledTimes(1);
    expect(p.onRefine.mock.calls[0][0]).toMatch(/carousel/i);
  });

  it('hides the Content section for a deliverable with no type settings', () => {
    renderPanel({ deliverable: 'default', deliverableLabel: 'Document' });
    expect(screen.queryByText('Update with AI')).not.toBeInTheDocument();
    // free-text refine is always available
    expect(screen.getByPlaceholderText(/add a chart/i)).toBeInTheDocument();
  });

  it('submits a free-text instruction and closes', () => {
    const p = renderPanel();
    const input = screen.getByPlaceholderText(/add a chart/i);
    fireEvent.change(input, { target: { value: 'add a closing thank-you slide' } });
    fireEvent.click(screen.getByText('Send'));
    expect(p.onRefine).toHaveBeenCalledWith('add a closing thank-you slide');
    expect(p.onClose).toHaveBeenCalled();
  });

  it('seeds fine-tune controls from the current theme', () => {
    renderPanel({ currentTheme: { accent: '#abc123' } });
    fireEvent.click(screen.getByText(/fine-tune/i));
    expect((screen.getByLabelText('Accent') as HTMLInputElement).value).toBe('#abc123');
  });
});
