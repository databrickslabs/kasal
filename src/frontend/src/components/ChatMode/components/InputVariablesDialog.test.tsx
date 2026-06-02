import { describe, it, expect, vi } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/react';
import InputVariablesDialog from './InputVariablesDialog';
import type { DetectedVariable } from '../utils/variableDetector';

const vars = (...v: DetectedVariable[]) => v;
const V = (name: string, required = true): DetectedVariable => ({ name, required } as DetectedVariable);

describe('InputVariablesDialog', () => {
  it('renders nothing when open is false', () => {
    const { container } = render(
      <InputVariablesDialog open={false} variables={vars(V('topic'))} onConfirm={vi.fn()} onCancel={vi.fn()} />,
    );
    expect(container.firstChild).toBeNull();
  });

  it('renders the dialog, the count badge and a field per variable when open', () => {
    render(
      <InputVariablesDialog open variables={vars(V('topic'), V('region'))} onConfirm={vi.fn()} onCancel={vi.fn()} />,
    );
    expect(screen.getByText('Input Variables')).toBeInTheDocument();
    expect(screen.getByText('2 required')).toBeInTheDocument();
    expect(screen.getByPlaceholderText('Enter topic')).toBeInTheDocument();
    expect(screen.getByPlaceholderText('Enter region')).toBeInTheDocument();
    // required asterisk
    expect(screen.getAllByText('*').length).toBe(2);
  });

  it('renders an empty variable list (0 required, no fields)', () => {
    render(<InputVariablesDialog open variables={[]} onConfirm={vi.fn()} onCancel={vi.fn()} />);
    expect(screen.getByText('0 required')).toBeInTheDocument();
  });

  it('typing updates the value and clears a pre-existing error', () => {
    const onConfirm = vi.fn();
    render(<InputVariablesDialog open variables={vars(V('topic'))} onConfirm={onConfirm} onCancel={vi.fn()} />);
    // Submit empty -> required error shows
    fireEvent.submit(screen.getByText('Run with variables').closest('form')!);
    expect(screen.getByText('This variable is required')).toBeInTheDocument();
    // Typing clears the error
    fireEvent.change(screen.getByPlaceholderText('Enter topic'), { target: { value: 'AI' } });
    expect(screen.queryByText('This variable is required')).not.toBeInTheDocument();
  });

  it('handleChange exercises both the truthy and falsy value branches', () => {
    render(<InputVariablesDialog open variables={vars(V('topic'))} onConfirm={vi.fn()} onCancel={vi.fn()} />);
    fireEvent.submit(screen.getByText('Run with variables').closest('form')!);
    expect(screen.getByText('This variable is required')).toBeInTheDocument();
    const input = screen.getByPlaceholderText('Enter topic');
    // truthy value -> clears error
    fireEvent.change(input, { target: { value: 'x' } });
    expect(screen.queryByText('This variable is required')).not.toBeInTheDocument();
    // falsy value -> no error-clearing branch (error already gone, stays gone)
    fireEvent.change(input, { target: { value: '' } });
    expect(screen.queryByText('This variable is required')).not.toBeInTheDocument();
  });

  it('confirm collects trimmed non-empty values and omits empty ones', () => {
    const onConfirm = vi.fn();
    render(
      <InputVariablesDialog
        open
        variables={vars(V('topic'), V('optional', false))}
        onConfirm={onConfirm}
        onCancel={vi.fn()}
      />,
    );
    fireEvent.change(screen.getByPlaceholderText('Enter topic'), { target: { value: '  AI news  ' } });
    // leave 'optional' empty
    fireEvent.submit(screen.getByText('Run with variables').closest('form')!);
    expect(onConfirm).toHaveBeenCalledWith({ topic: 'AI news' });
  });

  it('blocks submit and shows required error for a missing required field', () => {
    const onConfirm = vi.fn();
    render(
      <InputVariablesDialog
        open
        variables={vars(V('topic'), V('optional', false))}
        onConfirm={onConfirm}
        onCancel={vi.fn()}
      />,
    );
    fireEvent.submit(screen.getByText('Run with variables').closest('form')!);
    expect(onConfirm).not.toHaveBeenCalled();
    expect(screen.getByText('This variable is required')).toBeInTheDocument();
  });

  it('does not render an asterisk for non-required variables', () => {
    render(<InputVariablesDialog open variables={vars(V('optional', false))} onConfirm={vi.fn()} onCancel={vi.fn()} />);
    expect(screen.queryByText('*')).not.toBeInTheDocument();
  });

  it('treats sensitive variable names as password fields and toggles visibility', () => {
    render(<InputVariablesDialog open variables={vars(V('api_key'))} onConfirm={vi.fn()} onCancel={vi.fn()} />);
    const input = screen.getByPlaceholderText('Enter api key') as HTMLInputElement;
    expect(input.type).toBe('password');
    // toggle to show
    fireEvent.click(screen.getByTitle('Show'));
    expect((screen.getByPlaceholderText('Enter api key') as HTMLInputElement).type).toBe('text');
    // toggle back to hide
    fireEvent.click(screen.getByTitle('Hide'));
    expect((screen.getByPlaceholderText('Enter api key') as HTMLInputElement).type).toBe('password');
  });

  it.each(['secret', 'password', 'passwd', 'token', 'api-key', 'credential', 'private_key', 'access-key'])(
    'detects "%s" as sensitive',
    (name) => {
      render(<InputVariablesDialog open variables={vars(V(name))} onConfirm={vi.fn()} onCancel={vi.fn()} />);
      // sensitive -> a Show toggle button exists
      expect(screen.getByTitle('Show')).toBeInTheDocument();
    },
  );

  it('cancel via the Cancel button, the backdrop, and the header X', () => {
    const onCancel = vi.fn();
    const { container, rerender } = render(
      <InputVariablesDialog open variables={vars(V('topic'))} onConfirm={vi.fn()} onCancel={onCancel} />,
    );
    fireEvent.click(screen.getByText('Cancel'));
    expect(onCancel).toHaveBeenCalledTimes(1);

    // backdrop (the absolute inset-0 black overlay)
    const backdrop = container.querySelector('.bg-black\\/50') as HTMLElement;
    fireEvent.click(backdrop);
    expect(onCancel).toHaveBeenCalledTimes(2);

    // header X button (the first icon button with no text)
    rerender(<InputVariablesDialog open variables={vars(V('topic'))} onConfirm={vi.fn()} onCancel={onCancel} />);
    const headerButtons = container.querySelectorAll('button');
    // The X button sits in the header
    fireEvent.click(headerButtons[0]);
    expect(onCancel).toHaveBeenCalledTimes(3);
  });

  it('resets state when reopened with new variables (effect re-runs)', () => {
    const { rerender } = render(
      <InputVariablesDialog open variables={vars(V('topic'))} onConfirm={vi.fn()} onCancel={vi.fn()} />,
    );
    fireEvent.change(screen.getByPlaceholderText('Enter topic'), { target: { value: 'AI' } });
    // reopen with a different variable set
    rerender(<InputVariablesDialog open variables={vars(V('region'))} onConfirm={vi.fn()} onCancel={vi.fn()} />);
    expect((screen.getByPlaceholderText('Enter region') as HTMLInputElement).value).toBe('');
  });
});
