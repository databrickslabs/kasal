/**
 * Tests for the inline input-variables prompt (genie-style chat card that
 * replaced the modal InputVariablesDialog).
 */
import React from 'react';
import { describe, it, expect, vi } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/react';

import InputVariablesPrompt, { isSensitive } from './InputVariablesPrompt';

const VARS = [
  { name: 'topic', required: true },
  { name: 'api_key', required: false },
];

describe('InputVariablesPrompt', () => {
  it('renders a field per variable inline (no modal/backdrop)', () => {
    const { container } = render(
      <InputVariablesPrompt variables={VARS} messageId="m1" onSubmit={vi.fn()} />,
    );
    expect(screen.getByPlaceholderText('Enter topic')).toBeInTheDocument();
    expect(screen.getByPlaceholderText('Enter api key')).toBeInTheDocument();
    // Inline card: nothing fixed/overlaying
    expect(container.querySelector('.fixed')).toBeNull();
  });

  it('disables the run button until required fields are filled', () => {
    render(<InputVariablesPrompt variables={VARS} messageId="m2" onSubmit={vi.fn()} />);
    const btn = screen.getByRole('button', { name: /Fill in the variables to run/i });
    expect(btn).toBeDisabled();

    fireEvent.change(screen.getByPlaceholderText('Enter topic'), { target: { value: 'AI' } });
    expect(screen.getByRole('button', { name: /Run crew/i })).toBeEnabled();
  });

  it('submits trimmed non-empty values and locks itself afterwards', () => {
    const onSubmit = vi.fn();
    render(<InputVariablesPrompt variables={VARS} messageId="m3" onSubmit={onSubmit} />);
    fireEvent.change(screen.getByPlaceholderText('Enter topic'), { target: { value: '  AI  ' } });
    fireEvent.click(screen.getByRole('button', { name: /Run crew/i }));

    expect(onSubmit).toHaveBeenCalledWith({ topic: 'AI' });
    // One-shot: button flips to Running and disables, fields lock
    expect(screen.getByRole('button', { name: /Running/i })).toBeDisabled();
    expect(screen.getByPlaceholderText('Enter topic')).toBeDisabled();
  });

  it('remembers submission across remounts (same message id)', () => {
    const { unmount } = render(
      <InputVariablesPrompt variables={VARS} messageId="m4" onSubmit={vi.fn()} />,
    );
    fireEvent.change(screen.getByPlaceholderText('Enter topic'), { target: { value: 'X' } });
    fireEvent.click(screen.getByRole('button', { name: /Run crew/i }));
    unmount();

    render(<InputVariablesPrompt variables={VARS} messageId="m4" onSubmit={vi.fn()} />);
    expect(screen.getByRole('button', { name: /Running/i })).toBeDisabled();
  });

  it('masks sensitive fields with a visibility toggle', () => {
    render(<InputVariablesPrompt variables={VARS} messageId="m5" onSubmit={vi.fn()} />);
    const field = screen.getByPlaceholderText('Enter api key');
    expect(field).toHaveAttribute('type', 'password');
    fireEvent.click(screen.getByTitle('Show'));
    expect(field).toHaveAttribute('type', 'text');
  });

  it('drops whitespace-only optional values from the submitted inputs', () => {
    const onSubmit = vi.fn();
    render(<InputVariablesPrompt variables={VARS} messageId="m6" onSubmit={onSubmit} />);
    fireEvent.change(screen.getByPlaceholderText('Enter topic'), { target: { value: 'AI' } });
    fireEvent.change(screen.getByPlaceholderText('Enter api key'), { target: { value: '   ' } });
    fireEvent.click(screen.getByRole('button', { name: /Run crew/i }));

    expect(onSubmit).toHaveBeenCalledWith({ topic: 'AI' });
  });

  it('runs safely without an onSubmit handler', () => {
    render(<InputVariablesPrompt variables={VARS} messageId="m7" />);
    fireEvent.change(screen.getByPlaceholderText('Enter topic'), { target: { value: 'AI' } });
    fireEvent.click(screen.getByRole('button', { name: /Run crew/i }));
    expect(screen.getByRole('button', { name: /Running/i })).toBeDisabled();
  });

  it('isSensitive flags secret-like names only', () => {
    expect(isSensitive('api_key')).toBe(true);
    expect(isSensitive('PASSWORD')).toBe(true);
    expect(isSensitive('access-key')).toBe(true);
    expect(isSensitive('topic')).toBe(false);
  });
});
