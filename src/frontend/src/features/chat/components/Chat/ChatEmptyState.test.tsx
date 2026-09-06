import React from 'react';
import { describe, it, expect, vi } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/react';
import ChatEmptyState from './ChatEmptyState';

describe('Chat empty state', () => {
  it('keeps editable starters without the retired builder and docs footer', () => {
    const prefill = vi.fn();
    render(<ChatEmptyState onPrefill={prefill} />);
    fireEvent.click(screen.getByRole('button', { name: 'Create a diagram' }));
    expect(prefill).toHaveBeenCalledWith('Create a diagram of ');
    expect(screen.queryByText(/Want to design it yourself/)).not.toBeInTheDocument();
    expect(screen.queryByText(/New to Kasal/)).not.toBeInTheDocument();
  });
});
