import React from 'react';
import { describe, expect, it, vi } from 'vitest';
import { fireEvent, render, screen } from '@testing-library/react';
import ChatStarters, { STARTERS } from './ChatStarters';

describe('ChatStarters', () => {
  it('shows nine starters in a rectangular grid', () => {
    render(<ChatStarters onPick={vi.fn()} />);
    const group = screen.getByRole('group', { name: 'Start with' });
    expect(group.querySelectorAll('button')).toHaveLength(9);
    for (const label of [
      'Create a skill',
      'Create a presentation',
      'Create a diagram',
      'Create a quiz',
      'Create a dashboard',
      'Show a map',
      'Create a mindmap',
      'Create flashcards',
      'Create a chart',
    ]) {
      expect(screen.getByText(label)).toBeInTheDocument();
    }
  });

  it('drops an opening phrase into the composer instead of sending', () => {
    const onPick = vi.fn();
    render(<ChatStarters onPick={onPick} />);
    fireEvent.click(screen.getByText('Create a presentation'));
    expect(onPick).toHaveBeenCalledWith('Create a presentation about ');
    fireEvent.click(screen.getByText('Create a skill'));
    expect(onPick).toHaveBeenCalledWith('Create a skill for ');
  });

  it('every prefill ends with a space so the user just keeps typing', () => {
    for (const s of STARTERS) expect(s.prefill.endsWith(' ')).toBe(true);
  });
});
