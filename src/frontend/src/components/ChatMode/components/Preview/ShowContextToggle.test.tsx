import React from 'react';
import { describe, it, expect, beforeEach } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/react';
import ShowContextToggle from './ShowContextToggle';
import { useExecutionStore } from '../../store/executionStore';

describe('ShowContextToggle', () => {
  beforeEach(() => {
    useExecutionStore.getState().setShowRetrievedContext(false);
  });

  it('reflects the store flag and toggles it on click', () => {
    render(<ShowContextToggle />);
    const cb = screen.getByLabelText('Show retrieved context') as HTMLInputElement;
    expect(cb.checked).toBe(false);
    fireEvent.click(cb);
    expect(useExecutionStore.getState().showRetrievedContext).toBe(true);
  });
});
