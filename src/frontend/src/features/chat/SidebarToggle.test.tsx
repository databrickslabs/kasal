import React from 'react';
import { describe, it, expect, beforeEach } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/react';
import SidebarToggle from './SidebarToggle';
import { useAppStore } from './store/appStore';

describe('SidebarToggle (top-bar slot)', () => {
  beforeEach(() => useAppStore.setState({ sidebarOpen: true }));

  it('collapses and expands the sidebar from one fixed control', () => {
    render(<SidebarToggle />);
    fireEvent.click(screen.getByLabelText('Hide sessions'));
    expect(useAppStore.getState().sidebarOpen).toBe(false);
    fireEvent.click(screen.getByLabelText('Show sessions'));
    expect(useAppStore.getState().sidebarOpen).toBe(true);
  });
});
