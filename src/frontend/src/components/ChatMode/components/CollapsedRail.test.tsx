import React from 'react';
import { describe, it, expect, beforeEach } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/react';
import CollapsedRail from './CollapsedRail';
import { useAppStore } from '../store/appStore';

describe('CollapsedRail', () => {
  beforeEach(() => {
    useAppStore.setState({ sidebarOpen: false, theme: 'light', catalogOpen: false, savedCrews: [], savedFlows: [] });
  });

  it('expands the sidebar from the rail', () => {
    render(<CollapsedRail onNewChat={() => {}} />);
    fireEvent.click(screen.getByLabelText('Show chat history'));
    expect(useAppStore.getState().sidebarOpen).toBe(true);
  });

  it('starts a new chat from the rail', () => {
    let called = 0;
    render(<CollapsedRail onNewChat={() => { called += 1; }} />);
    fireEvent.click(screen.getByLabelText('New chat'));
    expect(called).toBe(1);
  });

  it('keeps the theme toggle available while collapsed', () => {
    render(<CollapsedRail onNewChat={() => {}} />);
    fireEvent.click(screen.getByLabelText('Switch to dark mode'));
    expect(useAppStore.getState().theme).toBe('dark');
    // Re-rendered label reflects the new state.
    expect(screen.getByLabelText('Switch to light mode')).toBeInTheDocument();
  });
});
