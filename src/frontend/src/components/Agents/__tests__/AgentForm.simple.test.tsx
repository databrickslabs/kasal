/**
 * Simple test to verify Jest setup without complex dependencies.
 */
import React from 'react';
import { render, screen } from '@testing-library/react';

// Simple component to test
const TestComponent = () => {
  return (
    <div>
      <h1>Template Test</h1>
      <p>System Template Info</p>
      <p>Prompt Template Info</p>
      <p>Response Template Info</p>
    </div>
  );
};

describe('Simple Template Tests', () => {
  it('should render template information', () => {
    render(<TestComponent />);
    
    expect(screen.getByText('Template Test')).toBeInTheDocument();
    expect(screen.getByText('System Template Info')).toBeInTheDocument();
    expect(screen.getByText('Prompt Template Info')).toBeInTheDocument();
    expect(screen.getByText('Response Template Info')).toBeInTheDocument();
  });

  it('should pass basic assertions', () => {
    expect(true).toBe(true);
    expect('system_template').toContain('template');
    expect(['system_template', 'prompt_template', 'response_template']).toHaveLength(3);
  });
});