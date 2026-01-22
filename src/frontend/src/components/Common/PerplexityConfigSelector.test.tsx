import { vi, beforeEach, describe, test, expect } from 'vitest';
import React from 'react';
import { render, screen, waitFor } from '@testing-library/react';
import { ThemeProvider } from '@mui/material/styles';
import { createTheme } from '@mui/material/styles';
import { PerplexityConfigSelector } from './PerplexityConfigSelector';

// Mock the API keys store - must use vi.hoisted
const mockFetchAPIKeys = vi.hoisted(() => vi.fn());

vi.mock('../../store/apiKeys', () => ({
  useAPIKeysStore: () => ({
    secrets: [],
    loading: false,
    error: null,
    fetchAPIKeys: mockFetchAPIKeys,
  }),
}));

// Create a theme for testing
const theme = createTheme();

// Test wrapper component
const TestWrapper: React.FC<{ children: React.ReactNode }> = ({ children }) => (
  <ThemeProvider theme={theme}>
    {children}
  </ThemeProvider>
);

describe('PerplexityConfigSelector', () => {
  const mockOnChange = vi.fn();
  const defaultConfig = {};

  beforeEach(() => {
    vi.clearAllMocks();
  });

  const renderComponent = (props = {}) => {
    const defaultProps = {
      value: defaultConfig,
      onChange: mockOnChange,
      ...props,
    };

    return render(
      <TestWrapper>
        <PerplexityConfigSelector {...defaultProps} />
      </TestWrapper>
    );
  };

  describe('Basic Rendering', () => {
    test('renders with default props', () => {
      renderComponent();

      expect(screen.getByText('Perplexity Configuration')).toBeInTheDocument();
      expect(screen.getByText('Configure Perplexity AI search parameters')).toBeInTheDocument();
    });

    test('renders custom label and helper text', () => {
      renderComponent({
        label: 'Custom Perplexity Settings',
        helperText: 'Custom help text for configuration',
      });

      expect(screen.getByText('Custom Perplexity Settings')).toBeInTheDocument();
      expect(screen.getByText('Custom help text for configuration')).toBeInTheDocument();
    });

    test('fetches API keys on mount', async () => {
      renderComponent();

      await waitFor(() => {
        expect(mockFetchAPIKeys).toHaveBeenCalled();
      });
    });
  });

  describe('Configuration Fields', () => {
    test('renders Perplexity configuration card', () => {
      renderComponent();

      // The component should render the main configuration card
      expect(screen.getByText('Perplexity Configuration')).toBeInTheDocument();
    });
  });

  describe('Disabled State', () => {
    test('renders when disabled', () => {
      renderComponent({ disabled: true });

      expect(screen.getByText('Perplexity Configuration')).toBeInTheDocument();
    });
  });
});
