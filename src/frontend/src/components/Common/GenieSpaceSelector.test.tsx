import { vi, beforeEach, describe, test, expect } from 'vitest';
import React from 'react';
import { render, screen, fireEvent, waitFor } from '@testing-library/react';
import { ThemeProvider } from '@mui/material/styles';
import { createTheme } from '@mui/material/styles';
import { GenieSpaceSelector } from './GenieSpaceSelector';

// Mock the GenieService - must use vi.hoisted for variables used in vi.mock
const mockGetSpaces = vi.hoisted(() => vi.fn());
const mockSearchSpaces = vi.hoisted(() => vi.fn());

vi.mock('../../api/GenieService', () => ({
  GenieService: {
    getSpaces: mockGetSpaces,
    searchSpaces: mockSearchSpaces,
  },
}));

// Create a theme for testing
const theme = createTheme();

// Test wrapper component
const TestWrapper: React.FC<{ children: React.ReactNode }> = ({ children }) => (
  <ThemeProvider theme={theme}>
    {children}
  </ThemeProvider>
);

// Mock data
const mockSpaces = [
  { id: 'space1', name: 'Development Space', description: 'Space for development work' },
  { id: 'space2', name: 'Production Space', description: 'Production environment space' },
  { id: 'space3', name: 'Testing Space', description: 'Testing environment space' },
];

const mockSpacesResponse = {
  spaces: mockSpaces,
  next_page_token: null,
  has_more: false,
  total_count: 3,
};

describe('GenieSpaceSelector', () => {
  const mockOnChange = vi.fn();
  const defaultValue = null;

  beforeEach(() => {
    vi.clearAllMocks();
    mockGetSpaces.mockResolvedValue(mockSpacesResponse);
    mockSearchSpaces.mockResolvedValue(mockSpacesResponse);
  });

  const renderComponent = (props = {}) => {
    const defaultProps = {
      value: defaultValue,
      onChange: mockOnChange,
      ...props,
    };

    return render(
      <TestWrapper>
        <GenieSpaceSelector {...defaultProps} />
      </TestWrapper>
    );
  };

  describe('Basic Rendering', () => {
    test('renders with default props', () => {
      renderComponent();

      expect(screen.getByRole('combobox')).toBeInTheDocument();
    });

    test('renders with custom props', () => {
      renderComponent({
        placeholder: 'Custom placeholder',
        helperText: 'Custom help text',
      });

      expect(screen.getByPlaceholderText('Custom placeholder')).toBeInTheDocument();
      expect(screen.getByText('Custom help text')).toBeInTheDocument();
    });

    test('handles disabled state', () => {
      renderComponent({ disabled: true });

      const autocomplete = screen.getByRole('combobox');
      expect(autocomplete).toBeDisabled();
    });
  });

  describe('Data Loading', () => {
    test('calls getSpaces when dropdown is focused and opened', async () => {
      renderComponent();

      const autocomplete = screen.getByRole('combobox');
      // Focus and then open the dropdown
      fireEvent.focus(autocomplete);
      fireEvent.mouseDown(autocomplete);

      // Wait for the async call - may take some time due to component logic
      await waitFor(() => {
        expect(mockGetSpaces).toHaveBeenCalled();
      }, { timeout: 3000 });
    });
  });

  describe('Accessibility', () => {
    test('has proper ARIA role', () => {
      renderComponent();

      expect(screen.getByRole('combobox')).toBeInTheDocument();
    });
  });
});
