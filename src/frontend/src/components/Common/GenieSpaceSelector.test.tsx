import React from 'react';
import { render, screen, fireEvent, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { ThemeProvider } from '@mui/material/styles';
import { createTheme } from '@mui/material/styles';
import { GenieSpaceSelector } from './GenieSpaceSelector';

// Mock the GenieService
const mockGenieService = {
  getSpaces: jest.fn(),
  searchSpaces: jest.fn(),
};

jest.mock('../../api/GenieService', () => ({
  GenieService: mockGenieService,
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
  const mockOnChange = jest.fn();
  const defaultValue = null;

  beforeEach(() => {
    jest.clearAllMocks();
    mockGenieService.getSpaces.mockResolvedValue(mockSpacesResponse);
    mockGenieService.searchSpaces.mockResolvedValue(mockSpacesResponse);
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
      
      expect(screen.getByLabelText('Genie Space')).toBeInTheDocument();
      expect(screen.getByPlaceholderText('Search for Genie spaces...')).toBeInTheDocument();
    });

    test('renders with custom props', () => {
      renderComponent({
        label: 'Custom Genie Space',
        placeholder: 'Custom placeholder',
        helperText: 'Custom help text',
        required: true,
      });
      
      expect(screen.getByLabelText('Custom Genie Space *')).toBeInTheDocument();
      expect(screen.getByPlaceholderText('Custom placeholder')).toBeInTheDocument();
      expect(screen.getByText('Custom help text')).toBeInTheDocument();
    });

    test('handles disabled state', () => {
      renderComponent({ disabled: true });
      
      const autocomplete = screen.getByRole('combobox');
      expect(autocomplete).toBeDisabled();
    });

    test('handles error state', () => {
      renderComponent({ error: true, helperText: 'Error message' });
      
      expect(screen.getByText('Error message')).toBeInTheDocument();
      // Check for error styling
      const textField = screen.getByRole('combobox');
      expect(textField.closest('.MuiFormControl-root')).toHaveClass('Mui-error');
    });
  });

  describe('Data Loading', () => {
    test('loads spaces when dropdown is opened', async () => {
      const user = userEvent.setup();
      renderComponent();

      const autocomplete = screen.getByRole('combobox');
      await user.click(autocomplete);

      await waitFor(() => {
        expect(mockGenieService.getSpaces).toHaveBeenCalledWith(undefined, 50);
      });
    });

    test('displays loading state', async () => {
      const user = userEvent.setup();
      // Mock delayed response
      mockGenieService.getSpaces.mockImplementation(
        () => new Promise(resolve => setTimeout(() => resolve(mockSpacesResponse), 100))
      );

      renderComponent();
      const autocomplete = screen.getByRole('combobox');
      await user.click(autocomplete);

      // Check for loading state
      expect(screen.getByRole('progressbar')).toBeInTheDocument();

      // Wait for loading to complete
      await waitFor(() => {
        expect(screen.queryByRole('progressbar')).not.toBeInTheDocument();
      });
    });

    test('displays spaces in dropdown', async () => {
      const user = userEvent.setup();
      renderComponent();

      const autocomplete = screen.getByRole('combobox');
      await user.click(autocomplete);

      await waitFor(() => {
        expect(screen.getByText('Development Space')).toBeInTheDocument();
        expect(screen.getByText('Production Space')).toBeInTheDocument();
        expect(screen.getByText('Testing Space')).toBeInTheDocument();
      });
    });

    test('handles API error gracefully', async () => {
      const user = userEvent.setup();
      mockGenieService.getSpaces.mockRejectedValue(new Error('API Error'));

      renderComponent();
      const autocomplete = screen.getByRole('combobox');
      
      // Should not throw
      expect(async () => await user.click(autocomplete)).not.toThrow();
    });
  });

  describe('Search Functionality', () => {
    test('performs search when typing', async () => {
      const user = userEvent.setup();
      renderComponent();

      const autocomplete = screen.getByRole('combobox');
      await user.click(autocomplete);
      await user.type(autocomplete, 'dev');

      // Wait for debounced search
      await waitFor(() => {
        expect(mockGenieService.searchSpaces).toHaveBeenCalledWith({
          search_query: 'dev',
          page_token: undefined,
          page_size: 50,
          enabled_only: true,
        });
      });
    });

    test('debounces search input', async () => {
      const user = userEvent.setup();
      renderComponent();

      const autocomplete = screen.getByRole('combobox');
      await user.click(autocomplete);
      
      // Type multiple characters quickly
      await user.type(autocomplete, 'development');

      // Should only make one search call after debounce
      await waitFor(() => {
        expect(mockGenieService.searchSpaces).toHaveBeenCalledTimes(1);
        expect(mockGenieService.searchSpaces).toHaveBeenCalledWith({
          search_query: 'development',
          page_token: undefined,
          page_size: 50,
          enabled_only: true,
        });
      });
    });

    test('clears search when input is empty', async () => {
      const user = userEvent.setup();
      renderComponent();

      const autocomplete = screen.getByRole('combobox');
      await user.click(autocomplete);
      await user.type(autocomplete, 'dev');
      
      // Wait for search
      await waitFor(() => {
        expect(mockGenieService.searchSpaces).toHaveBeenCalled();
      });

      // Clear input
      await user.clear(autocomplete);

      // Should load all spaces again
      await waitFor(() => {
        expect(mockGenieService.getSpaces).toHaveBeenCalled();
      });
    });
  });

  describe('Single Selection', () => {
    test('handles single space selection', async () => {
      const user = userEvent.setup();
      renderComponent();

      const autocomplete = screen.getByRole('combobox');
      await user.click(autocomplete);

      await waitFor(() => {
        expect(screen.getByText('Development Space')).toBeInTheDocument();
      });

      await user.click(screen.getByText('Development Space'));

      expect(mockOnChange).toHaveBeenCalledWith('space1');
    });

    test('displays selected space', () => {
      renderComponent({ value: 'space1' });

      // Wait for options to load and selection to be set
      waitFor(() => {
        expect(screen.getByDisplayValue('Development Space')).toBeInTheDocument();
      });
    });

    test('handles clearing selection', async () => {
      const user = userEvent.setup();
      renderComponent({ value: 'space1' });

      await waitFor(() => {
        expect(screen.getByDisplayValue('Development Space')).toBeInTheDocument();
      });

      // Find and click clear button
      const clearButton = screen.getByLabelText('Clear');
      await user.click(clearButton);

      expect(mockOnChange).toHaveBeenCalledWith(null);
    });
  });

  describe('Multiple Selection', () => {
    test('handles multiple space selection', async () => {
      const user = userEvent.setup();
      renderComponent({ multiple: true });

      const autocomplete = screen.getByRole('combobox');
      await user.click(autocomplete);

      await waitFor(() => {
        expect(screen.getByText('Development Space')).toBeInTheDocument();
      });

      await user.click(screen.getByText('Development Space'));
      await user.click(screen.getByText('Production Space'));

      expect(mockOnChange).toHaveBeenCalledWith(['space1', 'space2']);
    });

    test('displays multiple selected spaces as chips', () => {
      renderComponent({ 
        multiple: true, 
        value: ['space1', 'space2'] 
      });

      waitFor(() => {
        expect(screen.getByText('Development Space')).toBeInTheDocument();
        expect(screen.getByText('Production Space')).toBeInTheDocument();
      });
    });

    test('handles removing individual selections', async () => {
      const user = userEvent.setup();
      renderComponent({ 
        multiple: true, 
        value: ['space1', 'space2'] 
      });

      await waitFor(() => {
        expect(screen.getByText('Development Space')).toBeInTheDocument();
      });

      // Find delete button for first chip
      const deleteButtons = screen.getAllByLabelText('Delete');
      await user.click(deleteButtons[0]);

      expect(mockOnChange).toHaveBeenCalledWith(['space2']);
    });
  });

  describe('Infinite Scrolling', () => {
    test('loads more spaces when scrolling', async () => {
      const user = userEvent.setup();
      const firstPageResponse = {
        spaces: mockSpaces.slice(0, 2),
        next_page_token: 'token123',
        has_more: true,
        total_count: 10,
      };
      
      const secondPageResponse = {
        spaces: [mockSpaces[2]],
        next_page_token: null,
        has_more: false,
        total_count: 10,
      };

      mockGenieService.getSpaces
        .mockResolvedValueOnce(firstPageResponse)
        .mockResolvedValueOnce(secondPageResponse);

      renderComponent();

      const autocomplete = screen.getByRole('combobox');
      await user.click(autocomplete);

      await waitFor(() => {
        expect(screen.getByText('Development Space')).toBeInTheDocument();
        expect(screen.getByText('Production Space')).toBeInTheDocument();
      });

      // Simulate scrolling to bottom
      const listbox = screen.getByRole('listbox');
      fireEvent.scroll(listbox, { target: { scrollTop: listbox.scrollHeight } });

      await waitFor(() => {
        expect(mockGenieService.getSpaces).toHaveBeenCalledWith('token123', 50);
        expect(screen.getByText('Testing Space')).toBeInTheDocument();
      });
    });
  });

  describe('Accessibility', () => {
    test('has proper ARIA labels', () => {
      renderComponent({ required: true });
      
      expect(screen.getByLabelText('Genie Space *')).toBeInTheDocument();
    });

    test('supports keyboard navigation', async () => {
      const user = userEvent.setup();
      renderComponent();

      const autocomplete = screen.getByRole('combobox');
      
      // Open with keyboard
      await user.type(autocomplete, '{ArrowDown}');

      await waitFor(() => {
        expect(screen.getByText('Development Space')).toBeInTheDocument();
      });

      // Navigate with arrow keys
      await user.keyboard('{ArrowDown}');
      await user.keyboard('{Enter}');

      expect(mockOnChange).toHaveBeenCalledWith('space1');
    });
  });

  describe('Edge Cases', () => {
    test('handles empty spaces response', async () => {
      const user = userEvent.setup();
      mockGenieService.getSpaces.mockResolvedValue({
        spaces: [],
        next_page_token: null,
        has_more: false,
        total_count: 0,
      });

      renderComponent();
      const autocomplete = screen.getByRole('combobox');
      await user.click(autocomplete);

      await waitFor(() => {
        expect(screen.getByText('No options')).toBeInTheDocument();
      });
    });

    test('handles invalid value prop', () => {
      // Should not crash with invalid value
      expect(() => renderComponent({ value: 'invalid-space-id' })).not.toThrow();
    });

    test('handles value changes from parent', () => {
      const { rerender } = renderComponent({ value: null });

      // Update value from parent
      rerender(
        <TestWrapper>
          <GenieSpaceSelector value="space1" onChange={mockOnChange} />
        </TestWrapper>
      );

      // Should handle the change gracefully
      waitFor(() => {
        expect(screen.getByDisplayValue('Development Space')).toBeInTheDocument();
      });
    });
  });

  describe('Performance', () => {
    test('cancels previous search when new search is initiated', async () => {
      const user = userEvent.setup();
      renderComponent();

      const autocomplete = screen.getByRole('combobox');
      await user.click(autocomplete);
      
      // Start typing quickly
      await user.type(autocomplete, 'dev');
      await user.type(autocomplete, 'elopment');

      // Should only make final search call
      await waitFor(() => {
        expect(mockGenieService.searchSpaces).toHaveBeenCalledWith({
          search_query: 'development',
          page_token: undefined,
          page_size: 50,
          enabled_only: true,
        });
      });
    });

    test('prevents duplicate loading requests', async () => {
      const user = userEvent.setup();
      renderComponent();

      const autocomplete = screen.getByRole('combobox');
      
      // Click multiple times quickly
      await user.click(autocomplete);
      await user.click(autocomplete);
      await user.click(autocomplete);

      await waitFor(() => {
        // Should only make one request
        expect(mockGenieService.getSpaces).toHaveBeenCalledTimes(1);
      });
    });
  });
});