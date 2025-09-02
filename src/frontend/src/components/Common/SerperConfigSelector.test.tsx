import React from 'react';
import { render, screen, fireEvent, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { ThemeProvider } from '@mui/material/styles';
import { createTheme } from '@mui/material/styles';
import { SerperConfigSelector } from './SerperConfigSelector';
import { ApiKey } from '../../types/apiKeys';

// Mock the API keys store
const mockFetchAPIKeys = jest.fn();
const mockAPIKeysStore: { secrets: ApiKey[]; fetchAPIKeys: jest.Mock } = {
  secrets: [],
  fetchAPIKeys: mockFetchAPIKeys,
};

jest.mock('../../store/apiKeys', () => ({
  useAPIKeysStore: () => mockAPIKeysStore,
}));

// Create a theme for testing
const theme = createTheme();

// Test wrapper component
const TestWrapper: React.FC<{ children: React.ReactNode }> = ({ children }) => (
  <ThemeProvider theme={theme}>
    {children}
  </ThemeProvider>
);

describe('SerperConfigSelector', () => {
  const mockOnChange = jest.fn();
  const defaultConfig = {};

  beforeEach(() => {
    jest.clearAllMocks();
    mockAPIKeysStore.secrets = [];
  });

  const renderComponent = (props = {}) => {
    const defaultProps = {
      value: defaultConfig,
      onChange: mockOnChange,
      ...props,
    };

    return render(
      <TestWrapper>
        <SerperConfigSelector {...defaultProps} />
      </TestWrapper>
    );
  };

  describe('Basic Rendering', () => {
    test('renders with default props', () => {
      renderComponent();
      
      expect(screen.getByText('Serper Configuration')).toBeInTheDocument();
      expect(screen.getByText('Configure Serper.dev search parameters')).toBeInTheDocument();
    });

    test('renders custom label and helper text', () => {
      renderComponent({
        label: 'Custom Serper Settings',
        helperText: 'Custom help text for configuration',
      });
      
      expect(screen.getByText('Custom Serper Settings')).toBeInTheDocument();
      expect(screen.getByText('Custom help text for configuration')).toBeInTheDocument();
    });

    test('fetches API keys on mount', () => {
      renderComponent();
      expect(mockFetchAPIKeys).toHaveBeenCalledTimes(1);
    });
  });

  describe('API Key Management', () => {
    test('shows system API key hint when key exists', () => {
      mockAPIKeysStore.secrets = [
        { id: 1, name: 'SERPER_API_KEY', value: 'test-key' },
      ];

      renderComponent();
      
      expect(screen.getByText(/System API key configured/)).toBeInTheDocument();
      expect(screen.getByLabelText('API Key Override (Optional)')).toBeInTheDocument();
    });

    test('shows regular API key field when no system key exists', () => {
      mockAPIKeysStore.secrets = [];

      renderComponent();
      
      expect(screen.queryByText(/System API key configured/)).not.toBeInTheDocument();
      expect(screen.getByLabelText('API Key')).toBeInTheDocument();
    });

    test('detects serper key by name containing "serper"', () => {
      mockAPIKeysStore.secrets = [
        { id: 1, name: 'my-serper-key', value: 'test-key' },
      ];

      renderComponent();
      
      expect(screen.getByText(/System API key configured/)).toBeInTheDocument();
    });

    test('handles API key input changes', async () => {
      const user = userEvent.setup();
      renderComponent();

      const apiKeyInput = screen.getByLabelText('API Key');
      await user.type(apiKeyInput, 'test-serper-key');

      await waitFor(() => {
        expect(mockOnChange).toHaveBeenCalledWith(
          expect.objectContaining({
            serper_api_key: 'test-serper-key',
          })
        );
      });
    });
  });

  describe('Endpoint Selection', () => {
    test('renders endpoint dropdown with default value', () => {
      renderComponent();
      
      const endpointSelect = screen.getByLabelText('Search Endpoint');
      expect(endpointSelect).toHaveValue('search');
    });

    test('displays current endpoint URL', () => {
      renderComponent();
      
      expect(screen.getByText('Current Endpoint:')).toBeInTheDocument();
      expect(screen.getByText('https://google.serper.dev/search')).toBeInTheDocument();
    });

    test('handles endpoint selection changes and updates URL', async () => {
      const user = userEvent.setup();
      renderComponent();

      const endpointSelect = screen.getByLabelText('Search Endpoint');
      await user.selectOptions(endpointSelect, 'news');

      await waitFor(() => {
        expect(mockOnChange).toHaveBeenCalledWith(
          expect.objectContaining({
            endpoint_type: 'news',
            search_url: 'https://google.serper.dev/news',
          })
        );
      });

      // Check that the URL display updates
      expect(screen.getByText('https://google.serper.dev/news')).toBeInTheDocument();
    });

    test('displays all available endpoint options with descriptions', async () => {
      const user = userEvent.setup();
      renderComponent();

      const endpointSelect = screen.getByLabelText('Search Endpoint');
      await user.click(endpointSelect);

      // Check that all endpoints are available
      expect(screen.getByText('Search')).toBeInTheDocument();
      expect(screen.getByText('News')).toBeInTheDocument();
      expect(screen.getByText('Images')).toBeInTheDocument();
      expect(screen.getByText('Videos')).toBeInTheDocument();
      expect(screen.getByText('Places')).toBeInTheDocument();
      expect(screen.getByText('Shopping')).toBeInTheDocument();
      expect(screen.getByText('Scholar')).toBeInTheDocument();
      expect(screen.getByText('Patents')).toBeInTheDocument();
      expect(screen.getByText('Autocomplete')).toBeInTheDocument();

      // Check that descriptions are shown
      expect(screen.getByText(/Regular Google search results/)).toBeInTheDocument();
      expect(screen.getByText(/Google News results/)).toBeInTheDocument();
    });
  });

  describe('Basic Configuration', () => {
    test('renders number of results input with default value', () => {
      renderComponent();
      
      const resultsInput = screen.getByLabelText('Number of Results');
      expect(resultsInput).toHaveValue(10);
    });

    test('handles number of results changes', async () => {
      const user = userEvent.setup();
      renderComponent();

      const resultsInput = screen.getByLabelText('Number of Results');
      await user.clear(resultsInput);
      await user.type(resultsInput, '25');

      await waitFor(() => {
        expect(mockOnChange).toHaveBeenCalledWith(
          expect.objectContaining({
            n_results: 25,
          })
        );
      });
    });

    test('validates number of results range', () => {
      renderComponent();
      
      const resultsInput = screen.getByLabelText('Number of Results');
      expect(resultsInput).toHaveAttribute('min', '1');
      expect(resultsInput).toHaveAttribute('max', '100');
    });
  });

  describe('Geographic Configuration', () => {
    test('expands and shows geographic settings', async () => {
      const user = userEvent.setup();
      renderComponent();

      const geoAccordion = screen.getByText('Geographic & Language Settings');
      await user.click(geoAccordion);

      await waitFor(() => {
        expect(screen.getByLabelText('Country')).toBeInTheDocument();
        expect(screen.getByLabelText('Language Locale')).toBeInTheDocument();
        expect(screen.getByLabelText('Specific Location')).toBeInTheDocument();
      });
    });

    test('handles country selection changes', async () => {
      const user = userEvent.setup();
      renderComponent();

      // Expand accordion
      await user.click(screen.getByText('Geographic & Language Settings'));
      
      await waitFor(() => {
        expect(screen.getByLabelText('Country')).toBeInTheDocument();
      });

      const countrySelect = screen.getByLabelText('Country');
      await user.selectOptions(countrySelect, 'gb');

      await waitFor(() => {
        expect(mockOnChange).toHaveBeenCalledWith(
          expect.objectContaining({
            country: 'gb',
          })
        );
      });
    });

    test('displays all country options', async () => {
      const user = userEvent.setup();
      renderComponent();

      // Expand accordion
      await user.click(screen.getByText('Geographic & Language Settings'));
      
      await waitFor(() => {
        expect(screen.getByLabelText('Country')).toBeInTheDocument();
      });

      const countrySelect = screen.getByLabelText('Country');
      await user.click(countrySelect);

      expect(screen.getByText('United States (us)')).toBeInTheDocument();
      expect(screen.getByText('United Kingdom (gb)')).toBeInTheDocument();
      expect(screen.getByText('Canada (ca)')).toBeInTheDocument();
      expect(screen.getByText('Germany (de)')).toBeInTheDocument();
    });

    test('handles language locale changes', async () => {
      const user = userEvent.setup();
      renderComponent();

      // Expand accordion
      await user.click(screen.getByText('Geographic & Language Settings'));
      
      await waitFor(() => {
        expect(screen.getByLabelText('Language Locale')).toBeInTheDocument();
      });

      const localeSelect = screen.getByLabelText('Language Locale');
      await user.selectOptions(localeSelect, 'fr');

      await waitFor(() => {
        expect(mockOnChange).toHaveBeenCalledWith(
          expect.objectContaining({
            locale: 'fr',
          })
        );
      });
    });

    test('handles specific location input', async () => {
      const user = userEvent.setup();
      renderComponent();

      // Expand accordion
      await user.click(screen.getByText('Geographic & Language Settings'));
      
      await waitFor(() => {
        expect(screen.getByLabelText('Specific Location')).toBeInTheDocument();
      });

      const locationInput = screen.getByLabelText('Specific Location');
      await user.type(locationInput, 'New York');

      await waitFor(() => {
        expect(mockOnChange).toHaveBeenCalledWith(
          expect.objectContaining({
            location: 'New York',
          })
        );
      });
    });
  });

  describe('Configuration Persistence', () => {
    test('loads initial configuration values', () => {
      const initialConfig = {
        serper_api_key: 'initial-key',
        endpoint_type: 'news',
        n_results: 25,
        country: 'gb',
        locale: 'fr',
        location: 'London',
      };

      renderComponent({ value: initialConfig });

      const apiKeyInput = screen.getByDisplayValue('initial-key');
      const endpointSelect = screen.getByDisplayValue('news');
      const resultsInput = screen.getByDisplayValue('25');

      expect(apiKeyInput).toBeInTheDocument();
      expect(endpointSelect).toBeInTheDocument();
      expect(resultsInput).toBeInTheDocument();
    });

    test('updates when value prop changes', () => {
      const { rerender } = renderComponent({ value: { endpoint_type: 'search' } });

      // Update with new value
      const newConfig = { endpoint_type: 'images', n_results: 50 };
      
      rerender(
        <TestWrapper>
          <SerperConfigSelector value={newConfig} onChange={mockOnChange} />
        </TestWrapper>
      );

      const endpointSelect = screen.getByDisplayValue('images');
      const resultsInput = screen.getByDisplayValue('50');
      
      expect(endpointSelect).toBeInTheDocument();
      expect(resultsInput).toBeInTheDocument();
    });
  });

  describe('Disabled State', () => {
    test('disables all inputs when disabled prop is true', () => {
      renderComponent({ disabled: true });

      const apiKeyInput = screen.getByLabelText('API Key');
      const endpointSelect = screen.getByLabelText('Search Endpoint');
      const resultsInput = screen.getByLabelText('Number of Results');

      expect(apiKeyInput).toBeDisabled();
      expect(endpointSelect.closest('.MuiFormControl-root')).toHaveClass('Mui-disabled');
      expect(resultsInput).toBeDisabled();
    });
  });

  describe('Help Information', () => {
    test('displays helpful tip about Serper endpoints', () => {
      renderComponent();
      
      expect(screen.getByText(/Serper.dev provides access to multiple Google search types/)).toBeInTheDocument();
      expect(screen.getByText(/Choose from Search, News, Images, Videos, Places, Shopping, Scholar, Patents, and Autocomplete/)).toBeInTheDocument();
    });
  });

  describe('Error Handling', () => {
    test('handles fetch API keys error gracefully', () => {
      mockFetchAPIKeys.mockRejectedValueOnce(new Error('API Error'));
      
      // Should not throw
      expect(() => renderComponent()).not.toThrow();
    });

    test('handles invalid number input', async () => {
      const user = userEvent.setup();
      renderComponent();

      const resultsInput = screen.getByLabelText('Number of Results');
      await user.clear(resultsInput);
      await user.type(resultsInput, 'invalid');

      // Should fallback to default value
      await waitFor(() => {
        expect(mockOnChange).toHaveBeenCalledWith(
          expect.objectContaining({
            n_results: 10,
          })
        );
      });
    });
  });

  describe('Accessibility', () => {
    test('has proper ARIA labels', () => {
      renderComponent();

      expect(screen.getByLabelText('API Key')).toBeInTheDocument();
      expect(screen.getByLabelText('Search Endpoint')).toBeInTheDocument();
      expect(screen.getByLabelText('Number of Results')).toBeInTheDocument();
    });

    test('has proper helper text', () => {
      renderComponent();

      expect(screen.getByText('Enter your Serper.dev API key, or leave empty to use environment variable')).toBeInTheDocument();
      expect(screen.getByText('Number of search results to return (1-100)')).toBeInTheDocument();
    });

    test('shows tooltip for info icon when system key exists', async () => {
      const user = userEvent.setup();
      mockAPIKeysStore.secrets = [
        { id: 1, name: 'SERPER_API_KEY', value: 'test-key' },
      ];

      renderComponent();

      const infoIcon = screen.getByRole('button', { name: /info/i });
      await user.hover(infoIcon);

      await waitFor(() => {
        expect(screen.getByText(/A Serper API key is already configured/)).toBeInTheDocument();
      });
    });
  });

  describe('URL Management', () => {
    test('automatically updates search_url when endpoint_type changes', async () => {
      const user = userEvent.setup();
      renderComponent();

      const endpointSelect = screen.getByLabelText('Search Endpoint');
      
      // Change to images endpoint
      await user.selectOptions(endpointSelect, 'images');

      await waitFor(() => {
        expect(mockOnChange).toHaveBeenCalledWith(
          expect.objectContaining({
            endpoint_type: 'images',
            search_url: 'https://google.serper.dev/images',
          })
        );
      });

      // Change to scholar endpoint
      await user.selectOptions(endpointSelect, 'scholar');

      await waitFor(() => {
        expect(mockOnChange).toHaveBeenCalledWith(
          expect.objectContaining({
            endpoint_type: 'scholar',
            search_url: 'https://google.serper.dev/scholar',
          })
        );
      });
    });
  });
});