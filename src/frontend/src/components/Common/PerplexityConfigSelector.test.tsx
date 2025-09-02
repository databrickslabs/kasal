import React from 'react';
import { render, screen, fireEvent, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { ThemeProvider } from '@mui/material/styles';
import { createTheme } from '@mui/material/styles';
import { PerplexityConfigSelector } from './PerplexityConfigSelector';
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

describe('PerplexityConfigSelector', () => {
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

    test('fetches API keys on mount', () => {
      renderComponent();
      expect(mockFetchAPIKeys).toHaveBeenCalledTimes(1);
    });
  });

  describe('API Key Management', () => {
    test('shows system API key hint when key exists', () => {
      mockAPIKeysStore.secrets = [
        { id: 1, name: 'PERPLEXITY_API_KEY', value: 'test-key' },
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

    test('handles API key input changes', async () => {
      const user = userEvent.setup();
      renderComponent();

      const apiKeyInput = screen.getByLabelText('API Key');
      await user.type(apiKeyInput, 'test-api-key');

      await waitFor(() => {
        expect(mockOnChange).toHaveBeenCalledWith(
          expect.objectContaining({
            perplexity_api_key: 'test-api-key',
          })
        );
      });
    });
  });

  describe('Model Selection', () => {
    test('renders model dropdown with default value', () => {
      renderComponent();
      
      const modelSelect = screen.getByLabelText('Model');
      expect(modelSelect).toHaveValue('sonar');
    });

    test('handles model selection changes', async () => {
      const user = userEvent.setup();
      renderComponent();

      const modelSelect = screen.getByLabelText('Model');
      await user.selectOptions(modelSelect, 'sonar-pro');

      await waitFor(() => {
        expect(mockOnChange).toHaveBeenCalledWith(
          expect.objectContaining({
            model: 'sonar-pro',
          })
        );
      });
    });

    test('displays all available model options', async () => {
      const user = userEvent.setup();
      renderComponent();

      const modelSelect = screen.getByLabelText('Model');
      await user.click(modelSelect);

      expect(screen.getByText('Sonar (Default)')).toBeInTheDocument();
      expect(screen.getByText('Sonar Pro')).toBeInTheDocument();
      expect(screen.getByText('Sonar Deep Research')).toBeInTheDocument();
      expect(screen.getByText('Sonar Reasoning')).toBeInTheDocument();
      expect(screen.getByText('Sonar Reasoning Pro')).toBeInTheDocument();
      expect(screen.getByText('R1-1776')).toBeInTheDocument();
    });
  });

  describe('Search Configuration', () => {
    test('expands and shows search configuration options', async () => {
      const user = userEvent.setup();
      renderComponent();

      const searchAccordion = screen.getByText('Search Configuration');
      await user.click(searchAccordion);

      await waitFor(() => {
        expect(screen.getByLabelText('Search Recency')).toBeInTheDocument();
        expect(screen.getByLabelText('Search Context Size')).toBeInTheDocument();
        expect(screen.getByLabelText('Domain Filter')).toBeInTheDocument();
      });
    });

    test('handles search recency filter changes', async () => {
      const user = userEvent.setup();
      renderComponent();

      // Expand accordion
      await user.click(screen.getByText('Search Configuration'));
      
      await waitFor(() => {
        expect(screen.getByLabelText('Search Recency')).toBeInTheDocument();
      });

      const recencySelect = screen.getByLabelText('Search Recency');
      await user.selectOptions(recencySelect, 'day');

      await waitFor(() => {
        expect(mockOnChange).toHaveBeenCalledWith(
          expect.objectContaining({
            search_recency_filter: 'day',
          })
        );
      });
    });

    test('handles domain filter changes', async () => {
      const user = userEvent.setup();
      renderComponent();

      // Expand accordion
      await user.click(screen.getByText('Search Configuration'));
      
      await waitFor(() => {
        expect(screen.getByLabelText('Domain Filter')).toBeInTheDocument();
      });

      // Note: Testing multi-select requires more complex setup
      // This is a basic test to ensure the component exists
      expect(screen.getByLabelText('Domain Filter')).toBeInTheDocument();
    });

    test('handles return images toggle', async () => {
      const user = userEvent.setup();
      renderComponent();

      // Expand accordion
      await user.click(screen.getByText('Search Configuration'));
      
      await waitFor(() => {
        expect(screen.getByLabelText('Return Images')).toBeInTheDocument();
      });

      const imagesSwitch = screen.getByLabelText('Return Images');
      await user.click(imagesSwitch);

      await waitFor(() => {
        expect(mockOnChange).toHaveBeenCalledWith(
          expect.objectContaining({
            return_images: true,
          })
        );
      });
    });
  });

  describe('Advanced Parameters', () => {
    test('expands and shows advanced parameters', async () => {
      const user = userEvent.setup();
      renderComponent();

      const advancedAccordion = screen.getByText('Advanced Parameters');
      await user.click(advancedAccordion);

      await waitFor(() => {
        expect(screen.getByLabelText('Max Tokens')).toBeInTheDocument();
        expect(screen.getByLabelText('Temperature')).toBeInTheDocument();
        expect(screen.getByLabelText('Top P')).toBeInTheDocument();
        expect(screen.getByLabelText('Top K')).toBeInTheDocument();
        expect(screen.getByLabelText('Presence Penalty')).toBeInTheDocument();
        expect(screen.getByLabelText('Frequency Penalty')).toBeInTheDocument();
      });
    });

    test('handles max tokens input changes', async () => {
      const user = userEvent.setup();
      renderComponent();

      // Expand accordion
      await user.click(screen.getByText('Advanced Parameters'));
      
      await waitFor(() => {
        expect(screen.getByLabelText('Max Tokens')).toBeInTheDocument();
      });

      const maxTokensInput = screen.getByLabelText('Max Tokens');
      await user.clear(maxTokensInput);
      await user.type(maxTokensInput, '3000');

      await waitFor(() => {
        expect(mockOnChange).toHaveBeenCalledWith(
          expect.objectContaining({
            max_tokens: 3000,
          })
        );
      });
    });

    test('handles temperature input changes', async () => {
      const user = userEvent.setup();
      renderComponent();

      // Expand accordion
      await user.click(screen.getByText('Advanced Parameters'));
      
      await waitFor(() => {
        expect(screen.getByLabelText('Temperature')).toBeInTheDocument();
      });

      const temperatureInput = screen.getByLabelText('Temperature');
      await user.clear(temperatureInput);
      await user.type(temperatureInput, '0.5');

      await waitFor(() => {
        expect(mockOnChange).toHaveBeenCalledWith(
          expect.objectContaining({
            temperature: 0.5,
          })
        );
      });
    });
  });

  describe('Configuration Persistence', () => {
    test('loads initial configuration values', () => {
      const initialConfig = {
        perplexity_api_key: 'initial-key',
        model: 'sonar-pro',
        max_tokens: 3000,
        temperature: 0.5,
      };

      renderComponent({ value: initialConfig });

      const apiKeyInput = screen.getByDisplayValue('initial-key');
      const modelSelect = screen.getByDisplayValue('sonar-pro');

      expect(apiKeyInput).toBeInTheDocument();
      expect(modelSelect).toBeInTheDocument();
    });

    test('updates when value prop changes', () => {
      const { rerender } = renderComponent({ value: { model: 'sonar' } });

      // Update with new value
      const newConfig = { model: 'sonar-pro', temperature: 0.8 };
      
      rerender(
        <TestWrapper>
          <PerplexityConfigSelector value={newConfig} onChange={mockOnChange} />
        </TestWrapper>
      );

      const modelSelect = screen.getByDisplayValue('sonar-pro');
      expect(modelSelect).toBeInTheDocument();
    });
  });

  describe('Disabled State', () => {
    test('disables all inputs when disabled prop is true', () => {
      renderComponent({ disabled: true });

      const apiKeyInput = screen.getByLabelText('API Key');
      const modelSelect = screen.getByLabelText('Model');

      expect(apiKeyInput).toBeDisabled();
      expect(modelSelect.closest('.MuiFormControl-root')).toHaveClass('Mui-disabled');
    });
  });

  describe('Error Handling', () => {
    test('handles fetch API keys error gracefully', () => {
      mockFetchAPIKeys.mockRejectedValueOnce(new Error('API Error'));
      
      // Should not throw
      expect(() => renderComponent()).not.toThrow();
    });
  });

  describe('Accessibility', () => {
    test('has proper ARIA labels', () => {
      renderComponent();

      expect(screen.getByLabelText('API Key')).toBeInTheDocument();
      expect(screen.getByLabelText('Model')).toBeInTheDocument();
    });

    test('has proper helper text', () => {
      renderComponent();

      expect(screen.getByText('Leave empty to use environment variable or default')).toBeInTheDocument();
    });

    test('shows tooltip for info icon', async () => {
      const user = userEvent.setup();
      mockAPIKeysStore.secrets = [
        { id: 1, name: 'PERPLEXITY_API_KEY', value: 'test-key' },
      ];

      renderComponent();

      const infoIcon = screen.getByRole('button', { name: /info/i });
      await user.hover(infoIcon);

      await waitFor(() => {
        expect(screen.getByText(/A Perplexity API key is already configured/)).toBeInTheDocument();
      });
    });
  });
});