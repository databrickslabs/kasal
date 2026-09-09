/**
 * Unit tests for LLMLogs component.
 *
 * Tests the LLM logs viewer including data fetching, refresh functionality,
 * and verifying no polling occurs.
 */
import { render, screen, fireEvent, waitFor } from '@testing-library/react';
import { vi, describe, it, expect, beforeEach, afterEach } from 'vitest';
import LLMLogs from './LLMLogs';

// Mock the log service
const mockGetLLMLogs = vi.fn();

vi.mock('../../../api/execution/LogService', () => ({
  default: {
    getLLMLogs: (...args: any[]) => mockGetLLMLogs(...args),
  },
}));

// Mock react-i18next
vi.mock('react-i18next', () => ({
  useTranslation: () => ({
    t: (key: string) => {
      const translations: Record<string, string> = {
        'logs.title': 'LLM API Logs',
        'logs.filterByEndpoint': 'Filter by Endpoint',
        'logs.allEndpoints': 'All Endpoints',
        'logs.noLogs': 'No logs available',
        'logs.refresh': 'Refresh',
        'logs.columns.timestamp': 'Timestamp',
        'logs.columns.endpoint': 'Endpoint',
        'logs.columns.model': 'Model',
        'logs.columns.tokens': 'Tokens',
        'logs.columns.duration': 'Duration',
        'logs.columns.status': 'Status',
        'logs.details.title': 'Request Details',
        'logs.details.prompt': 'Prompt:',
        'logs.details.response': 'Response:',
        'logs.details.additionalData': 'Additional Data:',
        'logs.details.error': 'Error:',
      };
      return translations[key] || key;
    },
  }),
}));

const mockLogs = [
  {
    id: '1',
    created_at: '2024-01-15T10:30:00Z',
    endpoint: 'generate-crew',
    model: 'gpt-4',
    tokens_used: 1500,
    duration_ms: 2500,
    status: 'success',
    prompt: 'Test prompt 1',
    response: 'Test response 1',
    extra_data: null,
    error_message: null,
  },
  {
    id: '2',
    created_at: '2024-01-15T10:35:00Z',
    endpoint: 'generate-agent',
    model: 'gpt-3.5-turbo',
    tokens_used: 800,
    duration_ms: 1200,
    status: 'error',
    prompt: 'Test prompt 2',
    response: '',
    extra_data: { key: 'value' },
    error_message: 'API rate limit exceeded',
  },
];

describe('LLMLogs', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockGetLLMLogs.mockResolvedValue(mockLogs);
  });

  afterEach(() => {
    vi.resetAllMocks();
  });

  describe('Initial Render', () => {
    it('shows loading state initially', () => {
      mockGetLLMLogs.mockImplementation(() => new Promise(() => {}));

      render(<LLMLogs />);

      expect(screen.getByRole('progressbar')).toBeInTheDocument();
    });

    it('fetches logs on mount', async () => {
      render(<LLMLogs />);

      await waitFor(() => {
        expect(mockGetLLMLogs).toHaveBeenCalledTimes(1);
      });

      expect(mockGetLLMLogs).toHaveBeenCalledWith({
        page: 0,
        per_page: 10,
        endpoint: undefined,
      });
    });

    it('displays logs after fetching', async () => {
      render(<LLMLogs />);

      await waitFor(() => {
        expect(screen.getByRole('button', { name: 'Create crew · gpt-4' })).toBeInTheDocument();
      });

      expect(screen.getByRole('region', { name: 'Model call details' })).toHaveTextContent('gpt-4');
      expect(screen.getByRole('list', { name: 'Model calls' })).toHaveTextContent('1,500 tokens');
    });

    it('displays title', async () => {
      render(<LLMLogs />);

      await waitFor(() => {
        expect(screen.getByRole('heading', { name: 'Model calls' })).toBeInTheDocument();
      });
    });
  });

  it('shows the selected request, response and metadata without opening another dialog', async () => {
    render(<LLMLogs />);
    expect(await screen.findByText('Test response 1')).toBeVisible();
    fireEvent.click(screen.getByRole('button', { name: 'Request', exact: true }));
    expect(screen.getByText('Test prompt 1')).toBeVisible();
    fireEvent.click(screen.getByRole('button', { name: 'Create agent · gpt-3.5-turbo' }));
    expect(screen.getByText('API rate limit exceeded')).toBeVisible();
    fireEvent.click(screen.getByRole('button', { name: 'Metadata', exact: true }));
    expect(screen.getByRole('region', { name: 'Model call details' })).toHaveTextContent('"key": "value"');
    expect(screen.queryByRole('dialog')).toBeNull();
  });

  describe('Refresh Button', () => {
    it('renders refresh button after loading', async () => {
      render(<LLMLogs />);

      await waitFor(() => {
        expect(screen.getByLabelText('Refresh')).toBeInTheDocument();
      });
    });

    it('fetches logs when refresh button is clicked', async () => {
      render(<LLMLogs />);

      await waitFor(() => {
        expect(mockGetLLMLogs).toHaveBeenCalledTimes(1);
      });

      const refreshButton = screen.getByLabelText('Refresh');
      fireEvent.click(refreshButton);

      await waitFor(() => {
        expect(mockGetLLMLogs).toHaveBeenCalledTimes(2);
      });
    });
  });

  describe('No Polling', () => {
    it('does not poll for logs - only fetches once on mount', async () => {
      vi.useFakeTimers({ shouldAdvanceTime: true });

      render(<LLMLogs />);

      // Wait for initial fetch
      await vi.waitFor(() => {
        expect(mockGetLLMLogs).toHaveBeenCalledTimes(1);
      });

      // Advance time by 60 seconds (would have polled twice with old 30s interval)
      await vi.advanceTimersByTimeAsync(60000);

      // Should still only have been called once (no polling)
      expect(mockGetLLMLogs).toHaveBeenCalledTimes(1);

      vi.useRealTimers();
    });
  });

  describe('Empty State', () => {
    it('displays no logs message when empty', async () => {
      mockGetLLMLogs.mockResolvedValue([]);

      render(<LLMLogs />);

      await waitFor(() => {
        expect(screen.getByText('No model calls yet')).toBeInTheDocument();
      });
    });
  });

  describe('Error Handling', () => {
    it('renders calls when the provider did not report token usage', async () => {
      mockGetLLMLogs.mockResolvedValue([{ ...mockLogs[0], tokens_used: null }]);
      render(<LLMLogs />);
      expect(await screen.findByText('Test response 1')).toBeVisible();
      expect(screen.getByRole('list', { name: 'Model calls' })).toHaveTextContent('Tokens unavailable');
    });

    it('handles fetch error gracefully', async () => {
      const consoleSpy = vi.spyOn(console, 'error').mockImplementation(() => {});
      mockGetLLMLogs.mockRejectedValue(new Error('Network error'));

      render(<LLMLogs />);

      await waitFor(() => {
        expect(consoleSpy).toHaveBeenCalledWith('Error fetching logs:', expect.any(Error));
      });

      consoleSpy.mockRestore();
    });
  });
});
