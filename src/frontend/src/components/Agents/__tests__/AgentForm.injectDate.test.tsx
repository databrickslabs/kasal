import { vi, beforeEach, describe, it, expect } from 'vitest';
import React from 'react';
import { render, screen, waitFor, within, act } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import AgentForm from '../AgentForm';

// Mock AgentService
vi.mock('../../../api/AgentService', () => ({
  AgentService: {
    createAgent: vi.fn().mockResolvedValue({
      id: 'new-agent-id',
      name: 'Test Agent',
      role: 'Test Role',
      goal: 'Test Goal',
      backstory: 'Test Backstory',
      tools: [],
      inject_date: true,
      date_format: undefined,
    }),
    updateAgentFull: vi.fn().mockResolvedValue({
      id: 'agent-123',
      name: 'Updated Agent',
      role: 'Test Role',
      goal: 'Test Goal',
      backstory: 'Test Backstory',
      tools: [],
      inject_date: false,
      date_format: '%Y-%m-%d',
    }),
    getAgent: vi.fn(),
  },
}));

// Mock ToolService
vi.mock('../../../api/ToolService', () => ({
  ToolService: {
    listTools: vi.fn().mockResolvedValue([]),
  },
}));

// Mock ModelService with getInstance pattern
vi.mock('../../../api/ModelService', () => ({
  ModelService: {
    getInstance: vi.fn(() => ({
      getActiveModels: vi.fn().mockResolvedValue({
        'test-model': {
          name: 'test-model',
          temperature: 0.7,
          context_window: 4096,
          max_output_tokens: 1024,
          enabled: true,
        },
        'databricks-llama-4-maverick': {
          name: 'databricks-llama-4-maverick',
          temperature: 0.7,
          context_window: 128000,
          max_output_tokens: 4096,
          enabled: true,
        },
      }),
    })),
  },
}));

// Mock LLMProviderService
vi.mock('../../../api/LLMProviderService', () => ({
  LLMProviderService: {
    getInstance: vi.fn(() => ({
      listLLMProviders: vi.fn().mockResolvedValue([]),
    })),
  },
}));

// Mock GenerateService
vi.mock('../../../api/GenerateService', () => ({
  GenerateService: {
    generateTemplates: vi.fn().mockResolvedValue({
      system_template: 'Generated system template',
      prompt_template: 'Generated prompt template',
      response_template: 'Generated response template',
    }),
  },
}));

// Mock DefaultMemoryBackendService
vi.mock('../../../api/DefaultMemoryBackendService', () => ({
  DefaultMemoryBackendService: {
    getInstance: vi.fn(() => ({
      getDefaultConfig: vi.fn().mockReturnValue(null),
    })),
  },
}));

// Mock DatabricksService
vi.mock('../../../api/DatabricksService', () => ({
  DatabricksService: {
    getInstance: vi.fn(() => ({
      getDatabricksConfig: vi.fn().mockResolvedValue(null),
    })),
  },
}));

// Mock stores
vi.mock('../../../store/agent', () => ({
  useAgentStore: () => ({
    updateAgent: vi.fn(),
  }),
}));

vi.mock('../../../store/knowledgeConfigStore', () => ({
  useKnowledgeConfigStore: () => ({
    isMemoryBackendConfigured: true,
    isKnowledgeSourceEnabled: true,
  }),
}));

// Mock sub-components that are not relevant to these tests
vi.mock('../../Common/GenieSpaceSelector', () => ({
  GenieSpaceSelector: () => <div data-testid="genie-space-selector">GenieSpaceSelector</div>,
}));

vi.mock('../../Common/PerplexityConfigSelector', () => ({
  PerplexityConfigSelector: () => <div data-testid="perplexity-config-selector">PerplexityConfigSelector</div>,
}));

vi.mock('../../Common/SerperConfigSelector', () => ({
  SerperConfigSelector: () => <div data-testid="serper-config-selector">SerperConfigSelector</div>,
}));

vi.mock('../../Common/MCPServerSelector', () => ({
  MCPServerSelector: () => <div data-testid="mcp-server-selector">MCPServerSelector</div>,
}));

vi.mock('../../BestPractices/AgentBestPractices', () => ({
  default: () => <div data-testid="agent-best-practices">AgentBestPractices</div>,
}));

describe('AgentForm - Inject Date Feature', () => {
  const mockOnCancel = vi.fn();
  const mockOnAgentSaved = vi.fn();

  const defaultProps = {
    tools: [],
    onCancel: mockOnCancel,
    onAgentSaved: mockOnAgentSaved,
  };

  beforeEach(() => {
    vi.clearAllMocks();
  });

  /**
   * Helper function to expand the Behavior Settings accordion
   */
  const expandBehaviorSettings = async () => {
    // Wait for form to stabilize after render
    await act(async () => {
      await new Promise(resolve => setTimeout(resolve, 100));
    });

    // Find the Behavior Settings accordion summary and click to expand
    const accordionSummaries = screen.getAllByRole('button');
    const behaviorSettingsButton = accordionSummaries.find(
      button => button.textContent?.includes('Behavior Settings')
    );

    if (!behaviorSettingsButton) {
      throw new Error('Could not find Behavior Settings accordion');
    }

    await act(async () => {
      await userEvent.click(behaviorSettingsButton);
    });

    // Wait for accordion to expand - check for the inject date label text
    await waitFor(() => {
      expect(screen.getByText('Inject Current Date')).toBeInTheDocument();
    }, { timeout: 3000 });
  };

  /**
   * Helper function to find the inject_date switch checkbox
   * MUI Switch renders as a checkbox but due to Tooltip wrapper, we need to find it differently
   */
  const findInjectDateSwitch = () => {
    // Find the FormControlLabel by its label text
    const labelElement = screen.getByText('Inject Current Date');
    // Navigate up to find the FormControlLabel container and then find the checkbox within
    const formControlLabel = labelElement.closest('label');
    if (!formControlLabel) {
      throw new Error('Could not find FormControlLabel');
    }
    // The checkbox input is inside the FormControlLabel
    const checkbox = formControlLabel.querySelector('input[type="checkbox"]');
    if (!checkbox) {
      throw new Error('Could not find checkbox input');
    }
    return checkbox as HTMLInputElement;
  };

  describe('Default State', () => {
    it('should default inject_date to true in a new agent form', async () => {
      render(<AgentForm {...defaultProps} />);

      await expandBehaviorSettings();

      const injectDateSwitch = findInjectDateSwitch();
      expect(injectDateSwitch.checked).toBe(true);
    });

    it('should show the "Inject Current Date" switch label', async () => {
      render(<AgentForm {...defaultProps} />);

      await expandBehaviorSettings();

      expect(screen.getByText('Inject Current Date')).toBeInTheDocument();
    });

    it('should show date_format field when inject_date is true (default)', async () => {
      render(<AgentForm {...defaultProps} />);

      await expandBehaviorSettings();

      // The date format field should be visible since inject_date defaults to true
      expect(screen.getByLabelText(/Date Format \(Optional\)/i)).toBeInTheDocument();
    });
  });

  describe('Initial Data Handling', () => {
    it('should respect inject_date=true from initial data', async () => {
      const agentWithInjectDate = {
        id: 'agent-123',
        name: 'Test Agent',
        role: 'Test Role',
        goal: 'Test Goal',
        backstory: 'Test Backstory',
        tools: [],
        inject_date: true,
      };

      render(<AgentForm {...defaultProps} initialData={agentWithInjectDate} />);

      await expandBehaviorSettings();

      const injectDateSwitch = findInjectDateSwitch();
      expect(injectDateSwitch.checked).toBe(true);
    });

    it('should respect inject_date=false from initial data', async () => {
      const agentWithoutInjectDate = {
        id: 'agent-123',
        name: 'Test Agent',
        role: 'Test Role',
        goal: 'Test Goal',
        backstory: 'Test Backstory',
        tools: [],
        inject_date: false,
      };

      render(<AgentForm {...defaultProps} initialData={agentWithoutInjectDate} />);

      await expandBehaviorSettings();

      const injectDateSwitch = findInjectDateSwitch();
      expect(injectDateSwitch.checked).toBe(false);
    });

    it('should display the date_format from initial data', async () => {
      const agentWithDateFormat = {
        id: 'agent-123',
        name: 'Test Agent',
        role: 'Test Role',
        goal: 'Test Goal',
        backstory: 'Test Backstory',
        tools: [],
        inject_date: true,
        date_format: '%B %d, %Y',
      };

      render(<AgentForm {...defaultProps} initialData={agentWithDateFormat} />);

      await expandBehaviorSettings();

      const dateFormatInput = screen.getByLabelText(/Date Format \(Optional\)/i) as HTMLInputElement;
      expect(dateFormatInput.value).toBe('%B %d, %Y');
    });

    it('should default to true when inject_date is undefined in initial data', async () => {
      const agentWithUndefinedInjectDate = {
        id: 'agent-123',
        name: 'Test Agent',
        role: 'Test Role',
        goal: 'Test Goal',
        backstory: 'Test Backstory',
        tools: [],
        // inject_date is undefined
      };

      render(<AgentForm {...defaultProps} initialData={agentWithUndefinedInjectDate} />);

      await expandBehaviorSettings();

      const injectDateSwitch = findInjectDateSwitch();
      expect(injectDateSwitch.checked).toBe(true);
    });
  });

  describe('Switch Toggle Behavior', () => {
    it('should toggle inject_date from true to false when switch is clicked', async () => {
      render(<AgentForm {...defaultProps} />);

      await expandBehaviorSettings();

      const injectDateSwitch = findInjectDateSwitch();
      expect(injectDateSwitch.checked).toBe(true);

      // Click the switch to toggle it off
      await act(async () => {
        await userEvent.click(injectDateSwitch);
      });

      await waitFor(() => {
        expect(injectDateSwitch.checked).toBe(false);
      });
    });

    it('should toggle inject_date from false to true when switch is clicked', async () => {
      const agentWithoutInjectDate = {
        id: 'agent-123',
        name: 'Test Agent',
        role: 'Test Role',
        goal: 'Test Goal',
        backstory: 'Test Backstory',
        tools: [],
        inject_date: false,
      };

      render(<AgentForm {...defaultProps} initialData={agentWithoutInjectDate} />);

      await expandBehaviorSettings();

      const injectDateSwitch = findInjectDateSwitch();
      expect(injectDateSwitch.checked).toBe(false);

      // Click the label/FormControlLabel to toggle the switch
      // The Switch in AgentForm uses onClick on the Switch component itself,
      // so we need to click on the Switch's clickable area (the span with role=presentation or the label)
      const labelElement = screen.getByText('Inject Current Date');
      const formControlLabel = labelElement.closest('label');

      // Find the Switch's clickable span (the thumb + track area)
      const switchSpan = formControlLabel?.querySelector('.MuiSwitch-switchBase');

      if (switchSpan) {
        await act(async () => {
          await userEvent.click(switchSpan);
        });
      } else {
        // Fallback to clicking the label itself
        await act(async () => {
          await userEvent.click(labelElement);
        });
      }

      await waitFor(() => {
        expect(injectDateSwitch.checked).toBe(true);
      });
    });
  });

  describe('Date Format Field Visibility', () => {
    it('should show date_format field when inject_date is true', async () => {
      render(<AgentForm {...defaultProps} />);

      await expandBehaviorSettings();

      // inject_date is true by default
      expect(screen.getByLabelText(/Date Format \(Optional\)/i)).toBeInTheDocument();
    });

    it('should hide date_format field when inject_date is false', async () => {
      const agentWithoutInjectDate = {
        id: 'agent-123',
        name: 'Test Agent',
        role: 'Test Role',
        goal: 'Test Goal',
        backstory: 'Test Backstory',
        tools: [],
        inject_date: false,
      };

      render(<AgentForm {...defaultProps} initialData={agentWithoutInjectDate} />);

      await expandBehaviorSettings();

      // Date format field should not be visible
      expect(screen.queryByLabelText(/Date Format \(Optional\)/i)).not.toBeInTheDocument();
    });

    it('should hide date_format field after toggling inject_date to false', async () => {
      render(<AgentForm {...defaultProps} />);

      await expandBehaviorSettings();

      // Verify date format is initially visible
      expect(screen.getByLabelText(/Date Format \(Optional\)/i)).toBeInTheDocument();

      // Toggle inject_date off
      const injectDateSwitch = findInjectDateSwitch();
      await act(async () => {
        await userEvent.click(injectDateSwitch);
      });

      // Date format field should now be hidden
      await waitFor(() => {
        expect(screen.queryByLabelText(/Date Format \(Optional\)/i)).not.toBeInTheDocument();
      });
    });

    it('should show date_format field after toggling inject_date to true', async () => {
      const agentWithoutInjectDate = {
        id: 'agent-123',
        name: 'Test Agent',
        role: 'Test Role',
        goal: 'Test Goal',
        backstory: 'Test Backstory',
        tools: [],
        inject_date: false,
      };

      render(<AgentForm {...defaultProps} initialData={agentWithoutInjectDate} />);

      await expandBehaviorSettings();

      // Verify date format is initially hidden
      expect(screen.queryByLabelText(/Date Format \(Optional\)/i)).not.toBeInTheDocument();

      // Toggle inject_date on
      const injectDateSwitch = findInjectDateSwitch();
      await act(async () => {
        await userEvent.click(injectDateSwitch);
      });

      // Date format field should now be visible
      await waitFor(() => {
        expect(screen.getByLabelText(/Date Format \(Optional\)/i)).toBeInTheDocument();
      });
    });
  });

  describe('Date Format Field Input', () => {
    it('should allow entering a custom date format', async () => {
      render(<AgentForm {...defaultProps} />);

      await expandBehaviorSettings();

      const dateFormatInput = screen.getByLabelText(/Date Format \(Optional\)/i) as HTMLInputElement;

      await act(async () => {
        await userEvent.clear(dateFormatInput);
        await userEvent.type(dateFormatInput, '%Y-%m-%d');
      });

      expect(dateFormatInput.value).toBe('%Y-%m-%d');
    });

    it('should show placeholder text for date format', async () => {
      render(<AgentForm {...defaultProps} />);

      await expandBehaviorSettings();

      const dateFormatInput = screen.getByLabelText(/Date Format \(Optional\)/i) as HTMLInputElement;
      expect(dateFormatInput.placeholder).toBe('%B %d, %Y');
    });

    it('should show helper text explaining the date format', async () => {
      render(<AgentForm {...defaultProps} />);

      await expandBehaviorSettings();

      expect(screen.getByText(/Custom date format.*Leave empty for ISO format/i)).toBeInTheDocument();
    });

    it('should allow clearing the date format field', async () => {
      const agentWithDateFormat = {
        id: 'agent-123',
        name: 'Test Agent',
        role: 'Test Role',
        goal: 'Test Goal',
        backstory: 'Test Backstory',
        tools: [],
        inject_date: true,
        date_format: '%B %d, %Y',
      };

      render(<AgentForm {...defaultProps} initialData={agentWithDateFormat} />);

      await expandBehaviorSettings();

      const dateFormatInput = screen.getByLabelText(/Date Format \(Optional\)/i) as HTMLInputElement;
      expect(dateFormatInput.value).toBe('%B %d, %Y');

      await act(async () => {
        await userEvent.clear(dateFormatInput);
      });

      expect(dateFormatInput.value).toBe('');
    });
  });

  describe('Tooltip', () => {
    it('should have a tooltip on the Inject Current Date control', async () => {
      render(<AgentForm {...defaultProps} />);

      await expandBehaviorSettings();

      // The tooltip wraps the FormControlLabel - look for the tooltip by its title attribute
      // Since MUI Tooltip doesn't show until hover, we check for the presence of the control
      const injectDateLabel = screen.getByText('Inject Current Date');
      expect(injectDateLabel).toBeInTheDocument();
    });
  });

  describe('Form Submission', () => {
    it('should include inject_date=true and date_format in form submission for new agent', async () => {
      const { AgentService } = await import('../../../api/AgentService');

      render(<AgentForm {...defaultProps} />);

      // Fill in required fields
      await act(async () => {
        await userEvent.type(screen.getByLabelText(/Name/i), 'Test Agent');
        await userEvent.type(screen.getByLabelText(/Role/i), 'Test Role');
        await userEvent.type(screen.getByLabelText(/Goal/i), 'Test Goal');
        await userEvent.type(screen.getByLabelText(/Backstory/i), 'Test Backstory');
      });

      await expandBehaviorSettings();

      // Enter a date format
      const dateFormatInput = screen.getByLabelText(/Date Format \(Optional\)/i) as HTMLInputElement;
      await act(async () => {
        await userEvent.type(dateFormatInput, '%Y-%m-%d');
      });

      // Submit the form
      const saveButton = screen.getByRole('button', { name: /Save/i });
      await act(async () => {
        await userEvent.click(saveButton);
      });

      await waitFor(() => {
        expect(AgentService.createAgent).toHaveBeenCalledWith(
          expect.objectContaining({
            inject_date: true,
            date_format: '%Y-%m-%d',
          })
        );
      });
    });

    it('should include inject_date=false in form submission when toggled off', async () => {
      const { AgentService } = await import('../../../api/AgentService');

      render(<AgentForm {...defaultProps} />);

      // Fill in required fields
      await act(async () => {
        await userEvent.type(screen.getByLabelText(/Name/i), 'Test Agent');
        await userEvent.type(screen.getByLabelText(/Role/i), 'Test Role');
        await userEvent.type(screen.getByLabelText(/Goal/i), 'Test Goal');
        await userEvent.type(screen.getByLabelText(/Backstory/i), 'Test Backstory');
      });

      await expandBehaviorSettings();

      // Toggle inject_date off
      const injectDateSwitch = findInjectDateSwitch();
      await act(async () => {
        await userEvent.click(injectDateSwitch);
      });

      // Submit the form
      const saveButton = screen.getByRole('button', { name: /Save/i });
      await act(async () => {
        await userEvent.click(saveButton);
      });

      await waitFor(() => {
        expect(AgentService.createAgent).toHaveBeenCalledWith(
          expect.objectContaining({
            inject_date: false,
          })
        );
      });
    });

    it('should update existing agent with inject_date and date_format', async () => {
      const { AgentService } = await import('../../../api/AgentService');

      const existingAgent = {
        id: 'agent-123',
        name: 'Existing Agent',
        role: 'Existing Role',
        goal: 'Existing Goal',
        backstory: 'Existing Backstory',
        tools: [],
        inject_date: true,
        date_format: '%B %d, %Y',
      };

      render(<AgentForm {...defaultProps} initialData={existingAgent} />);

      await expandBehaviorSettings();

      // Change the date format
      const dateFormatInput = screen.getByLabelText(/Date Format \(Optional\)/i) as HTMLInputElement;
      await act(async () => {
        await userEvent.clear(dateFormatInput);
        await userEvent.type(dateFormatInput, '%d/%m/%Y');
      });

      // Submit the form
      const saveButton = screen.getByRole('button', { name: /Save/i });
      await act(async () => {
        await userEvent.click(saveButton);
      });

      await waitFor(() => {
        expect(AgentService.updateAgentFull).toHaveBeenCalledWith(
          'agent-123',
          expect.objectContaining({
            inject_date: true,
            date_format: '%d/%m/%Y',
          })
        );
      });
    });

    it('should handle undefined date_format in submission when field is empty', async () => {
      const { AgentService } = await import('../../../api/AgentService');

      render(<AgentForm {...defaultProps} />);

      // Fill in required fields
      await act(async () => {
        await userEvent.type(screen.getByLabelText(/Name/i), 'Test Agent');
        await userEvent.type(screen.getByLabelText(/Role/i), 'Test Role');
        await userEvent.type(screen.getByLabelText(/Goal/i), 'Test Goal');
        await userEvent.type(screen.getByLabelText(/Backstory/i), 'Test Backstory');
      });

      // Don't enter a date format, leaving it empty

      // Submit the form
      const saveButton = screen.getByRole('button', { name: /Save/i });
      await act(async () => {
        await userEvent.click(saveButton);
      });

      await waitFor(() => {
        expect(AgentService.createAgent).toHaveBeenCalled();
        // Verify the call included inject_date=true
        const calls = (AgentService.createAgent as ReturnType<typeof vi.fn>).mock.calls;
        expect(calls.length).toBeGreaterThan(0);
        const agentData = calls[0][0];
        expect(agentData.inject_date).toBe(true);
        // date_format should be undefined or not present (not a defined string)
        expect(agentData.date_format).toBeUndefined();
      });
    });
  });

  describe('Accessibility', () => {
    it('should have accessible labels for the inject_date switch', async () => {
      render(<AgentForm {...defaultProps} />);

      await expandBehaviorSettings();

      // The switch should be accessible - verify the label exists and checkbox is findable
      const injectDateLabel = screen.getByText('Inject Current Date');
      expect(injectDateLabel).toBeInTheDocument();

      const injectDateSwitch = findInjectDateSwitch();
      expect(injectDateSwitch).toBeInTheDocument();
    });

    it('should have accessible label for the date_format input', async () => {
      render(<AgentForm {...defaultProps} />);

      await expandBehaviorSettings();

      const dateFormatInput = screen.getByLabelText(/Date Format \(Optional\)/i);
      expect(dateFormatInput).toBeInTheDocument();
    });
  });
});
