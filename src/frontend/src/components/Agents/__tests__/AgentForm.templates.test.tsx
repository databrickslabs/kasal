/**
 * Unit tests for AgentForm template functionality.
 * 
 * Tests the template generation features, expandable text areas,
 * and helper text functionality added to the AgentForm component.
 */
import React from 'react';
import { render, screen, fireEvent, waitFor } from '@testing-library/react';
import { act } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { ThemeProvider } from '@mui/material/styles';
import { createTheme } from '@mui/material/styles';
import AgentForm from '../AgentForm';
import { GenerateService } from '../../../api/GenerateService';

// Mock the GenerateService
jest.mock('../../../api/GenerateService');
const mockGenerateService = GenerateService as jest.Mocked<typeof GenerateService>;

// Mock theme
const theme = createTheme();

// Mock props
const mockProps = {
  tools: [
    { id: 1, title: 'Test Tool', description: 'A test tool', enabled: true },
    { id: 2, title: 'Another Tool', description: 'Another test tool', enabled: true }
  ],
  onCancel: jest.fn(),
  onAgentSaved: jest.fn()
};

const MockedAgentForm = (props: any) => (
  <ThemeProvider theme={theme}>
    <AgentForm {...mockProps} {...props} />
  </ThemeProvider>
);

describe('AgentForm Template Features', () => {
  beforeEach(() => {
    jest.clearAllMocks();
  });

  describe('Template Fields Rendering', () => {
    it('should render all three template fields with labels', () => {
      render(<MockedAgentForm />);

      expect(screen.getByLabelText('System Template')).toBeInTheDocument();
      expect(screen.getByLabelText('Prompt Template')).toBeInTheDocument();
      expect(screen.getByLabelText('Response Template')).toBeInTheDocument();
    });

    it('should render helper text for each template field', () => {
      render(<MockedAgentForm />);

      // Check for system template helper text
      expect(screen.getByText(/Defines the agent's core identity/)).toBeInTheDocument();
      expect(screen.getByText(/role.*goal.*backstory/)).toBeInTheDocument();

      // Check for prompt template helper text
      expect(screen.getByText(/Structures how tasks are presented/)).toBeInTheDocument();
      expect(screen.getByText(/input.*context/)).toBeInTheDocument();

      // Check for response template helper text
      expect(screen.getByText(/Guides how the agent formats its responses/)).toBeInTheDocument();
      expect(screen.getByText(/THOUGHTS.*ACTION.*RESULT/)).toBeInTheDocument();
    });

    it('should render expand buttons for each template field', () => {
      render(<MockedAgentForm />);

      const expandButtons = screen.getAllByTitle(/Expand.*template/);
      expect(expandButtons).toHaveLength(3);

      expect(screen.getByTitle('Expand system template')).toBeInTheDocument();
      expect(screen.getByTitle('Expand prompt template')).toBeInTheDocument();
      expect(screen.getByTitle('Expand response template')).toBeInTheDocument();
    });
  });

  describe('Template Field Interactions', () => {
    it('should allow typing in template fields', async () => {
      const user = userEvent.setup();
      render(<MockedAgentForm />);

      const systemTemplateField = screen.getByLabelText('System Template');
      await user.type(systemTemplateField, 'You are a {role} expert.');

      expect(systemTemplateField).toHaveValue('You are a {role} expert.');
    });

    it('should open system template dialog when expand button is clicked', async () => {
      const user = userEvent.setup();
      render(<MockedAgentForm />);

      const expandButton = screen.getByTitle('Expand system template');
      await user.click(expandButton);

      await waitFor(() => {
        expect(screen.getByText('System Template')).toBeInTheDocument();
        expect(screen.getByRole('dialog')).toBeInTheDocument();
      });
    });

    it('should open prompt template dialog when expand button is clicked', async () => {
      const user = userEvent.setup();
      render(<MockedAgentForm />);

      const expandButton = screen.getByTitle('Expand prompt template');
      await user.click(expandButton);

      await waitFor(() => {
        expect(screen.getByText('Prompt Template')).toBeInTheDocument();
        expect(screen.getByRole('dialog')).toBeInTheDocument();
      });
    });

    it('should open response template dialog when expand button is clicked', async () => {
      const user = userEvent.setup();
      render(<MockedAgentForm />);

      const expandButton = screen.getByTitle('Expand response template');
      await user.click(expandButton);

      await waitFor(() => {
        expect(screen.getByText('Response Template')).toBeInTheDocument();
        expect(screen.getByRole('dialog')).toBeInTheDocument();
      });
    });
  });

  describe('Template Dialogs', () => {
    it('should close system template dialog when close button is clicked', async () => {
      const user = userEvent.setup();
      render(<MockedAgentForm />);

      // Open dialog
      const expandButton = screen.getByTitle('Expand system template');
      await user.click(expandButton);

      await waitFor(() => {
        expect(screen.getByRole('dialog')).toBeInTheDocument();
      });

      // Close dialog
      const closeButton = screen.getByRole('button', { name: /close/i });
      await user.click(closeButton);

      await waitFor(() => {
        expect(screen.queryByRole('dialog')).not.toBeInTheDocument();
      });
    });

    it('should close dialog when Done button is clicked', async () => {
      const user = userEvent.setup();
      render(<MockedAgentForm />);

      // Open dialog
      const expandButton = screen.getByTitle('Expand system template');
      await user.click(expandButton);

      await waitFor(() => {
        expect(screen.getByRole('dialog')).toBeInTheDocument();
      });

      // Close dialog with Done button
      const doneButton = screen.getByRole('button', { name: /done/i });
      await user.click(doneButton);

      await waitFor(() => {
        expect(screen.queryByRole('dialog')).not.toBeInTheDocument();
      });
    });

    it('should sync content between field and dialog', async () => {
      const user = userEvent.setup();
      render(<MockedAgentForm />);

      // Type in the main field
      const systemTemplateField = screen.getByLabelText('System Template');
      await user.type(systemTemplateField, 'Test content');

      // Open dialog
      const expandButton = screen.getByTitle('Expand system template');
      await user.click(expandButton);

      await waitFor(() => {
        const dialogField = screen.getByDisplayValue('Test content');
        expect(dialogField).toBeInTheDocument();
      });
    });

    it('should have larger text area in dialog (15 rows)', async () => {
      const user = userEvent.setup();
      render(<MockedAgentForm />);

      // Open dialog
      const expandButton = screen.getByTitle('Expand system template');
      await user.click(expandButton);

      await waitFor(() => {
        const dialogTextarea = screen.getByRole('dialog').querySelector('textarea');
        expect(dialogTextarea).toHaveAttribute('rows', '15');
      });
    });
  });

  describe('Template Generation', () => {
    it('should render Generate Templates button', () => {
      render(<MockedAgentForm />);

      expect(screen.getByRole('button', { name: /generate templates/i })).toBeInTheDocument();
    });

    it('should disable Generate Templates button when required fields are missing', () => {
      render(<MockedAgentForm />);

      const generateButton = screen.getByRole('button', { name: /generate templates/i });
      expect(generateButton).toBeDisabled();
    });

    it('should enable Generate Templates button when role, goal, and backstory are filled', async () => {
      const user = userEvent.setup();
      render(<MockedAgentForm />);

      // Fill required fields
      await user.type(screen.getByLabelText('Role'), 'Data Analyst');
      await user.type(screen.getByLabelText('Goal'), 'Analyze data effectively');
      await user.type(screen.getByLabelText('Backstory'), 'Expert in data analysis');

      const generateButton = screen.getByRole('button', { name: /generate templates/i });
      expect(generateButton).toBeEnabled();
    });

    it('should show loading state when generating templates', async () => {
      const user = userEvent.setup();
      
      // Mock the service to return a promise that doesn't resolve immediately
      mockGenerateService.generateTemplates.mockImplementation(() => 
        new Promise(resolve => setTimeout(resolve, 100))
      );

      render(<MockedAgentForm />);

      // Fill required fields
      await user.type(screen.getByLabelText('Role'), 'Data Analyst');
      await user.type(screen.getByLabelText('Goal'), 'Analyze data effectively');
      await user.type(screen.getByLabelText('Backstory'), 'Expert in data analysis');

      // Click generate button
      const generateButton = screen.getByRole('button', { name: /generate templates/i });
      await user.click(generateButton);

      // Should show loading state
      expect(screen.getByText('Generating...')).toBeInTheDocument();
      expect(generateButton).toBeDisabled();
    });

    it('should call GenerateService with correct parameters', async () => {
      const user = userEvent.setup();
      
      mockGenerateService.generateTemplates.mockResolvedValue({
        system_template: 'Generated system template',
        prompt_template: 'Generated prompt template',
        response_template: 'Generated response template'
      });

      render(<MockedAgentForm />);

      // Fill required fields
      await user.type(screen.getByLabelText('Role'), 'Data Analyst');
      await user.type(screen.getByLabelText('Goal'), 'Analyze data effectively');
      await user.type(screen.getByLabelText('Backstory'), 'Expert in data analysis');

      // Click generate button
      const generateButton = screen.getByRole('button', { name: /generate templates/i });
      await user.click(generateButton);

      await waitFor(() => {
        expect(mockGenerateService.generateTemplates).toHaveBeenCalledWith({
          role: 'Data Analyst',
          goal: 'Analyze data effectively',
          backstory: 'Expert in data analysis',
          model: 'databricks-llama-4-maverick'
        });
      });
    });

    it('should populate template fields after successful generation', async () => {
      const user = userEvent.setup();
      
      mockGenerateService.generateTemplates.mockResolvedValue({
        system_template: 'Generated system template',
        prompt_template: 'Generated prompt template',
        response_template: 'Generated response template'
      });

      render(<MockedAgentForm />);

      // Fill required fields
      await user.type(screen.getByLabelText('Role'), 'Data Analyst');
      await user.type(screen.getByLabelText('Goal'), 'Analyze data effectively');
      await user.type(screen.getByLabelText('Backstory'), 'Expert in data analysis');

      // Click generate button
      const generateButton = screen.getByRole('button', { name: /generate templates/i });
      await user.click(generateButton);

      await waitFor(() => {
        expect(screen.getByDisplayValue('Generated system template')).toBeInTheDocument();
        expect(screen.getByDisplayValue('Generated prompt template')).toBeInTheDocument();
        expect(screen.getByDisplayValue('Generated response template')).toBeInTheDocument();
      });
    });
  });

  describe('Template Field Validation', () => {
    it('should preserve existing template values when editing', async () => {
      const user = userEvent.setup();
      const initialData = {
        id: '1',
        name: 'Test Agent',
        role: 'Analyst',
        goal: 'Test goal',
        backstory: 'Test backstory',
        system_template: 'Existing system template',
        prompt_template: 'Existing prompt template',
        response_template: 'Existing response template'
      };

      render(<MockedAgentForm initialData={initialData} />);

      expect(screen.getByDisplayValue('Existing system template')).toBeInTheDocument();
      expect(screen.getByDisplayValue('Existing prompt template')).toBeInTheDocument();
      expect(screen.getByDisplayValue('Existing response template')).toBeInTheDocument();
    });

    it('should handle empty template values gracefully', () => {
      const initialData = {
        id: '1',
        name: 'Test Agent',
        role: 'Analyst',
        goal: 'Test goal',
        backstory: 'Test backstory',
        system_template: '',
        prompt_template: null,
        response_template: undefined
      };

      render(<MockedAgentForm initialData={initialData} />);

      // Should not throw errors and fields should be empty
      const systemField = screen.getByLabelText('System Template');
      const promptField = screen.getByLabelText('Prompt Template');
      const responseField = screen.getByLabelText('Response Template');

      expect(systemField).toHaveValue('');
      expect(promptField).toHaveValue('');
      expect(responseField).toHaveValue('');
    });
  });

  describe('Accessibility', () => {
    it('should have proper ARIA labels for template fields', () => {
      render(<MockedAgentForm />);

      expect(screen.getByLabelText('System Template')).toBeInTheDocument();
      expect(screen.getByLabelText('Prompt Template')).toBeInTheDocument();
      expect(screen.getByLabelText('Response Template')).toBeInTheDocument();
    });

    it('should have proper button titles for expand buttons', () => {
      render(<MockedAgentForm />);

      expect(screen.getByTitle('Expand system template')).toBeInTheDocument();
      expect(screen.getByTitle('Expand prompt template')).toBeInTheDocument();
      expect(screen.getByTitle('Expand response template')).toBeInTheDocument();
    });

    it('should have proper dialog titles', async () => {
      const user = userEvent.setup();
      render(<MockedAgentForm />);

      // Test each dialog title
      const dialogs = [
        { button: 'Expand system template', title: 'System Template' },
        { button: 'Expand prompt template', title: 'Prompt Template' },
        { button: 'Expand response template', title: 'Response Template' }
      ];

      for (const dialog of dialogs) {
        const expandButton = screen.getByTitle(dialog.button);
        await user.click(expandButton);

        await waitFor(() => {
          expect(screen.getByText(dialog.title)).toBeInTheDocument();
        });

        // Close dialog
        const closeButton = screen.getByRole('button', { name: /close/i });
        await user.click(closeButton);

        await waitFor(() => {
          expect(screen.queryByRole('dialog')).not.toBeInTheDocument();
        });
      }
    });
  });
});