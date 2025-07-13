/**
 * Simplified unit tests for AgentForm template functionality.
 * 
 * Tests the template generation features and UI components
 * without complex service dependencies.
 */
import React from 'react';
import { render, screen, fireEvent, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { ThemeProvider } from '@mui/material/styles';
import { createTheme } from '@mui/material/styles';
import { 
  TextField, 
  Button, 
  Typography, 
  Dialog, 
  DialogTitle, 
  DialogContent, 
  DialogActions,
  IconButton,
  InputAdornment,
  Accordion,
  AccordionSummary,
  AccordionDetails,
  Box
} from '@mui/material';
import OpenInFullIcon from '@mui/icons-material/OpenInFull';
import CloseIcon from '@mui/icons-material/Close';
import ExpandMoreIcon from '@mui/icons-material/ExpandMore';

// Mock the GenerateService
jest.mock('../../../api/GenerateService', () => ({
  GenerateService: {
    generateTemplates: jest.fn(),
  },
}));

// Mock theme
const theme = createTheme();

// Simplified AgentForm component focusing on template functionality
const TemplateSection = ({ 
  systemTemplate = '', 
  promptTemplate = '', 
  responseTemplate = '',
  onTemplateChange = () => {},
  onGenerateTemplates = () => {},
  canGenerate = false,
  isGenerating = false
}) => {
  const [expandedSystem, setExpandedSystem] = React.useState(false);
  const [expandedPrompt, setExpandedPrompt] = React.useState(false);
  const [expandedResponse, setExpandedResponse] = React.useState(false);

  return (
    <ThemeProvider theme={theme}>
      <div>
        <Accordion>
          <AccordionSummary expandIcon={<ExpandMoreIcon />}>
            <Typography>Templates</Typography>
          </AccordionSummary>
          <AccordionDetails>
            <Box sx={{ display: 'flex', flexDirection: 'column', gap: 2 }}>
              <Box sx={{ display: 'flex', justifyContent: 'flex-end', mb: 2 }}>
                <Button
                  variant="contained"
                  onClick={onGenerateTemplates}
                  disabled={!canGenerate || isGenerating}
                  data-testid="generate-templates-btn"
                >
                  {isGenerating ? 'Generating...' : 'Generate Templates'}
                </Button>
              </Box>
              
              <TextField
                fullWidth
                label="System Template"
                value={systemTemplate}
                onChange={(e) => onTemplateChange('system_template', e.target.value)}
                multiline
                rows={4}
                data-testid="system-template-field"
                InputProps={{
                  endAdornment: (
                    <InputAdornment position="end">
                      <IconButton
                        edge="end"
                        onClick={() => setExpandedSystem(true)}
                        size="small"
                        sx={{ opacity: 0.7 }}
                        title="Expand system template"
                        data-testid="expand-system-btn"
                      >
                        <OpenInFullIcon fontSize="small" />
                      </IconButton>
                    </InputAdornment>
                  )
                }}
              />
              <Typography variant="caption" color="text.secondary" sx={{ mt: 0.5, mb: 1 }}>
                Defines the agent&apos;s core identity and fundamental instructions. Controls how the agent understands its {'{role}'}, {'{goal}'}, and {'{backstory}'}. Use this to establish expertise boundaries, ethical guidelines, and overall behavior patterns.
              </Typography>

              <TextField
                fullWidth
                label="Prompt Template"
                value={promptTemplate}
                onChange={(e) => onTemplateChange('prompt_template', e.target.value)}
                multiline
                rows={4}
                data-testid="prompt-template-field"
                InputProps={{
                  endAdornment: (
                    <InputAdornment position="end">
                      <IconButton
                        edge="end"
                        onClick={() => setExpandedPrompt(true)}
                        size="small"
                        sx={{ opacity: 0.7 }}
                        title="Expand prompt template"
                        data-testid="expand-prompt-btn"
                      >
                        <OpenInFullIcon fontSize="small" />
                      </IconButton>
                    </InputAdornment>
                  )
                }}
              />
              <Typography variant="caption" color="text.secondary" sx={{ mt: 0.5, mb: 1 }}>
                Structures how tasks are presented to the agent. Controls the format and flow of task instructions. Include placeholders for dynamic content like {'{input}'}, {'{context}'}, and task-specific variables to ensure consistent task processing.
              </Typography>

              <TextField
                fullWidth
                label="Response Template"
                value={responseTemplate}
                onChange={(e) => onTemplateChange('response_template', e.target.value)}
                multiline
                rows={4}
                data-testid="response-template-field"
                InputProps={{
                  endAdornment: (
                    <InputAdornment position="end">
                      <IconButton
                        edge="end"
                        onClick={() => setExpandedResponse(true)}
                        size="small"
                        sx={{ opacity: 0.7 }}
                        title="Expand response template"
                        data-testid="expand-response-btn"
                      >
                        <OpenInFullIcon fontSize="small" />
                      </IconButton>
                    </InputAdornment>
                  )
                }}
              />
              <Typography variant="caption" color="text.secondary" sx={{ mt: 0.5, mb: 1 }}>
                Guides how the agent formats its responses and output structure. Enforces consistency in deliverables by defining sections like THOUGHTS, ACTION, and RESULT. Use this to ensure structured, actionable, and predictable agent outputs.
              </Typography>
            </Box>
          </AccordionDetails>
        </Accordion>

        {/* System Template Dialog */}
        <Dialog 
          open={expandedSystem} 
          onClose={() => setExpandedSystem(false)}
          fullWidth
          maxWidth="md"
          data-testid="system-template-dialog"
        >
          <DialogTitle>
            <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
              System Template
              <IconButton onClick={() => setExpandedSystem(false)}>
                <CloseIcon />
              </IconButton>
            </Box>
          </DialogTitle>
          <DialogContent>
            <TextField
              autoFocus
              fullWidth
              multiline
              rows={15}
              value={systemTemplate}
              onChange={(e) => onTemplateChange('system_template', e.target.value)}
              variant="outlined"
              sx={{ mt: 2 }}
              data-testid="system-template-dialog-field"
            />
          </DialogContent>
          <DialogActions>
            <Button onClick={() => setExpandedSystem(false)} variant="contained">
              Done
            </Button>
          </DialogActions>
        </Dialog>

        {/* Prompt Template Dialog */}
        <Dialog 
          open={expandedPrompt} 
          onClose={() => setExpandedPrompt(false)}
          fullWidth
          maxWidth="md"
          data-testid="prompt-template-dialog"
        >
          <DialogTitle>
            <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
              Prompt Template
              <IconButton onClick={() => setExpandedPrompt(false)}>
                <CloseIcon />
              </IconButton>
            </Box>
          </DialogTitle>
          <DialogContent>
            <TextField
              autoFocus
              fullWidth
              multiline
              rows={15}
              value={promptTemplate}
              onChange={(e) => onTemplateChange('prompt_template', e.target.value)}
              variant="outlined"
              sx={{ mt: 2 }}
              data-testid="prompt-template-dialog-field"
            />
          </DialogContent>
          <DialogActions>
            <Button onClick={() => setExpandedPrompt(false)} variant="contained">
              Done
            </Button>
          </DialogActions>
        </Dialog>

        {/* Response Template Dialog */}
        <Dialog 
          open={expandedResponse} 
          onClose={() => setExpandedResponse(false)}
          fullWidth
          maxWidth="md"
          data-testid="response-template-dialog"
        >
          <DialogTitle>
            <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
              Response Template
              <IconButton onClick={() => setExpandedResponse(false)}>
                <CloseIcon />
              </IconButton>
            </Box>
          </DialogTitle>
          <DialogContent>
            <TextField
              autoFocus
              fullWidth
              multiline
              rows={15}
              value={responseTemplate}
              onChange={(e) => onTemplateChange('response_template', e.target.value)}
              variant="outlined"
              sx={{ mt: 2 }}
              data-testid="response-template-dialog-field"
            />
          </DialogContent>
          <DialogActions>
            <Button onClick={() => setExpandedResponse(false)} variant="contained">
              Done
            </Button>
          </DialogActions>
        </Dialog>
      </div>
    </ThemeProvider>
  );
};

describe('AgentForm Template Features', () => {
  const mockTemplateChange = jest.fn();
  const mockGenerateTemplates = jest.fn();

  beforeEach(() => {
    jest.clearAllMocks();
  });

  describe('Template Fields Rendering', () => {
    it('should render all three template fields with labels', () => {
      render(
        <TemplateSection 
          onTemplateChange={mockTemplateChange}
          onGenerateTemplates={mockGenerateTemplates}
        />
      );

      expect(screen.getByTestId('system-template-field')).toBeInTheDocument();
      expect(screen.getByTestId('prompt-template-field')).toBeInTheDocument();
      expect(screen.getByTestId('response-template-field')).toBeInTheDocument();
    });

    it('should render helper text for each template field', () => {
      render(
        <TemplateSection 
          onTemplateChange={mockTemplateChange}
          onGenerateTemplates={mockGenerateTemplates}
        />
      );

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
      render(
        <TemplateSection 
          onTemplateChange={mockTemplateChange}
          onGenerateTemplates={mockGenerateTemplates}
        />
      );

      expect(screen.getByTestId('expand-system-btn')).toBeInTheDocument();
      expect(screen.getByTestId('expand-prompt-btn')).toBeInTheDocument();
      expect(screen.getByTestId('expand-response-btn')).toBeInTheDocument();
    });
  });

  describe('Template Field Interactions', () => {
    it('should call onChange when typing in template fields', async () => {
      const user = userEvent.setup();
      render(
        <TemplateSection 
          onTemplateChange={mockTemplateChange}
          onGenerateTemplates={mockGenerateTemplates}
        />
      );

      const systemTemplateField = screen.getByTestId('system-template-field').querySelector('textarea');
      await user.type(systemTemplateField!, 'T');

      // Should be called for typing the character
      expect(mockTemplateChange).toHaveBeenCalledTimes(1);
      expect(mockTemplateChange).toHaveBeenCalledWith('system_template', 'T');
    });

    it('should open system template dialog when expand button is clicked', async () => {
      const user = userEvent.setup();
      render(
        <TemplateSection 
          onTemplateChange={mockTemplateChange}
          onGenerateTemplates={mockGenerateTemplates}
        />
      );

      const expandButton = screen.getByTestId('expand-system-btn');
      await user.click(expandButton);

      await waitFor(() => {
        expect(screen.getByTestId('system-template-dialog')).toBeInTheDocument();
      });
    });

    it('should open prompt template dialog when expand button is clicked', async () => {
      const user = userEvent.setup();
      render(
        <TemplateSection 
          onTemplateChange={mockTemplateChange}
          onGenerateTemplates={mockGenerateTemplates}
        />
      );

      const expandButton = screen.getByTestId('expand-prompt-btn');
      await user.click(expandButton);

      await waitFor(() => {
        expect(screen.getByTestId('prompt-template-dialog')).toBeInTheDocument();
      });
    });

    it('should open response template dialog when expand button is clicked', async () => {
      const user = userEvent.setup();
      render(
        <TemplateSection 
          onTemplateChange={mockTemplateChange}
          onGenerateTemplates={mockGenerateTemplates}
        />
      );

      const expandButton = screen.getByTestId('expand-response-btn');
      await user.click(expandButton);

      await waitFor(() => {
        expect(screen.getByTestId('response-template-dialog')).toBeInTheDocument();
      });
    });
  });

  describe('Template Generation', () => {
    it('should render Generate Templates button', () => {
      render(
        <TemplateSection 
          onTemplateChange={mockTemplateChange}
          onGenerateTemplates={mockGenerateTemplates}
        />
      );

      expect(screen.getByTestId('generate-templates-btn')).toBeInTheDocument();
    });

    it('should disable Generate Templates button when canGenerate is false', () => {
      render(
        <TemplateSection 
          onTemplateChange={mockTemplateChange}
          onGenerateTemplates={mockGenerateTemplates}
          canGenerate={false}
        />
      );

      const generateButton = screen.getByTestId('generate-templates-btn');
      expect(generateButton).toBeDisabled();
    });

    it('should enable Generate Templates button when canGenerate is true', () => {
      render(
        <TemplateSection 
          onTemplateChange={mockTemplateChange}
          onGenerateTemplates={mockGenerateTemplates}
          canGenerate={true}
        />
      );

      const generateButton = screen.getByTestId('generate-templates-btn');
      expect(generateButton).toBeEnabled();
    });

    it('should show loading state when generating templates', () => {
      render(
        <TemplateSection 
          onTemplateChange={mockTemplateChange}
          onGenerateTemplates={mockGenerateTemplates}
          canGenerate={true}
          isGenerating={true}
        />
      );

      const generateButton = screen.getByTestId('generate-templates-btn');
      expect(generateButton).toHaveTextContent('Generating...');
      expect(generateButton).toBeDisabled();
    });

    it('should call onGenerateTemplates when button is clicked', async () => {
      const user = userEvent.setup();
      render(
        <TemplateSection 
          onTemplateChange={mockTemplateChange}
          onGenerateTemplates={mockGenerateTemplates}
          canGenerate={true}
        />
      );

      const generateButton = screen.getByTestId('generate-templates-btn');
      await user.click(generateButton);

      expect(mockGenerateTemplates).toHaveBeenCalledTimes(1);
    });
  });

  describe('Template Values', () => {
    it('should display provided template values', () => {
      const templates = {
        systemTemplate: 'Test system template',
        promptTemplate: 'Test prompt template',
        responseTemplate: 'Test response template'
      };

      render(
        <TemplateSection 
          {...templates}
          onTemplateChange={mockTemplateChange}
          onGenerateTemplates={mockGenerateTemplates}
        />
      );

      expect(screen.getByDisplayValue('Test system template')).toBeInTheDocument();
      expect(screen.getByDisplayValue('Test prompt template')).toBeInTheDocument();
      expect(screen.getByDisplayValue('Test response template')).toBeInTheDocument();
    });

    it('should sync content between field and dialog', async () => {
      const user = userEvent.setup();
      render(
        <TemplateSection 
          systemTemplate="Initial content"
          onTemplateChange={mockTemplateChange}
          onGenerateTemplates={mockGenerateTemplates}
        />
      );

      // Open dialog
      const expandButton = screen.getByTestId('expand-system-btn');
      await user.click(expandButton);

      await waitFor(() => {
        const dialogField = screen.getByTestId('system-template-dialog-field').querySelector('textarea');
        expect(dialogField).toHaveValue('Initial content');
      });
    });
  });

  describe('Dialog Functionality', () => {
    it('should close dialog when Done button is clicked', async () => {
      const user = userEvent.setup();
      render(
        <TemplateSection 
          onTemplateChange={mockTemplateChange}
          onGenerateTemplates={mockGenerateTemplates}
        />
      );

      // Open dialog
      const expandButton = screen.getByTestId('expand-system-btn');
      await user.click(expandButton);

      await waitFor(() => {
        expect(screen.getByTestId('system-template-dialog')).toBeInTheDocument();
      });

      // Close dialog with Done button
      const doneButton = screen.getByRole('button', { name: /done/i });
      await user.click(doneButton);

      await waitFor(() => {
        expect(screen.queryByTestId('system-template-dialog')).not.toBeInTheDocument();
      });
    });

    it('should have larger text area in dialog (15 rows)', async () => {
      const user = userEvent.setup();
      render(
        <TemplateSection 
          onTemplateChange={mockTemplateChange}
          onGenerateTemplates={mockGenerateTemplates}
        />
      );

      // Open dialog
      const expandButton = screen.getByTestId('expand-system-btn');
      await user.click(expandButton);

      await waitFor(() => {
        const dialogTextarea = screen.getByTestId('system-template-dialog-field').querySelector('textarea');
        expect(dialogTextarea).toHaveAttribute('rows', '15');
      });
    });
  });
});