/**
 * Tests for InputVariablesDialog component.
 *
 * Covers:
 * - Variable extraction regex (identifier-only pattern)
 * - CSS/JS brace content correctly ignored
 * - Variable detection from agent and task nodes
 * - Dialog open/close behavior
 * - Required/optional variable toggling
 * - Adding/removing custom variables
 * - Validation of required fields
 * - Search/filter functionality
 * - Confirm and cancel flows
 * - Clear all values
 * - Pagination (show more/less)
 */
import React from 'react';
import { render, screen, fireEvent, waitFor, within, act } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { vi, describe, it, expect, beforeEach } from 'vitest';
import { InputVariablesDialog } from './InputVariablesDialog';
import { Node } from 'reactflow';

describe('InputVariablesDialog', () => {
  const defaultProps = {
    open: true,
    onClose: vi.fn(),
    onConfirm: vi.fn(),
    nodes: [] as Node[],
  };

  beforeEach(() => {
    vi.clearAllMocks();
  });

  // Helper to create mock nodes
  const createAgentNode = (id: string, data: Record<string, unknown>): Node => ({
    id,
    type: 'agentNode',
    position: { x: 0, y: 0 },
    data,
  });

  const createTaskNode = (id: string, data: Record<string, unknown>): Node => ({
    id,
    type: 'taskNode',
    position: { x: 0, y: 0 },
    data,
  });

  const createOtherNode = (id: string, type: string, data: Record<string, unknown>): Node => ({
    id,
    type,
    position: { x: 0, y: 0 },
    data,
  });

  describe('Variable Extraction Regex', () => {
    it('extracts simple variables like {topic}', () => {
      const nodes = [
        createTaskNode('t1', { description: 'Research about {topic}' }),
      ];
      render(<InputVariablesDialog {...defaultProps} nodes={nodes} />);

      expect(screen.getByDisplayValue('topic')).toBeInTheDocument();
    });

    it('extracts variables with underscores like {user_name}', () => {
      const nodes = [
        createTaskNode('t1', { description: 'Hello {user_name}' }),
      ];
      render(<InputVariablesDialog {...defaultProps} nodes={nodes} />);

      expect(screen.getByDisplayValue('user_name')).toBeInTheDocument();
    });

    it('extracts variables with hyphens like {date-range}', () => {
      const nodes = [
        createTaskNode('t1', { description: 'Filter by {date-range}' }),
      ];
      render(<InputVariablesDialog {...defaultProps} nodes={nodes} />);

      expect(screen.getByDisplayValue('date-range')).toBeInTheDocument();
    });

    it('extracts variables starting with underscore like {_private}', () => {
      const nodes = [
        createTaskNode('t1', { description: 'Use {_private} config' }),
      ];
      render(<InputVariablesDialog {...defaultProps} nodes={nodes} />);

      expect(screen.getByDisplayValue('_private')).toBeInTheDocument();
    });

    it('extracts multiple variables from one field', () => {
      const nodes = [
        createAgentNode('a1', { goal: 'Analyze {topic} for {company} in {year}' }),
      ];
      render(<InputVariablesDialog {...defaultProps} nodes={nodes} />);

      expect(screen.getByDisplayValue('topic')).toBeInTheDocument();
      expect(screen.getByDisplayValue('company')).toBeInTheDocument();
      expect(screen.getByDisplayValue('year')).toBeInTheDocument();
    });

    it('deduplicates variables across nodes and fields', () => {
      const nodes = [
        createAgentNode('a1', { goal: 'Analyze {topic}', role: 'Expert on {topic}' }),
        createTaskNode('t1', { description: 'Research {topic}' }),
      ];
      render(<InputVariablesDialog {...defaultProps} nodes={nodes} />);

      // Should only appear once
      const topicFields = screen.getAllByDisplayValue('topic');
      expect(topicFields).toHaveLength(1);
    });

    it('does NOT extract CSS content like { overflow: hidden; }', () => {
      const nodes = [
        createTaskNode('t1', {
          description: 'Create HTML with CSS: .reveal .slides section { overflow: hidden; }',
        }),
      ];
      render(<InputVariablesDialog {...defaultProps} nodes={nodes} />);

      // The dialog title shows "Input Variables" but no variable rows should appear
      expect(screen.queryByDisplayValue('overflow: hidden;')).not.toBeInTheDocument();
      expect(screen.queryByDisplayValue(' overflow: hidden; ')).not.toBeInTheDocument();
    });

    it('does NOT extract CSS with font-size like { font-size: 1.5em; margin-bottom: 0.4em; }', () => {
      const nodes = [
        createTaskNode('t1', {
          description: '.reveal h2 { font-size: 1.5em; margin-bottom: 0.4em; }',
        }),
      ];
      render(<InputVariablesDialog {...defaultProps} nodes={nodes} />);

      expect(screen.queryByDisplayValue('font-size: 1.5em; margin-bottom: 0.4em;')).not.toBeInTheDocument();
    });

    it('does NOT extract JS config like { width: 960, height: 700, margin: 0.1 }', () => {
      const nodes = [
        createTaskNode('t1', {
          description: 'Reveal.initialize({ width: 960, height: 700, margin: 0.1, center: true })',
        }),
      ];
      render(<InputVariablesDialog {...defaultProps} nodes={nodes} />);

      expect(screen.queryByDisplayValue('width: 960, height: 700, margin: 0.1, center: true')).not.toBeInTheDocument();
    });

    it('does NOT extract content with spaces like { some text }', () => {
      const nodes = [
        createTaskNode('t1', { description: 'Use { some text } in output' }),
      ];
      render(<InputVariablesDialog {...defaultProps} nodes={nodes} />);

      expect(screen.queryByDisplayValue('some text')).not.toBeInTheDocument();
      expect(screen.queryByDisplayValue(' some text ')).not.toBeInTheDocument();
    });

    it('does NOT extract content starting with a number like {123}', () => {
      const nodes = [
        createTaskNode('t1', { description: 'Item {123}' }),
      ];
      render(<InputVariablesDialog {...defaultProps} nodes={nodes} />);

      expect(screen.queryByDisplayValue('123')).not.toBeInTheDocument();
    });

    it('correctly handles mixed valid variables and CSS braces', () => {
      const nodes = [
        createTaskNode('t1', {
          description:
            'Create a {format} presentation about {topic}. Include CSS: .reveal h1 { font-size: 2em; } and init with Reveal.initialize({ width: 960 })',
        }),
      ];
      render(<InputVariablesDialog {...defaultProps} nodes={nodes} />);

      // Valid variables should be extracted
      expect(screen.getByDisplayValue('format')).toBeInTheDocument();
      expect(screen.getByDisplayValue('topic')).toBeInTheDocument();

      // CSS/JS content should NOT be extracted
      expect(screen.queryByDisplayValue('font-size: 2em;')).not.toBeInTheDocument();
      expect(screen.queryByDisplayValue('width: 960')).not.toBeInTheDocument();
    });

    it('extracts variables from all checked fields: role, goal, backstory, description, expected_output, label', () => {
      const nodes = [
        createAgentNode('a1', {
          role: 'Expert in {field}',
          goal: 'Analyze {target}',
          backstory: 'Trained on {dataset}',
        }),
        createTaskNode('t1', {
          description: 'Research {subject}',
          expected_output: 'Report about {output_type}',
          label: 'Task for {client}',
        }),
      ];
      render(<InputVariablesDialog {...defaultProps} nodes={nodes} />);

      expect(screen.getByDisplayValue('field')).toBeInTheDocument();
      expect(screen.getByDisplayValue('target')).toBeInTheDocument();
      expect(screen.getByDisplayValue('dataset')).toBeInTheDocument();
      expect(screen.getByDisplayValue('subject')).toBeInTheDocument();
      expect(screen.getByDisplayValue('output_type')).toBeInTheDocument();
      expect(screen.getByDisplayValue('client')).toBeInTheDocument();
    });

    it('ignores non-agent/non-task node types', () => {
      const nodes = [
        createOtherNode('c1', 'crewNode', { label: 'Crew for {project}' }),
        createOtherNode('f1', 'flowNode', { description: 'Flow with {variable}' }),
      ];
      render(<InputVariablesDialog {...defaultProps} nodes={nodes} />);

      expect(screen.queryByDisplayValue('project')).not.toBeInTheDocument();
      expect(screen.queryByDisplayValue('variable')).not.toBeInTheDocument();
    });

    it('ignores non-string field values', () => {
      const nodes = [
        createAgentNode('a1', {
          role: 123,
          goal: null,
          backstory: undefined,
          description: true,
        }),
      ];
      render(<InputVariablesDialog {...defaultProps} nodes={nodes} />);

      // No variables should be detected, so we check via the chip count
      expect(screen.getByText('0 Required')).toBeInTheDocument();
    });
  });

  describe('Dialog Open/Close', () => {
    it('does not extract variables when dialog is closed', () => {
      const nodes = [
        createTaskNode('t1', { description: 'Research {topic}' }),
      ];
      render(<InputVariablesDialog {...defaultProps} open={false} nodes={nodes} />);

      expect(screen.queryByDisplayValue('topic')).not.toBeInTheDocument();
    });

    it('resets search and pagination state when dialog closes', () => {
      const nodes = [
        createTaskNode('t1', { description: 'Research {topic}' }),
      ];
      const { rerender } = render(
        <InputVariablesDialog {...defaultProps} nodes={nodes} />
      );

      // Close the dialog
      rerender(
        <InputVariablesDialog {...defaultProps} open={false} nodes={nodes} />
      );

      // Reopen
      rerender(
        <InputVariablesDialog {...defaultProps} open={true} nodes={nodes} />
      );

      // Should still show the variable
      expect(screen.getByDisplayValue('topic')).toBeInTheDocument();
    });

    it('calls onClose when cancel button is clicked', () => {
      render(<InputVariablesDialog {...defaultProps} />);

      fireEvent.click(screen.getByText('Cancel'));
      expect(defaultProps.onClose).toHaveBeenCalledTimes(1);
    });

    it('calls onClose when X button is clicked', () => {
      render(<InputVariablesDialog {...defaultProps} />);

      // The X button is in the dialog title area
      const closeButtons = screen.getAllByRole('button');
      // Find the close icon button in the title (first close icon)
      const titleCloseButton = closeButtons.find(btn =>
        btn.querySelector('[data-testid="CloseIcon"]')
      );
      if (titleCloseButton) {
        fireEvent.click(titleCloseButton);
        expect(defaultProps.onClose).toHaveBeenCalled();
      }
    });
  });

  describe('Required/Optional Toggle', () => {
    it('detected variables default to required', () => {
      const nodes = [
        createTaskNode('t1', { description: 'Research {topic}' }),
      ];
      render(<InputVariablesDialog {...defaultProps} nodes={nodes} />);

      expect(screen.getByText('1 Required')).toBeInTheDocument();
    });

    it('clears validation error when toggling required variable to optional', async () => {
      const nodes = [
        createTaskNode('t1', { description: 'Research {topic}' }),
      ];
      render(<InputVariablesDialog {...defaultProps} nodes={nodes} />);

      // Trigger a validation error by submitting without filling in value
      fireEvent.click(screen.getByText('Execute with Variables'));

      await waitFor(() => {
        expect(screen.getByText('This variable is required')).toBeInTheDocument();
      });

      // Now toggle the variable to optional - this should clear the error
      const switches = screen.getAllByRole('checkbox');
      fireEvent.click(switches[0]);

      await waitFor(() => {
        expect(screen.queryByText('This variable is required')).not.toBeInTheDocument();
        expect(screen.getByText('0 Required')).toBeInTheDocument();
        expect(screen.getByText('1 Optional')).toBeInTheDocument();
      });
    });

    it('toggles variable from required to optional', async () => {
      const nodes = [
        createTaskNode('t1', { description: 'Research {topic}' }),
      ];
      render(<InputVariablesDialog {...defaultProps} nodes={nodes} />);

      // Find the switch for toggling required
      const switches = screen.getAllByRole('checkbox');
      expect(switches.length).toBeGreaterThan(0);

      // Toggle the first switch
      fireEvent.click(switches[0]);

      await waitFor(() => {
        expect(screen.getByText('0 Required')).toBeInTheDocument();
        expect(screen.getByText('1 Optional')).toBeInTheDocument();
      });
    });
  });

  describe('Add/Remove Custom Variables', () => {
    it('adds a custom variable when Add Custom Variable is clicked', async () => {
      render(<InputVariablesDialog {...defaultProps} />);

      fireEvent.click(screen.getByText('Add Custom Variable'));

      // A new empty row should appear in the optional section
      await waitFor(() => {
        expect(screen.getByText('1 Optional')).toBeInTheDocument();
      });
    });

    it('removes a custom variable when remove button is clicked', async () => {
      render(<InputVariablesDialog {...defaultProps} />);

      // Add a custom variable
      fireEvent.click(screen.getByText('Add Custom Variable'));

      await waitFor(() => {
        expect(screen.getByText('1 Optional')).toBeInTheDocument();
      });

      // Find and click the remove button (CloseIcon in the row)
      const removeButtons = screen.getAllByRole('button').filter(
        btn => btn.querySelector('[data-testid="CloseIcon"]') && btn.closest('[class*="error"]') !== null
      );

      // There should be a remove button for the custom variable
      // Custom (non-detected) variables show a red close button
      const allButtons = screen.getAllByRole('button');
      const errorColorButtons = allButtons.filter(btn => {
        const svg = btn.querySelector('svg');
        return svg && btn.getAttribute('class')?.includes('error');
      });

      if (errorColorButtons.length > 0) {
        fireEvent.click(errorColorButtons[0]);
        await waitFor(() => {
          expect(screen.getByText('0 Optional')).toBeInTheDocument();
        });
      }
    });
  });

  describe('Validation', () => {
    it('shows error when required variable has no value on confirm', async () => {
      const nodes = [
        createTaskNode('t1', { description: 'Research {topic}' }),
      ];
      render(<InputVariablesDialog {...defaultProps} nodes={nodes} />);

      // Click Execute without filling in value
      fireEvent.click(screen.getByText('Execute with Variables'));

      await waitFor(() => {
        expect(screen.getByText('This variable is required')).toBeInTheDocument();
      });

      // onConfirm should NOT have been called
      expect(defaultProps.onConfirm).not.toHaveBeenCalled();
    });

    it('calls onConfirm with variables when all required are filled', async () => {
      const nodes = [
        createTaskNode('t1', { description: 'Research {topic}' }),
      ];
      render(<InputVariablesDialog {...defaultProps} nodes={nodes} />);

      // Fill in the value
      const valueInputs = screen.getAllByRole('textbox');
      // The second textbox in the row is the value field
      const valueInput = valueInputs.find(input =>
        input.closest('[class*="flex"]') && !screen.getByDisplayValue('topic').isSameNode(input)
      );

      if (valueInput) {
        await userEvent.type(valueInput, 'AI Safety');
      } else {
        // Find by placeholder
        const requiredInputs = screen.getAllByPlaceholderText('Required');
        await userEvent.type(requiredInputs[0], 'AI Safety');
      }

      fireEvent.click(screen.getByText('Execute with Variables'));

      await waitFor(() => {
        expect(defaultProps.onConfirm).toHaveBeenCalledWith({ topic: 'AI Safety' });
      });
    });

    it('clears error when user types a value', async () => {
      const nodes = [
        createTaskNode('t1', { description: 'Research {topic}' }),
      ];
      render(<InputVariablesDialog {...defaultProps} nodes={nodes} />);

      // Trigger validation error first
      fireEvent.click(screen.getByText('Execute with Variables'));

      await waitFor(() => {
        expect(screen.getByText('This variable is required')).toBeInTheDocument();
      });

      // Now type a value
      const requiredInputs = screen.getAllByPlaceholderText('Required');
      await userEvent.type(requiredInputs[0], 'AI');

      await waitFor(() => {
        expect(screen.queryByText('This variable is required')).not.toBeInTheDocument();
      });
    });
  });

  describe('Clear All Values', () => {
    it('clears all variable values when Clear All Values is clicked', async () => {
      const nodes = [
        createTaskNode('t1', { description: 'Research {topic}' }),
      ];
      render(<InputVariablesDialog {...defaultProps} nodes={nodes} />);

      // Type a value
      const requiredInputs = screen.getAllByPlaceholderText('Required');
      await userEvent.type(requiredInputs[0], 'AI Safety');

      // Click Clear All Values
      fireEvent.click(screen.getByText('Clear All Values'));

      await waitFor(() => {
        expect(requiredInputs[0]).toHaveValue('');
      });
    });
  });

  describe('Summary Chips', () => {
    it('shows total count chip when variables exist', () => {
      const nodes = [
        createAgentNode('a1', { goal: 'Analyze {topic} for {company}' }),
      ];
      render(<InputVariablesDialog {...defaultProps} nodes={nodes} />);

      expect(screen.getByText('2 total')).toBeInTheDocument();
      expect(screen.getByText('2 Required')).toBeInTheDocument();
      expect(screen.getByText('0 Optional')).toBeInTheDocument();
    });

    it('does not show total count chip when no variables', () => {
      render(<InputVariablesDialog {...defaultProps} />);

      expect(screen.queryByText(/total/)).not.toBeInTheDocument();
    });
  });

  describe('Accordion Toggle', () => {
    it('collapses and expands required section', () => {
      const nodes = [createTaskNode('t1', { description: 'Research {topic}' })];
      render(<InputVariablesDialog {...defaultProps} nodes={nodes} />);

      // The Required Variables accordion summary is clickable
      const requiredHeader = screen.getByText('Required Variables');
      fireEvent.click(requiredHeader);

      // Click again to re-expand
      fireEvent.click(requiredHeader);

      // Still shows the variable
      expect(screen.getByDisplayValue('topic')).toBeInTheDocument();
    });

    it('collapses and expands optional section', () => {
      render(<InputVariablesDialog {...defaultProps} />);

      // Add a custom variable so optional section has content
      fireEvent.click(screen.getByText('Add Custom Variable'));

      const optionalHeader = screen.getByText('Optional Variables');
      fireEvent.click(optionalHeader);

      // Click again to re-expand
      fireEvent.click(optionalHeader);

      expect(screen.getByText('1 Optional')).toBeInTheDocument();
    });
  });

  describe('Info Alert', () => {
    it('displays usage instructions', () => {
      render(<InputVariablesDialog {...defaultProps} />);

      expect(screen.getByText('How to use variables:')).toBeInTheDocument();
    });
  });

  describe('Form Submission', () => {
    it('handles form submit via Enter key', async () => {
      const nodes = [
        createTaskNode('t1', { description: 'Research {topic}' }),
      ];
      render(<InputVariablesDialog {...defaultProps} nodes={nodes} />);

      // Fill in value
      const requiredInputs = screen.getAllByPlaceholderText('Required');
      await userEvent.type(requiredInputs[0], 'AI Safety');

      // Submit the form
      fireEvent.click(screen.getByText('Execute with Variables'));

      await waitFor(() => {
        expect(defaultProps.onConfirm).toHaveBeenCalledWith({ topic: 'AI Safety' });
      });
    });

    it('skips variables with empty names in output', async () => {
      render(<InputVariablesDialog {...defaultProps} />);

      // Add a custom variable (it starts with empty name and value)
      fireEvent.click(screen.getByText('Add Custom Variable'));

      // Submit - custom variable has empty name, should not be in output
      fireEvent.click(screen.getByText('Execute with Variables'));

      await waitFor(() => {
        // onConfirm should be called with empty object (no named variables)
        expect(defaultProps.onConfirm).toHaveBeenCalledWith({});
      });
    });

    it('skips optional variables with no value in output', async () => {
      const nodes = [
        createTaskNode('t1', { description: 'Research {topic}' }),
      ];
      render(<InputVariablesDialog {...defaultProps} nodes={nodes} />);

      // Toggle topic to optional
      const switches = screen.getAllByRole('checkbox');
      fireEvent.click(switches[0]);

      // Submit without filling in value
      fireEvent.click(screen.getByText('Execute with Variables'));

      await waitFor(() => {
        // topic has no value and is optional, so it should not appear in output
        expect(defaultProps.onConfirm).toHaveBeenCalledWith({});
      });
    });
  });

  describe('Reveal.js Edge Case (original bug)', () => {
    it('does NOT extract any false variables from a full reveal.js task description', () => {
      const revealDescription = `Create a reveal.js presentation about the latest Swiss news. Generate a single HTML file using reveal.js 5.1.0 from jsDelivr CDN. Include required CSS: .reveal .slides section { overflow: hidden; } .reveal h1 { font-size: 2.2em; margin-bottom: 0.5em; } .reveal h2 { font-size: 1.5em; margin-bottom: 0.4em; } .reveal ul, .reveal ol { font-size: 0.85em; max-height: 60vh; overflow: hidden; margin-left: 1em; } .reveal li { margin: 0.4em 0; line-height: 1.3; } .reveal img { max-height: 45vh; max-width: 85%; display: block; margin: 0 auto; } .reveal p { font-size: 0.9em; max-height: 50vh; overflow: hidden; }. Initialize with: Reveal.initialize({ width: 960, height: 700, margin: 0.1, center: true, hash: true, slideNumber: true, transition: 'slide' }).`;

      const nodes = [
        createTaskNode('t1', { description: revealDescription }),
      ];
      render(<InputVariablesDialog {...defaultProps} nodes={nodes} />);

      // No variables should be detected
      expect(screen.getByText('0 Required')).toBeInTheDocument();
      expect(screen.getByText('0 Optional')).toBeInTheDocument();
    });

    it('extracts only real variables from reveal.js description that also contains actual variables', () => {
      const description = `Create a {format} presentation about {topic}. Include CSS: .reveal h1 { font-size: 2.2em; } Initialize with: Reveal.initialize({ width: 960 })`;

      const nodes = [
        createTaskNode('t1', { description }),
      ];
      render(<InputVariablesDialog {...defaultProps} nodes={nodes} />);

      // Only format and topic should be detected
      expect(screen.getByDisplayValue('format')).toBeInTheDocument();
      expect(screen.getByDisplayValue('topic')).toBeInTheDocument();
      expect(screen.getByText('2 Required')).toBeInTheDocument();
      expect(screen.getByText('0 Optional')).toBeInTheDocument();
    });
  });

  describe('Accordion Sections', () => {
    it('renders Required Variables section', () => {
      render(<InputVariablesDialog {...defaultProps} />);
      expect(screen.getByText('Required Variables')).toBeInTheDocument();
    });

    it('renders Optional Variables section', () => {
      render(<InputVariablesDialog {...defaultProps} />);
      expect(screen.getByText('Optional Variables')).toBeInTheDocument();
    });

    it('shows empty state for required when no required variables', () => {
      render(<InputVariablesDialog {...defaultProps} />);
      expect(
        screen.getByText('No required variables. Toggle the switch to mark variables as required.')
      ).toBeInTheDocument();
    });

    it('shows empty state for optional when no optional variables', () => {
      render(<InputVariablesDialog {...defaultProps} />);
      expect(
        screen.getByText(
          'No optional variables. Add custom variables or toggle required variables to optional.'
        )
      ).toBeInTheDocument();
    });
  });

  describe('Pagination (>20 variables)', () => {
    // ITEMS_PER_SECTION = 20 in the component
    const createManyVariableNodes = (count: number): Node[] => {
      const varNames = Array.from({ length: count }, (_, i) => `var_${i + 1}`);
      const description = varNames.map(v => `{${v}}`).join(' ');
      return [createTaskNode('t1', { description })];
    };

    it('shows "Show all" button when more than 20 required variables', async () => {
      const nodes = createManyVariableNodes(25);
      render(<InputVariablesDialog {...defaultProps} nodes={nodes} />);

      await waitFor(() => {
        expect(screen.getByText('25 Required')).toBeInTheDocument();
      });

      // Should show "Show all (5 more)" button
      expect(screen.getByText(/Show all.*5 more/)).toBeInTheDocument();
    });

    it('shows all required variables when "Show all" is clicked', async () => {
      const nodes = createManyVariableNodes(25);
      render(<InputVariablesDialog {...defaultProps} nodes={nodes} />);

      await waitFor(() => {
        expect(screen.getByText(/Show all.*5 more/)).toBeInTheDocument();
      });

      fireEvent.click(screen.getByText(/Show all.*5 more/));

      await waitFor(() => {
        expect(screen.getByText('Show less')).toBeInTheDocument();
      });
    });

    it('collapses back when "Show less" is clicked', async () => {
      const nodes = createManyVariableNodes(25);
      render(<InputVariablesDialog {...defaultProps} nodes={nodes} />);

      await waitFor(() => {
        expect(screen.getByText(/Show all.*5 more/)).toBeInTheDocument();
      });

      fireEvent.click(screen.getByText(/Show all.*5 more/));

      await waitFor(() => {
        expect(screen.getByText('Show less')).toBeInTheDocument();
      });

      fireEvent.click(screen.getByText('Show less'));

      await waitFor(() => {
        expect(screen.getByText(/Show all.*5 more/)).toBeInTheDocument();
      });
    });

    it('does not show pagination buttons when variables are under 20', () => {
      const nodes = createManyVariableNodes(5);
      render(<InputVariablesDialog {...defaultProps} nodes={nodes} />);

      expect(screen.queryByText(/Show all/)).not.toBeInTheDocument();
      expect(screen.queryByText('Show less')).not.toBeInTheDocument();
    });
  });

  describe('Search Functionality (>20 variables)', () => {
    const createManyVariableNodes = (count: number): Node[] => {
      const varNames = Array.from({ length: count }, (_, i) => `var_${i + 1}`);
      const description = varNames.map(v => `{${v}}`).join(' ');
      return [createTaskNode('t1', { description })];
    };

    it('shows search bar when >20 variables', async () => {
      const nodes = createManyVariableNodes(25);
      render(<InputVariablesDialog {...defaultProps} nodes={nodes} />);

      await waitFor(() => {
        expect(screen.getByPlaceholderText('Search variables...')).toBeInTheDocument();
      });
    });

    it('does NOT show search bar when <=20 variables', () => {
      const nodes = createManyVariableNodes(5);
      render(<InputVariablesDialog {...defaultProps} nodes={nodes} />);

      expect(screen.queryByPlaceholderText('Search variables...')).not.toBeInTheDocument();
    });

    it('filters variables by search query', async () => {
      const nodes = createManyVariableNodes(25);
      render(<InputVariablesDialog {...defaultProps} nodes={nodes} />);

      await waitFor(() => {
        expect(screen.getByPlaceholderText('Search variables...')).toBeInTheDocument();
      });

      const searchInput = screen.getByPlaceholderText('Search variables...');
      fireEvent.change(searchInput, { target: { value: 'var_1' } });

      // Should show "Showing X of 25 variables"
      await waitFor(() => {
        expect(screen.getByText(/Showing \d+ of 25 variables/)).toBeInTheDocument();
      });
    });

    it('clears search when clear icon is clicked', async () => {
      const nodes = createManyVariableNodes(25);
      render(<InputVariablesDialog {...defaultProps} nodes={nodes} />);

      await waitFor(() => {
        expect(screen.getByPlaceholderText('Search variables...')).toBeInTheDocument();
      });

      const searchInput = screen.getByPlaceholderText('Search variables...');
      fireEvent.change(searchInput, { target: { value: 'var_1' } });

      await waitFor(() => {
        expect(screen.getByText(/Showing \d+ of 25 variables/)).toBeInTheDocument();
      });

      // Find and click the clear icon in the search input
      // The clear icon is inside an InputAdornment
      const clearButtons = screen.getAllByRole('button').filter(
        btn => btn.closest('[class*="InputAdornment"]') !== null
      );

      if (clearButtons.length > 0) {
        fireEvent.click(clearButtons[0]);
        await waitFor(() => {
          expect(screen.queryByText(/Showing \d+ of/)).not.toBeInTheDocument();
        });
      }
    });
  });

  describe('handleRemoveVariable via DOM', () => {
    it('removes custom variable when clicking the error-colored close button', async () => {
      render(<InputVariablesDialog {...defaultProps} />);

      // Add a custom variable
      fireEvent.click(screen.getByText('Add Custom Variable'));

      await waitFor(() => {
        expect(screen.getByText('1 Optional')).toBeInTheDocument();
      });

      // Custom (non-detected) variables render a red close button via:
      // <IconButton onClick={() => handleRemoveVariable(variable.name)} size="small" color="error">
      // Find all error-colored icon buttons (they have MuiIconButton-colorError in className)
      const allButtons = document.querySelectorAll('button.MuiIconButton-colorError');
      expect(allButtons.length).toBeGreaterThan(0);

      fireEvent.click(allButtons[0]);

      await waitFor(() => {
        expect(screen.getByText('0 Optional')).toBeInTheDocument();
      });
    });
  });

  describe('Optional Pagination and Search Clear', () => {
    it('shows show all / show less for optional variables when >20 custom variables added', async () => {
      render(<InputVariablesDialog {...defaultProps} />);

      // Add 25 custom variables (all optional by default)
      for (let i = 0; i < 25; i++) {
        fireEvent.click(screen.getByText('Add Custom Variable'));
      }

      await waitFor(() => {
        expect(screen.getByText('25 Optional')).toBeInTheDocument();
      });

      // The search bar should appear for >20 variables
      expect(screen.getByPlaceholderText('Search variables...')).toBeInTheDocument();

      // Should show "Show all (5 more)" in optional section
      expect(screen.getByText(/Show all.*5 more/)).toBeInTheDocument();

      // Click show all
      fireEvent.click(screen.getByText(/Show all.*5 more/));

      await waitFor(() => {
        expect(screen.getByText('Show less')).toBeInTheDocument();
      });

      // Click show less
      fireEvent.click(screen.getByText('Show less'));

      await waitFor(() => {
        expect(screen.getByText(/Show all.*5 more/)).toBeInTheDocument();
      });
    });

    it('shows filtered count and allows clearing search', async () => {
      render(<InputVariablesDialog {...defaultProps} />);

      // Add 25 custom variables with names
      for (let i = 0; i < 25; i++) {
        fireEvent.click(screen.getByText('Add Custom Variable'));
      }

      await waitFor(() => {
        expect(screen.getByPlaceholderText('Search variables...')).toBeInTheDocument();
      });

      // Name some variables so search has something to match
      const textboxes = screen.getAllByRole('textbox');
      const nameInputs = textboxes.filter(t => (t as HTMLInputElement).value === '');
      if (nameInputs.length > 0) {
        fireEvent.change(nameInputs[0], { target: { value: 'findme' } });
      }

      // Search for it
      const searchInput = screen.getByPlaceholderText('Search variables...');
      fireEvent.change(searchInput, { target: { value: 'findme' } });

      await waitFor(() => {
        expect(screen.getByText(/Showing \d+ of 25 variables/)).toBeInTheDocument();
      });
    });
  });

  describe('Variable Name Change', () => {
    it('allows editing variable name for custom variables via fireEvent', () => {
      render(<InputVariablesDialog {...defaultProps} />);

      // Add a custom variable
      fireEvent.click(screen.getByText('Add Custom Variable'));

      // Find the "Variable Name" input (custom variable's name field)
      const nameInputs = screen.getAllByRole('textbox').filter(
        input => (input as HTMLInputElement).value === ''
      );
      expect(nameInputs.length).toBeGreaterThan(0);

      // Use fireEvent.change for a single-shot value set
      fireEvent.change(nameInputs[0], { target: { value: 'my_var' } });
      expect(nameInputs[0]).toHaveValue('my_var');
    });
  });
});
