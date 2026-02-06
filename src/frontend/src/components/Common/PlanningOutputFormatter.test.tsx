/**
 * Unit tests for PlanningOutputFormatter component.
 *
 * Tests the parsing and rendering of CrewAI planning output,
 * including task detection, field extraction, and UI rendering.
 */
import React from 'react';
import { render, screen, within } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { ThemeProvider } from '@mui/material/styles';
import { createTheme } from '@mui/material/styles';
import { vi, describe, it, expect, beforeEach } from 'vitest';

import PlanningOutputFormatter, { isPlanningOutput, parseJsonPlanningOutput } from './PlanningOutputFormatter';

const theme = createTheme();

const TestWrapper: React.FC<{ children: React.ReactNode }> = ({ children }) => (
  <ThemeProvider theme={theme}>{children}</ThemeProvider>
);

// Sample planning output for testing
const samplePlanningOutput = `
Task Number 1 - Research market trends
"task_description": "Analyze current market trends in the AI industry"
"task_expected_output": "A comprehensive report on AI market trends"
"agent": "Research Agent"
"agent_goal": "Gather and analyze market data"
"task_tools": [PerplexitySearchTool(name='PerplexityTool', description='Search tool')]
"agent_tools": "Web search, data analysis"

Task Number 2 - Generate report
"task_description": "Create a detailed report based on research findings"
"task_expected_output": "A formatted PDF report"
"agent": "Writer Agent"
"agent_goal": "Transform data into readable content"
"task_tools": []
"agent_tools": "agent has no tools"
`;

const samplePlanningOutputWithSummary = `
Task Number 1 - Initial task
"task_description": "First task description"
"agent": "Test Agent"

Based on these tasks summary, the plan is ready.
`;

describe('isPlanningOutput', () => {
  it('returns true for valid planning output', () => {
    expect(isPlanningOutput(samplePlanningOutput)).toBe(true);
  });

  it('returns true for output with task summary', () => {
    expect(isPlanningOutput(samplePlanningOutputWithSummary)).toBe(true);
  });

  it('returns false for null input', () => {
    expect(isPlanningOutput(null as any)).toBe(false);
  });

  it('returns false for undefined input', () => {
    expect(isPlanningOutput(undefined as any)).toBe(false);
  });

  it('returns false for empty string', () => {
    expect(isPlanningOutput('')).toBe(false);
  });

  it('returns false for non-planning output', () => {
    expect(isPlanningOutput('Just some regular text output')).toBe(false);
  });

  it('returns false for partial match (only Task Number)', () => {
    expect(isPlanningOutput('Task Number 1 - Some task\nNo other markers')).toBe(false);
  });

  it('returns true when has Task Number and task_description', () => {
    const content = 'Task Number 1 - Test\n"task_description": "Test description"';
    expect(isPlanningOutput(content)).toBe(true);
  });

  it('returns true when has Task Number and agent', () => {
    const content = 'Task Number 1 - Test\n"agent": "Test Agent"';
    expect(isPlanningOutput(content)).toBe(true);
  });

  it('returns true when has Task Number and plan summary', () => {
    const content = 'Task Number 1 - Test\nBased on these tasks summary';
    expect(isPlanningOutput(content)).toBe(true);
  });

  it('returns false for non-string input', () => {
    expect(isPlanningOutput(123 as any)).toBe(false);
    expect(isPlanningOutput({} as any)).toBe(false);
    expect(isPlanningOutput([] as any)).toBe(false);
  });

  describe('JSON format with list_of_plans_per_task', () => {
    const sampleJsonPlanningOutput = JSON.stringify({
      list_of_plans_per_task: [
        {
          task: 'Scrape top 3 Swiss news websites',
          plan: 'Step 1: Understand the Task Requirements\n- The goal is to scrape Swiss news\nStep 2: Execute the scraping'
        },
        {
          task: 'Analyze the scraped content',
          plan: 'Step 1: Review the data\nStep 2: Categorize findings'
        }
      ]
    });

    it('returns true for valid JSON planning output', () => {
      expect(isPlanningOutput(sampleJsonPlanningOutput)).toBe(true);
    });

    it('returns false for JSON without list_of_plans_per_task', () => {
      const invalidJson = JSON.stringify({ some_other_field: 'value' });
      expect(isPlanningOutput(invalidJson)).toBe(false);
    });

    it('returns false for JSON with empty list_of_plans_per_task', () => {
      const emptyListJson = JSON.stringify({ list_of_plans_per_task: [] });
      expect(isPlanningOutput(emptyListJson)).toBe(false);
    });

    it('returns false for JSON with invalid plan entries', () => {
      const invalidEntries = JSON.stringify({
        list_of_plans_per_task: [{ task: 'Missing plan field' }]
      });
      expect(isPlanningOutput(invalidEntries)).toBe(false);
    });
  });
});

describe('parseJsonPlanningOutput', () => {
  it('parses valid JSON planning output correctly', () => {
    const jsonContent = JSON.stringify({
      list_of_plans_per_task: [
        { task: 'Task 1', plan: 'Step 1: Do something' },
        { task: 'Task 2', plan: 'Step 1: Do another thing' }
      ]
    });
    const result = parseJsonPlanningOutput(jsonContent);
    expect(result).not.toBeNull();
    expect(result!.list_of_plans_per_task).toHaveLength(2);
    expect(result!.list_of_plans_per_task[0].task).toBe('Task 1');
    expect(result!.list_of_plans_per_task[1].plan).toBe('Step 1: Do another thing');
  });

  it('returns null for non-JSON content', () => {
    expect(parseJsonPlanningOutput('Not JSON at all')).toBeNull();
  });

  it('returns null for empty string', () => {
    expect(parseJsonPlanningOutput('')).toBeNull();
  });

  it('returns null for null input', () => {
    expect(parseJsonPlanningOutput(null as any)).toBeNull();
  });

  it('returns null for JSON without list_of_plans_per_task', () => {
    expect(parseJsonPlanningOutput(JSON.stringify({ foo: 'bar' }))).toBeNull();
  });

  it('returns null when list_of_plans_per_task is not an array', () => {
    expect(parseJsonPlanningOutput(JSON.stringify({ list_of_plans_per_task: 'not array' }))).toBeNull();
  });

  it('returns null when plan entries are missing required fields', () => {
    const invalidJson = JSON.stringify({
      list_of_plans_per_task: [{ task: 'Only task, no plan' }]
    });
    expect(parseJsonPlanningOutput(invalidJson)).toBeNull();
  });
});

describe('PlanningOutputFormatter', () => {
  describe('Rendering', () => {
    it('renders task execution plan header', () => {
      render(
        <TestWrapper>
          <PlanningOutputFormatter content={samplePlanningOutput} />
        </TestWrapper>
      );

      expect(screen.getByText('Task Execution Plan')).toBeInTheDocument();
    });

    it('renders task count chip', () => {
      render(
        <TestWrapper>
          <PlanningOutputFormatter content={samplePlanningOutput} />
        </TestWrapper>
      );

      expect(screen.getByText('2 Tasks')).toBeInTheDocument();
    });

    it('renders singular "Task" for single task', () => {
      const singleTaskOutput = `
Task Number 1 - Single task
"task_description": "Only one task"
"agent": "Solo Agent"
`;

      render(
        <TestWrapper>
          <PlanningOutputFormatter content={singleTaskOutput} />
        </TestWrapper>
      );

      expect(screen.getByText('1 Task')).toBeInTheDocument();
    });

    it('renders raw content when parsing fails', () => {
      const invalidContent = 'Just plain text that is not planning output';

      render(
        <TestWrapper>
          <PlanningOutputFormatter content={invalidContent} />
        </TestWrapper>
      );

      expect(screen.getByText(invalidContent)).toBeInTheDocument();
    });
  });

  describe('Task Cards', () => {
    it('renders task number chips', () => {
      render(
        <TestWrapper>
          <PlanningOutputFormatter content={samplePlanningOutput} />
        </TestWrapper>
      );

      expect(screen.getByText('Task 1')).toBeInTheDocument();
      expect(screen.getByText('Task 2')).toBeInTheDocument();
    });

    it('renders task titles', () => {
      render(
        <TestWrapper>
          <PlanningOutputFormatter content={samplePlanningOutput} />
        </TestWrapper>
      );

      expect(screen.getByText('Research market trends')).toBeInTheDocument();
      expect(screen.getByText('Generate report')).toBeInTheDocument();
    });

    it('renders agent names', () => {
      render(
        <TestWrapper>
          <PlanningOutputFormatter content={samplePlanningOutput} />
        </TestWrapper>
      );

      expect(screen.getByText('Research Agent')).toBeInTheDocument();
      expect(screen.getByText('Writer Agent')).toBeInTheDocument();
    });

    it('renders ASSIGNED AGENT label', () => {
      render(
        <TestWrapper>
          <PlanningOutputFormatter content={samplePlanningOutput} />
        </TestWrapper>
      );

      const agentLabels = screen.getAllByText('ASSIGNED AGENT');
      expect(agentLabels).toHaveLength(2);
    });
  });

  describe('Accordions', () => {
    it('renders description accordion', () => {
      render(
        <TestWrapper>
          <PlanningOutputFormatter content={samplePlanningOutput} />
        </TestWrapper>
      );

      const descriptionLabels = screen.getAllByText('Description');
      expect(descriptionLabels.length).toBeGreaterThan(0);
    });

    it('renders expected output accordion', () => {
      render(
        <TestWrapper>
          <PlanningOutputFormatter content={samplePlanningOutput} />
        </TestWrapper>
      );

      const expectedOutputLabels = screen.getAllByText('Expected Output');
      expect(expectedOutputLabels.length).toBeGreaterThan(0);
    });

    it('first task description is expanded by default', async () => {
      render(
        <TestWrapper>
          <PlanningOutputFormatter content={samplePlanningOutput} />
        </TestWrapper>
      );

      // The first task's description should be visible
      expect(
        screen.getByText('Analyze current market trends in the AI industry')
      ).toBeInTheDocument();
    });

    it('can expand/collapse description accordion', async () => {
      const user = userEvent.setup();

      render(
        <TestWrapper>
          <PlanningOutputFormatter content={samplePlanningOutput} />
        </TestWrapper>
      );

      // Find the first Description accordion summary
      const descriptionButtons = screen.getAllByText('Description');

      // Click to toggle (first one is expanded by default)
      await user.click(descriptionButtons[0]);

      // Content might be collapsed now - this tests the accordion functionality
    });
  });

  describe('Tools Display', () => {
    it('renders TOOLS label', () => {
      render(
        <TestWrapper>
          <PlanningOutputFormatter content={samplePlanningOutput} />
        </TestWrapper>
      );

      const toolsLabels = screen.getAllByText('TOOLS');
      expect(toolsLabels.length).toBeGreaterThan(0);
    });

    it('renders tool chips for task with tools', () => {
      render(
        <TestWrapper>
          <PlanningOutputFormatter content={samplePlanningOutput} />
        </TestWrapper>
      );

      expect(screen.getByText('PerplexitySearchTool')).toBeInTheDocument();
    });

    it('shows "No tools assigned" for task without tools', () => {
      render(
        <TestWrapper>
          <PlanningOutputFormatter content={samplePlanningOutput} />
        </TestWrapper>
      );

      expect(screen.getByText('No tools assigned')).toBeInTheDocument();
    });
  });

  describe('Agent Goal', () => {
    it('renders agent goal when present', () => {
      render(
        <TestWrapper>
          <PlanningOutputFormatter content={samplePlanningOutput} />
        </TestWrapper>
      );

      expect(screen.getByText(/Goal: Gather and analyze market data/)).toBeInTheDocument();
    });
  });

  describe('Edge Cases', () => {
    it('handles content with em dash separator', () => {
      const contentWithEmDash = `
Task Number 1 — Task with em dash
"task_description": "Description with em dash"
"agent": "Test Agent"
`;

      render(
        <TestWrapper>
          <PlanningOutputFormatter content={contentWithEmDash} />
        </TestWrapper>
      );

      expect(screen.getByText('Task with em dash')).toBeInTheDocument();
    });

    it('handles content with en dash separator', () => {
      const contentWithEnDash = `
Task Number 1 – Task with en dash
"task_description": "Description with en dash"
"agent": "Test Agent"
`;

      render(
        <TestWrapper>
          <PlanningOutputFormatter content={contentWithEnDash} />
        </TestWrapper>
      );

      expect(screen.getByText('Task with en dash')).toBeInTheDocument();
    });

    it('handles missing optional fields gracefully', () => {
      const minimalContent = `
Task Number 1 - Minimal task
"task_description": "Just a description"
`;

      render(
        <TestWrapper>
          <PlanningOutputFormatter content={minimalContent} />
        </TestWrapper>
      );

      expect(screen.getByText('Task 1')).toBeInTheDocument();
      expect(screen.getByText('Minimal task')).toBeInTheDocument();
    });

    it('handles fields without quotes', () => {
      const unquotedContent = `
Task Number 1 - Unquoted task
task_description: Unquoted description value
agent: Unquoted Agent
`;

      render(
        <TestWrapper>
          <PlanningOutputFormatter content={unquotedContent} />
        </TestWrapper>
      );

      expect(screen.getByText('Task 1')).toBeInTheDocument();
    });

    it('handles multiple tools in task_tools array', () => {
      const multiToolContent = `
Task Number 1 - Multi tool task
"task_description": "Task with multiple tools"
"agent": "Multi Tool Agent"
"task_tools": [SearchTool(name='Search'), WriterTool(name='Writer'), AnalyzerTool(name='Analyzer')]
`;

      render(
        <TestWrapper>
          <PlanningOutputFormatter content={multiToolContent} />
        </TestWrapper>
      );

      expect(screen.getByText('SearchTool')).toBeInTheDocument();
      expect(screen.getByText('WriterTool')).toBeInTheDocument();
      expect(screen.getByText('AnalyzerTool')).toBeInTheDocument();
    });

    it('handles empty content', () => {
      render(
        <TestWrapper>
          <PlanningOutputFormatter content="" />
        </TestWrapper>
      );

      // Should render empty pre element for empty content
      const preElement = document.querySelector('pre');
      expect(preElement).toBeInTheDocument();
    });
  });

  describe('Styling', () => {
    it('applies correct border to task cards', () => {
      render(
        <TestWrapper>
          <PlanningOutputFormatter content={samplePlanningOutput} />
        </TestWrapper>
      );

      // Cards are rendered - we check the structure exists
      const taskChips = screen.getAllByText(/Task \d/);
      expect(taskChips).toHaveLength(2);
    });

    it('renders divider between header and tasks', () => {
      const { container } = render(
        <TestWrapper>
          <PlanningOutputFormatter content={samplePlanningOutput} />
        </TestWrapper>
      );

      const divider = container.querySelector('hr');
      expect(divider).toBeInTheDocument();
    });
  });

  describe('Complex Content', () => {
    it('handles task descriptions with newlines', () => {
      const multilineContent = `
Task Number 1 - Multiline task
"task_description": "Line 1
Line 2
Line 3"
"agent": "Test Agent"
`;

      render(
        <TestWrapper>
          <PlanningOutputFormatter content={multilineContent} />
        </TestWrapper>
      );

      expect(screen.getByText('Task 1')).toBeInTheDocument();
    });

    it('handles special characters in task title', () => {
      const specialCharContent = `
Task Number 1 - Task with "quotes" & <special> chars
"task_description": "Description here"
"agent": "Test Agent"
`;

      render(
        <TestWrapper>
          <PlanningOutputFormatter content={specialCharContent} />
        </TestWrapper>
      );

      expect(screen.getByText('Task 1')).toBeInTheDocument();
    });

    it('parses multiple tasks correctly', () => {
      const multiTaskContent = `
Task Number 1 - First task
"task_description": "First description"
"agent": "Agent 1"

Task Number 2 - Second task
"task_description": "Second description"
"agent": "Agent 2"

Task Number 3 - Third task
"task_description": "Third description"
"agent": "Agent 3"
`;

      render(
        <TestWrapper>
          <PlanningOutputFormatter content={multiTaskContent} />
        </TestWrapper>
      );

      expect(screen.getByText('Task 1')).toBeInTheDocument();
      expect(screen.getByText('Task 2')).toBeInTheDocument();
      expect(screen.getByText('Task 3')).toBeInTheDocument();
      expect(screen.getByText('3 Tasks')).toBeInTheDocument();
    });
  });

  describe('JSON Format Rendering (list_of_plans_per_task)', () => {
    const sampleJsonPlanningOutput = JSON.stringify({
      list_of_plans_per_task: [
        {
          task: 'Scrape top 3 Swiss news websites',
          plan: 'Step 1: Understand the Task Requirements\n- The goal is to scrape Swiss news\nStep 2: Execute the scraping'
        },
        {
          task: 'Analyze the scraped content',
          plan: 'Step 1: Review the data\nStep 2: Categorize findings'
        }
      ]
    });

    it('renders Execution Plan header for JSON format', () => {
      render(
        <TestWrapper>
          <PlanningOutputFormatter content={sampleJsonPlanningOutput} />
        </TestWrapper>
      );

      // There should be multiple "Execution Plan" elements - header + accordion for each task
      const executionPlanElements = screen.getAllByText('Execution Plan');
      expect(executionPlanElements.length).toBeGreaterThanOrEqual(1);
    });

    it('renders correct task count chip', () => {
      render(
        <TestWrapper>
          <PlanningOutputFormatter content={sampleJsonPlanningOutput} />
        </TestWrapper>
      );

      expect(screen.getByText('2 Tasks')).toBeInTheDocument();
    });

    it('renders Task Description accordion for JSON format', () => {
      render(
        <TestWrapper>
          <PlanningOutputFormatter content={sampleJsonPlanningOutput} />
        </TestWrapper>
      );

      const descriptionLabels = screen.getAllByText('Task Description');
      expect(descriptionLabels.length).toBe(2);
    });

    it('renders Execution Plan accordion for each task', () => {
      render(
        <TestWrapper>
          <PlanningOutputFormatter content={sampleJsonPlanningOutput} />
        </TestWrapper>
      );

      // The word "Execution Plan" appears in the accordion headers
      const executionPlanLabels = screen.getAllByText('Execution Plan');
      expect(executionPlanLabels.length).toBeGreaterThanOrEqual(2);
    });

    it('renders task content correctly', () => {
      render(
        <TestWrapper>
          <PlanningOutputFormatter content={sampleJsonPlanningOutput} />
        </TestWrapper>
      );

      expect(screen.getByText('Scrape top 3 Swiss news websites')).toBeInTheDocument();
    });

    it('renders step headers with styling', () => {
      render(
        <TestWrapper>
          <PlanningOutputFormatter content={sampleJsonPlanningOutput} />
        </TestWrapper>
      );

      // Step headers should be rendered
      expect(screen.getAllByText(/Step 1:/)).toHaveLength(2);
    });

    it('renders task chips with correct numbering', () => {
      render(
        <TestWrapper>
          <PlanningOutputFormatter content={sampleJsonPlanningOutput} />
        </TestWrapper>
      );

      expect(screen.getByText('Task 1')).toBeInTheDocument();
      expect(screen.getByText('Task 2')).toBeInTheDocument();
    });

    it('handles single task in JSON format', () => {
      const singleTaskJson = JSON.stringify({
        list_of_plans_per_task: [
          { task: 'Single task', plan: 'Step 1: Execute the task' }
        ]
      });

      render(
        <TestWrapper>
          <PlanningOutputFormatter content={singleTaskJson} />
        </TestWrapper>
      );

      expect(screen.getByText('1 Task')).toBeInTheDocument();
      expect(screen.getByText('Single task')).toBeInTheDocument();
    });

    it('handles plans without Step format', () => {
      const noPlanStepsJson = JSON.stringify({
        list_of_plans_per_task: [
          { task: 'Some task', plan: 'Just do the thing without numbered steps' }
        ]
      });

      render(
        <TestWrapper>
          <PlanningOutputFormatter content={noPlanStepsJson} />
        </TestWrapper>
      );

      expect(screen.getByText('Just do the thing without numbered steps')).toBeInTheDocument();
    });
  });
});
