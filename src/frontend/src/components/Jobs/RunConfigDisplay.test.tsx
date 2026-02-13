/**
 * Tests for Run Configuration extraction and display.
 *
 * Covers:
 * - RunConfig extraction from trace_metadata in crew_started/execution_started events
 * - RunConfig button visibility (shown only when config exists)
 * - RunConfig dialog rendering with agents, tasks, and inputs
 * - Edge cases: missing fields, empty arrays, no crew_inputs
 */
import React from 'react';
import { render, screen, fireEvent, within, waitFor } from '@testing-library/react';
import { describe, it, expect, vi } from 'vitest';
import TraceTimelineContent, { TraceTimelineContentProps } from './TraceTimelineContent';
import { RunConfig, RunConfigAgent, RunConfigTask, ProcessedTraces } from '../../types/trace';

/**
 * Test the RunConfig extraction logic used in processTraces (useTraceData hook).
 * This replicates the extraction logic to verify correctness in isolation.
 */
describe('RunConfig Extraction Logic', () => {
  interface MockTrace {
    trace_metadata?: Record<string, unknown> | null;
    event_type?: string;
    event_source?: string;
  }

  /**
   * Replicates the extraction logic from processTraces in useTraceData.ts.
   */
  const extractRunConfig = (sorted: MockTrace[]): RunConfig | undefined => {
    let runConfig: RunConfig | undefined;
    for (const trace of sorted) {
      if (trace.trace_metadata && typeof trace.trace_metadata === 'object') {
        const meta = trace.trace_metadata as Record<string, unknown>;
        if (meta.crew_agents && Array.isArray(meta.crew_agents)) {
          runConfig = {
            crew_key: meta.crew_key as string | undefined,
            crew_id: meta.crew_id as string | undefined,
            crew_agents: meta.crew_agents as RunConfigAgent[],
            crew_tasks: (meta.crew_tasks || []) as RunConfigTask[],
            crew_inputs: meta.crew_inputs as Record<string, unknown> | undefined,
          };
          break;
        }
      }
    }
    return runConfig;
  };

  describe('Successful extraction', () => {
    it('should extract runConfig from a crew_started trace with crew_agents', () => {
      const traces: MockTrace[] = [
        {
          event_type: 'crew_started',
          event_source: 'crew',
          trace_metadata: {
            crew_key: 'my_crew',
            crew_id: 'abc-123',
            crew_agents: [
              { key: 'agent1', id: 'a1', role: 'Researcher', goal: 'Find info', backstory: 'Expert', tools_names: ['search'] },
            ],
            crew_tasks: [
              { id: 't1', description: 'Research topic', expected_output: 'Report', agent_role: 'Researcher', agent_key: 'agent1' },
            ],
            crew_inputs: { run_name: 'Test Run', topic: 'AI' },
          },
        },
      ];

      const result = extractRunConfig(traces);
      expect(result).toBeDefined();
      expect(result!.crew_key).toBe('my_crew');
      expect(result!.crew_id).toBe('abc-123');
      expect(result!.crew_agents).toHaveLength(1);
      expect(result!.crew_agents[0].role).toBe('Researcher');
      expect(result!.crew_tasks).toHaveLength(1);
      expect(result!.crew_tasks[0].description).toBe('Research topic');
      expect(result!.crew_inputs?.run_name).toBe('Test Run');
      expect(result!.crew_inputs?.topic).toBe('AI');
    });

    it('should extract from the first matching trace', () => {
      const traces: MockTrace[] = [
        { event_type: 'some_event', trace_metadata: { foo: 'bar' } },
        {
          event_type: 'crew_started',
          trace_metadata: {
            crew_key: 'first_crew',
            crew_agents: [{ key: 'a1', id: '1', role: 'First', goal: 'g', backstory: 'b' }],
            crew_tasks: [],
          },
        },
        {
          event_type: 'crew_started',
          trace_metadata: {
            crew_key: 'second_crew',
            crew_agents: [{ key: 'a2', id: '2', role: 'Second', goal: 'g', backstory: 'b' }],
            crew_tasks: [],
          },
        },
      ];

      const result = extractRunConfig(traces);
      expect(result).toBeDefined();
      expect(result!.crew_key).toBe('first_crew');
    });

    it('should handle missing crew_tasks gracefully', () => {
      const traces: MockTrace[] = [
        {
          event_type: 'crew_started',
          trace_metadata: {
            crew_agents: [{ key: 'a1', id: '1', role: 'Agent', goal: 'g', backstory: 'b' }],
          },
        },
      ];

      const result = extractRunConfig(traces);
      expect(result).toBeDefined();
      expect(result!.crew_tasks).toEqual([]);
    });

    it('should handle missing crew_inputs gracefully', () => {
      const traces: MockTrace[] = [
        {
          event_type: 'crew_started',
          trace_metadata: {
            crew_agents: [{ key: 'a1', id: '1', role: 'Agent', goal: 'g', backstory: 'b' }],
            crew_tasks: [],
          },
        },
      ];

      const result = extractRunConfig(traces);
      expect(result).toBeDefined();
      expect(result!.crew_inputs).toBeUndefined();
    });

    it('should handle missing crew_key and crew_id', () => {
      const traces: MockTrace[] = [
        {
          event_type: 'crew_started',
          trace_metadata: {
            crew_agents: [{ key: 'a1', id: '1', role: 'Agent', goal: 'g', backstory: 'b' }],
            crew_tasks: [],
          },
        },
      ];

      const result = extractRunConfig(traces);
      expect(result).toBeDefined();
      expect(result!.crew_key).toBeUndefined();
      expect(result!.crew_id).toBeUndefined();
    });
  });

  describe('No extraction scenarios', () => {
    it('should return undefined for empty traces', () => {
      expect(extractRunConfig([])).toBeUndefined();
    });

    it('should return undefined when no trace has crew_agents', () => {
      const traces: MockTrace[] = [
        { event_type: 'llm_call', trace_metadata: { model: 'gpt-4' } },
        { event_type: 'task_started', trace_metadata: { task_name: 'Research' } },
      ];

      expect(extractRunConfig(traces)).toBeUndefined();
    });

    it('should return undefined when trace_metadata is null', () => {
      const traces: MockTrace[] = [
        { event_type: 'crew_started', trace_metadata: null },
      ];

      expect(extractRunConfig(traces)).toBeUndefined();
    });

    it('should return undefined when crew_agents is not an array', () => {
      const traces: MockTrace[] = [
        {
          event_type: 'crew_started',
          trace_metadata: { crew_agents: 'not_an_array' },
        },
      ];

      expect(extractRunConfig(traces)).toBeUndefined();
    });

    it('should return undefined when trace_metadata is missing', () => {
      const traces: MockTrace[] = [
        { event_type: 'crew_started' },
      ];

      expect(extractRunConfig(traces)).toBeUndefined();
    });
  });

  describe('Multiple agents and tasks', () => {
    it('should extract multiple agents', () => {
      const traces: MockTrace[] = [
        {
          event_type: 'crew_started',
          trace_metadata: {
            crew_agents: [
              { key: 'agent1', id: 'a1', role: 'Researcher', goal: 'Research', backstory: 'Expert' },
              { key: 'agent2', id: 'a2', role: 'Writer', goal: 'Write', backstory: 'Author' },
              { key: 'agent3', id: 'a3', role: 'Editor', goal: 'Edit', backstory: 'Reviewer' },
            ],
            crew_tasks: [
              { id: 't1', description: 'Task 1', expected_output: 'Output 1', agent_role: 'Researcher', agent_key: 'agent1' },
              { id: 't2', description: 'Task 2', expected_output: 'Output 2', agent_role: 'Writer', agent_key: 'agent2' },
            ],
          },
        },
      ];

      const result = extractRunConfig(traces);
      expect(result!.crew_agents).toHaveLength(3);
      expect(result!.crew_tasks).toHaveLength(2);
    });

    it('should preserve agent tool names', () => {
      const traces: MockTrace[] = [
        {
          event_type: 'crew_started',
          trace_metadata: {
            crew_agents: [
              {
                key: 'agent1', id: 'a1', role: 'Researcher', goal: 'g', backstory: 'b',
                tools_names: ['search_tool', 'web_scraper', 'file_reader'],
                delegation_enabled: true,
                max_iter: 15,
                max_rpm: 10,
              },
            ],
            crew_tasks: [],
          },
        },
      ];

      const result = extractRunConfig(traces);
      const agent = result!.crew_agents[0];
      expect(agent.tools_names).toEqual(['search_tool', 'web_scraper', 'file_reader']);
      expect(agent.delegation_enabled).toBe(true);
      expect(agent.max_iter).toBe(15);
      expect(agent.max_rpm).toBe(10);
    });

    it('should preserve task context and flags', () => {
      const traces: MockTrace[] = [
        {
          event_type: 'crew_started',
          trace_metadata: {
            crew_agents: [{ key: 'a1', id: '1', role: 'Agent', goal: 'g', backstory: 'b' }],
            crew_tasks: [
              {
                id: 't1', description: 'Task', expected_output: 'Out',
                agent_role: 'Agent', agent_key: 'a1',
                async_execution: true, human_input: false,
                tools_names: ['tool_a'], context: ['t0'],
              },
            ],
          },
        },
      ];

      const result = extractRunConfig(traces);
      const task = result!.crew_tasks[0];
      expect(task.async_execution).toBe(true);
      expect(task.human_input).toBe(false);
      expect(task.tools_names).toEqual(['tool_a']);
      expect(task.context).toEqual(['t0']);
    });
  });
});

/**
 * Test the TraceTimelineContent component's Run Configuration button and dialog.
 */
describe('TraceTimelineContent RunConfig UI', () => {
  const mockRunConfig: RunConfig = {
    crew_key: 'test_crew',
    crew_id: 'crew-uuid-12345678',
    crew_agents: [
      {
        key: 'researcher',
        id: 'agent-1',
        role: 'Senior Researcher',
        goal: 'Find comprehensive information on the given topic',
        backstory: 'An experienced researcher with decades of expertise',
        delegation_enabled: true,
        max_iter: 15,
        max_rpm: 10,
        tools_names: ['search_tool', 'web_scraper'],
      },
      {
        key: 'writer',
        id: 'agent-2',
        role: 'Content Writer',
        goal: 'Create well-structured content',
        backstory: 'A professional writer',
        tools_names: ['file_writer'],
      },
    ],
    crew_tasks: [
      {
        id: 'task-1',
        description: 'Research the given topic thoroughly',
        expected_output: 'A detailed research report',
        agent_role: 'Senior Researcher',
        agent_key: 'researcher',
        tools_names: ['search_tool'],
        async_execution: false,
        human_input: false,
        context: null,
      },
      {
        id: 'task-2',
        description: 'Write an article based on the research',
        expected_output: 'A polished article',
        agent_role: 'Content Writer',
        agent_key: 'writer',
        async_execution: true,
        human_input: true,
        tools_names: [],
        context: ['task-1'],
      },
    ],
    crew_inputs: { run_name: 'My Test Run', topic: 'Artificial Intelligence' },
  };

  const baseProcessedTraces: ProcessedTraces = {
    globalStart: new Date('2024-01-01T00:00:00Z'),
    globalEnd: new Date('2024-01-01T00:05:00Z'),
    totalDuration: 300000,
    agents: [],
    globalEvents: { start: [], end: [] },
    crewPlanningEvents: [],
  };

  const baseProps: TraceTimelineContentProps = {
    processedTraces: baseProcessedTraces,
    loading: false,
    error: null,
    viewMode: 'summary',
    setViewMode: vi.fn(),
    expandedAgents: new Set<number>(),
    expandedTasks: new Set<string>(),
    toggleAgent: vi.fn(),
    toggleTask: vi.fn(),
    selectedEvent: null,
    setSelectedEvent: vi.fn(),
    handleEventClick: vi.fn(),
    selectedTaskDescription: null,
    setSelectedTaskDescription: vi.fn(),
    handleTaskDescriptionClick: vi.fn(),
    formatDuration: (ms: number) => `${(ms / 1000).toFixed(1)}s`,
    formatTimeDelta: (_start: Date, _ts: Date) => '+0s',
    truncateTaskName: (name: string) => name,
  };

  describe('Run Config button visibility', () => {
    it('should show Run Config button when runConfig is provided', () => {
      render(<TraceTimelineContent {...baseProps} runConfig={mockRunConfig} />);
      expect(screen.getByRole('button', { name: /run config/i })).toBeInTheDocument();
    });

    it('should show Run Config button when processedTraces has runConfig', () => {
      const tracesWithConfig: ProcessedTraces = {
        ...baseProcessedTraces,
        runConfig: mockRunConfig,
      };
      render(<TraceTimelineContent {...baseProps} processedTraces={tracesWithConfig} />);
      expect(screen.getByRole('button', { name: /run config/i })).toBeInTheDocument();
    });

    it('should NOT show Run Config button when runConfig is absent', () => {
      render(<TraceTimelineContent {...baseProps} />);
      expect(screen.queryByRole('button', { name: /run config/i })).not.toBeInTheDocument();
    });

    it('should NOT show Run Config button when processedTraces is null', () => {
      render(<TraceTimelineContent {...baseProps} processedTraces={null} />);
      expect(screen.queryByRole('button', { name: /run config/i })).not.toBeInTheDocument();
    });
  });

  describe('Run Config dialog', () => {
    it('should open dialog when Run Config button is clicked', () => {
      render(<TraceTimelineContent {...baseProps} runConfig={mockRunConfig} />);

      const button = screen.getByRole('button', { name: /run config/i });
      fireEvent.click(button);

      expect(screen.getByText('Run Configuration')).toBeInTheDocument();
    });

    it('should display the run name from crew_inputs', () => {
      render(<TraceTimelineContent {...baseProps} runConfig={mockRunConfig} />);
      fireEvent.click(screen.getByRole('button', { name: /run config/i }));

      expect(screen.getByText('My Test Run')).toBeInTheDocument();
    });

    it('should display crew key and crew ID', () => {
      render(<TraceTimelineContent {...baseProps} runConfig={mockRunConfig} />);
      fireEvent.click(screen.getByRole('button', { name: /run config/i }));

      expect(screen.getByText(/Crew: test_crew/)).toBeInTheDocument();
      expect(screen.getByText(/ID: crew-uui\.\.\./)).toBeInTheDocument();
    });

    it('should display all agents with their roles', () => {
      render(<TraceTimelineContent {...baseProps} runConfig={mockRunConfig} />);
      fireEvent.click(screen.getByRole('button', { name: /run config/i }));

      expect(screen.getByText('Agents (2)')).toBeInTheDocument();
      // Roles appear in both agent cards and task cards, so use getAllByText
      expect(screen.getAllByText('Senior Researcher').length).toBeGreaterThanOrEqual(1);
      expect(screen.getAllByText('Content Writer').length).toBeGreaterThanOrEqual(1);
    });

    it('should display agent goals', () => {
      render(<TraceTimelineContent {...baseProps} runConfig={mockRunConfig} />);
      fireEvent.click(screen.getByRole('button', { name: /run config/i }));

      expect(screen.getByText('Find comprehensive information on the given topic')).toBeInTheDocument();
      expect(screen.getByText('Create well-structured content')).toBeInTheDocument();
    });

    it('should display agent backstory', () => {
      render(<TraceTimelineContent {...baseProps} runConfig={mockRunConfig} />);
      fireEvent.click(screen.getByRole('button', { name: /run config/i }));

      expect(screen.getByText('An experienced researcher with decades of expertise')).toBeInTheDocument();
      expect(screen.getByText('A professional writer')).toBeInTheDocument();
    });

    it('should display agent tools', () => {
      render(<TraceTimelineContent {...baseProps} runConfig={mockRunConfig} />);
      fireEvent.click(screen.getByRole('button', { name: /run config/i }));

      // Tools may appear in both agent cards and task cards
      expect(screen.getAllByText('search_tool').length).toBeGreaterThanOrEqual(1);
      expect(screen.getAllByText('web_scraper').length).toBeGreaterThanOrEqual(1);
      expect(screen.getAllByText('file_writer').length).toBeGreaterThanOrEqual(1);
    });

    it('should display agent delegation and limits', () => {
      render(<TraceTimelineContent {...baseProps} runConfig={mockRunConfig} />);
      fireEvent.click(screen.getByRole('button', { name: /run config/i }));

      expect(screen.getByText('Delegation')).toBeInTheDocument();
      expect(screen.getByText('Max Iter: 15')).toBeInTheDocument();
      expect(screen.getByText('Max RPM: 10')).toBeInTheDocument();
    });

    it('should display all tasks with descriptions', () => {
      render(<TraceTimelineContent {...baseProps} runConfig={mockRunConfig} />);
      fireEvent.click(screen.getByRole('button', { name: /run config/i }));

      expect(screen.getByText('Tasks (2)')).toBeInTheDocument();
      expect(screen.getByText('Research the given topic thoroughly')).toBeInTheDocument();
      expect(screen.getByText('Write an article based on the research')).toBeInTheDocument();
    });

    it('should display task expected outputs', () => {
      render(<TraceTimelineContent {...baseProps} runConfig={mockRunConfig} />);
      fireEvent.click(screen.getByRole('button', { name: /run config/i }));

      expect(screen.getByText('A detailed research report')).toBeInTheDocument();
      expect(screen.getByText('A polished article')).toBeInTheDocument();
    });

    it('should display task agent assignments', () => {
      render(<TraceTimelineContent {...baseProps} runConfig={mockRunConfig} />);
      fireEvent.click(screen.getByRole('button', { name: /run config/i }));

      // Agent roles in task cards
      const dialog = screen.getByText('Run Configuration').closest('[role="dialog"]')!;
      const taskSection = within(dialog).getByText('Tasks (2)').parentElement!;
      expect(within(taskSection).getAllByText('Senior Researcher')).toHaveLength(1);
      expect(within(taskSection).getAllByText('Content Writer')).toHaveLength(1);
    });

    it('should display task async and human input flags', () => {
      render(<TraceTimelineContent {...baseProps} runConfig={mockRunConfig} />);
      fireEvent.click(screen.getByRole('button', { name: /run config/i }));

      expect(screen.getByText('Async')).toBeInTheDocument();
      expect(screen.getByText('Human Input')).toBeInTheDocument();
    });

    it('should display task context references', () => {
      render(<TraceTimelineContent {...baseProps} runConfig={mockRunConfig} />);
      fireEvent.click(screen.getByRole('button', { name: /run config/i }));

      expect(screen.getByText('task-1')).toBeInTheDocument();
    });

    it('should display additional crew inputs (excluding run_name)', () => {
      render(<TraceTimelineContent {...baseProps} runConfig={mockRunConfig} />);
      fireEvent.click(screen.getByRole('button', { name: /run config/i }));

      expect(screen.getByText('Crew Inputs')).toBeInTheDocument();
      // The topic input should be displayed in the JSON
      expect(screen.getByText(/"topic": "Artificial Intelligence"/)).toBeInTheDocument();
    });

    it('should close dialog when Close button is clicked', async () => {
      render(<TraceTimelineContent {...baseProps} runConfig={mockRunConfig} />);
      fireEvent.click(screen.getByRole('button', { name: /run config/i }));

      expect(screen.getByText('Run Configuration')).toBeInTheDocument();

      // Click the Close button in dialog actions
      const closeButtons = screen.getAllByRole('button', { name: /close/i });
      const dialogCloseButton = closeButtons[closeButtons.length - 1];
      fireEvent.click(dialogCloseButton);

      await waitFor(() => {
        expect(screen.queryByRole('dialog', { name: /run configuration/i })).not.toBeInTheDocument();
      });
    });

    it('should close dialog when X icon is clicked', async () => {
      render(<TraceTimelineContent {...baseProps} runConfig={mockRunConfig} />);
      fireEvent.click(screen.getByRole('button', { name: /run config/i }));

      expect(screen.getByText('Run Configuration')).toBeInTheDocument();

      // Click the X icon button in the dialog title
      const dialog = screen.getByText('Run Configuration').closest('[role="dialog"]')!;
      const titleArea = within(dialog).getByText('Run Configuration').closest('.MuiDialogTitle-root')!;
      const iconButton = within(titleArea).getByRole('button');
      fireEvent.click(iconButton);

      await waitFor(() => {
        expect(screen.queryByRole('dialog', { name: /run configuration/i })).not.toBeInTheDocument();
      });
    });
  });

  describe('RunConfig edge cases', () => {
    it('should handle runConfig with no crew_inputs', () => {
      const configNoInputs: RunConfig = {
        crew_agents: [{ key: 'a1', id: '1', role: 'Agent', goal: 'Goal', backstory: 'Story' }],
        crew_tasks: [],
      };

      render(<TraceTimelineContent {...baseProps} runConfig={configNoInputs} />);
      fireEvent.click(screen.getByRole('button', { name: /run config/i }));

      expect(screen.queryByText('Run Name')).not.toBeInTheDocument();
      expect(screen.queryByText('Crew Inputs')).not.toBeInTheDocument();
    });

    it('should handle runConfig with empty agents array', () => {
      const configNoAgents: RunConfig = {
        crew_agents: [],
        crew_tasks: [{ id: 't1', description: 'Task', expected_output: 'Out', agent_role: 'Agent', agent_key: 'a1' }],
      };

      render(<TraceTimelineContent {...baseProps} runConfig={configNoAgents} />);
      fireEvent.click(screen.getByRole('button', { name: /run config/i }));

      expect(screen.queryByText(/Agents \(/)).not.toBeInTheDocument();
      expect(screen.getByText('Tasks (1)')).toBeInTheDocument();
    });

    it('should handle runConfig with empty tasks array', () => {
      const configNoTasks: RunConfig = {
        crew_agents: [{ key: 'a1', id: '1', role: 'Agent', goal: 'Goal', backstory: 'Story' }],
        crew_tasks: [],
      };

      render(<TraceTimelineContent {...baseProps} runConfig={configNoTasks} />);
      fireEvent.click(screen.getByRole('button', { name: /run config/i }));

      expect(screen.getByText('Agents (1)')).toBeInTheDocument();
      expect(screen.queryByText(/Tasks \(/)).not.toBeInTheDocument();
    });

    it('should handle runConfig with only run_name in crew_inputs (no extra inputs section)', () => {
      const configOnlyRunName: RunConfig = {
        crew_agents: [{ key: 'a1', id: '1', role: 'Agent', goal: 'Goal', backstory: 'Story' }],
        crew_tasks: [],
        crew_inputs: { run_name: 'Only Run Name' },
      };

      render(<TraceTimelineContent {...baseProps} runConfig={configOnlyRunName} />);
      fireEvent.click(screen.getByRole('button', { name: /run config/i }));

      expect(screen.getByText('Only Run Name')).toBeInTheDocument();
      expect(screen.queryByText('Crew Inputs')).not.toBeInTheDocument();
    });

    it('should handle agent without tools_names', () => {
      const configNoTools: RunConfig = {
        crew_agents: [{ key: 'a1', id: '1', role: 'Agent Without Tools', goal: 'Goal', backstory: 'Story' }],
        crew_tasks: [],
      };

      render(<TraceTimelineContent {...baseProps} runConfig={configNoTools} />);
      fireEvent.click(screen.getByRole('button', { name: /run config/i }));

      expect(screen.getByText('Agent Without Tools')).toBeInTheDocument();
    });

    it('should handle agent with empty tools_names array', () => {
      const configEmptyTools: RunConfig = {
        crew_agents: [{ key: 'a1', id: '1', role: 'Agent', goal: 'Goal', backstory: 'Story', tools_names: [] }],
        crew_tasks: [],
      };

      render(<TraceTimelineContent {...baseProps} runConfig={configEmptyTools} />);
      fireEvent.click(screen.getByRole('button', { name: /run config/i }));

      // Should not render Tools section for empty array
      expect(screen.getByText('Agent')).toBeInTheDocument();
    });

    it('should handle task with null context', () => {
      const configNullContext: RunConfig = {
        crew_agents: [],
        crew_tasks: [
          { id: 't1', description: 'Task', expected_output: 'Out', agent_role: 'Agent', agent_key: 'a1', context: null },
        ],
      };

      render(<TraceTimelineContent {...baseProps} runConfig={configNullContext} />);
      fireEvent.click(screen.getByRole('button', { name: /run config/i }));

      expect(screen.queryByText('Context Tasks')).not.toBeInTheDocument();
    });

    it('should handle task with empty context array', () => {
      const configEmptyContext: RunConfig = {
        crew_agents: [],
        crew_tasks: [
          { id: 't1', description: 'Task', expected_output: 'Out', agent_role: 'Agent', agent_key: 'a1', context: [] },
        ],
      };

      render(<TraceTimelineContent {...baseProps} runConfig={configEmptyContext} />);
      fireEvent.click(screen.getByRole('button', { name: /run config/i }));

      expect(screen.queryByText('Context Tasks')).not.toBeInTheDocument();
    });

    it('should prefer prop runConfig over processedTraces runConfig', () => {
      const propConfig: RunConfig = {
        crew_key: 'prop_crew',
        crew_agents: [{ key: 'a1', id: '1', role: 'Prop Agent', goal: 'g', backstory: 'b' }],
        crew_tasks: [],
      };

      const tracesConfig: RunConfig = {
        crew_key: 'traces_crew',
        crew_agents: [{ key: 'a2', id: '2', role: 'Traces Agent', goal: 'g', backstory: 'b' }],
        crew_tasks: [],
      };

      const tracesWithConfig: ProcessedTraces = {
        ...baseProcessedTraces,
        runConfig: tracesConfig,
      };

      render(<TraceTimelineContent {...baseProps} processedTraces={tracesWithConfig} runConfig={propConfig} />);
      fireEvent.click(screen.getByRole('button', { name: /run config/i }));

      expect(screen.getByText(/Crew: prop_crew/)).toBeInTheDocument();
      expect(screen.getByText('Prop Agent')).toBeInTheDocument();
    });

    it('should handle runConfig without crew_key and crew_id', () => {
      const configNoIds: RunConfig = {
        crew_agents: [{ key: 'a1', id: '1', role: 'Agent', goal: 'Goal', backstory: 'Story' }],
        crew_tasks: [],
      };

      render(<TraceTimelineContent {...baseProps} runConfig={configNoIds} />);
      fireEvent.click(screen.getByRole('button', { name: /run config/i }));

      expect(screen.queryByText(/Crew:/)).not.toBeInTheDocument();
      expect(screen.queryByText(/ID:/)).not.toBeInTheDocument();
    });
  });
});
