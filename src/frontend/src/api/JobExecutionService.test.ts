import { vi, Mock, beforeEach, afterEach, describe, it, expect } from 'vitest';
import { JobExecutionService, JobResponse } from './JobExecutionService';
import { apiClient } from '../config/api/ApiConfig';
import { ModelService } from './ModelService';
import { Node, Edge } from 'reactflow';
import { AgentYaml } from '../types/crew';

// Mock dependencies
vi.mock('../config/api/ApiConfig', () => ({
  apiClient: {
    get: vi.fn(),
    post: vi.fn(),
    put: vi.fn(),
    delete: vi.fn(),
    patch: vi.fn(),
  },
}));

vi.mock('./ModelService', () => ({
  ModelService: {
    getInstance: vi.fn(() => ({
      getActiveModels: vi.fn().mockResolvedValue({}),
      getActiveModelsSync: vi.fn().mockReturnValue({}),
    })),
  },
}));

vi.mock('../utils/flowConfigBuilder', () => ({
  buildFlowConfiguration: vi.fn(() => ({
    listeners: [],
    actions: [],
    startingPoints: [],
  })),
}));

// Mock axios for isAxiosError
vi.mock('axios', () => ({
  default: {
    isAxiosError: vi.fn((error: unknown) => {
      return error !== null && typeof error === 'object' && 'isAxiosError' in error;
    }),
  },
  isAxiosError: vi.fn((error: unknown) => {
    return error !== null && typeof error === 'object' && 'isAxiosError' in error;
  }),
}));

describe('JobExecutionService', () => {
  let service: JobExecutionService;

  beforeEach(() => {
    vi.clearAllMocks();
    service = new JobExecutionService();
  });

  afterEach(() => {
    vi.resetAllMocks();
  });

  // ===========================================================================
  // Test Data Factories
  // ===========================================================================

  const createMockAgentNode = (
    id: string,
    overrides: Partial<Node['data']> = {}
  ): Node => ({
    id,
    type: 'agentNode',
    position: { x: 0, y: 0 },
    data: {
      role: 'Test Agent',
      goal: 'Test goal',
      backstory: 'Test backstory',
      tools: [],
      ...overrides,
    },
  });

  const createMockTaskNode = (
    id: string,
    overrides: Partial<Node['data']> = {}
  ): Node => ({
    id,
    type: 'taskNode',
    position: { x: 100, y: 0 },
    data: {
      description: 'Test task description',
      expected_output: 'Test expected output',
      tools: [],
      config: {},
      ...overrides,
    },
  });

  const createMockEdge = (source: string, target: string): Edge => ({
    id: `edge-${source}-${target}`,
    source,
    target,
  });

  const createMockJobResponse = (
    overrides: Partial<JobResponse> = {}
  ): JobResponse => ({
    job_id: 'test-job-123',
    execution_id: 'exec-456',
    status: 'running',
    created_at: '2024-01-01T00:00:00Z',
    result: {
      output: 'Test output',
      task_results: [],
    },
    error: null,
    ...overrides,
  });

  // ===========================================================================
  // inject_date and date_format Tests
  // ===========================================================================

  describe('inject_date field handling', () => {
    it('should default inject_date to true when not provided in agentData', async () => {
      const agentNode = createMockAgentNode('agent-1', {
        role: 'Researcher',
        goal: 'Research topics',
        backstory: 'Expert researcher',
        // inject_date is NOT provided - should default to true
      });
      const taskNode = createMockTaskNode('task-1');
      const edge = createMockEdge('agent-1', 'task-1');

      const mockResponse = createMockJobResponse();
      (apiClient.post as Mock).mockResolvedValue({ data: mockResponse });

      await service.executeJob([agentNode, taskNode], [edge]);

      expect(apiClient.post).toHaveBeenCalledTimes(1);
      const callArgs = (apiClient.post as Mock).mock.calls[0];
      const config = callArgs[1];

      // Verify inject_date defaults to true
      expect(config.agents_yaml['agent_agent-1'].inject_date).toBe(true);
    });

    it('should set inject_date to true when explicitly set to true in agentData', async () => {
      const agentNode = createMockAgentNode('agent-2', {
        role: 'Analyst',
        goal: 'Analyze data',
        backstory: 'Data analyst',
        inject_date: true,
      });
      const taskNode = createMockTaskNode('task-2');
      const edge = createMockEdge('agent-2', 'task-2');

      const mockResponse = createMockJobResponse();
      (apiClient.post as Mock).mockResolvedValue({ data: mockResponse });

      await service.executeJob([agentNode, taskNode], [edge]);

      expect(apiClient.post).toHaveBeenCalledTimes(1);
      const callArgs = (apiClient.post as Mock).mock.calls[0];
      const config = callArgs[1];

      expect(config.agents_yaml['agent_agent-2'].inject_date).toBe(true);
    });

    it('should set inject_date to false when explicitly set to false in agentData', async () => {
      const agentNode = createMockAgentNode('agent-3', {
        role: 'Writer',
        goal: 'Write content',
        backstory: 'Content writer',
        inject_date: false,
      });
      const taskNode = createMockTaskNode('task-3');
      const edge = createMockEdge('agent-3', 'task-3');

      const mockResponse = createMockJobResponse();
      (apiClient.post as Mock).mockResolvedValue({ data: mockResponse });

      await service.executeJob([agentNode, taskNode], [edge]);

      expect(apiClient.post).toHaveBeenCalledTimes(1);
      const callArgs = (apiClient.post as Mock).mock.calls[0];
      const config = callArgs[1];

      expect(config.agents_yaml['agent_agent-3'].inject_date).toBe(false);
    });

    it('should handle inject_date with undefined value and default to true', async () => {
      const agentNode = createMockAgentNode('agent-4', {
        role: 'Editor',
        goal: 'Edit documents',
        backstory: 'Document editor',
        inject_date: undefined,
      });
      const taskNode = createMockTaskNode('task-4');
      const edge = createMockEdge('agent-4', 'task-4');

      const mockResponse = createMockJobResponse();
      (apiClient.post as Mock).mockResolvedValue({ data: mockResponse });

      await service.executeJob([agentNode, taskNode], [edge]);

      expect(apiClient.post).toHaveBeenCalledTimes(1);
      const callArgs = (apiClient.post as Mock).mock.calls[0];
      const config = callArgs[1];

      // undefined ?? true should result in true
      expect(config.agents_yaml['agent_agent-4'].inject_date).toBe(true);
    });

    it('should handle inject_date with null value and default to true', async () => {
      const agentNode = createMockAgentNode('agent-5', {
        role: 'Reviewer',
        goal: 'Review content',
        backstory: 'Content reviewer',
        inject_date: null as unknown as boolean,
      });
      const taskNode = createMockTaskNode('task-5');
      const edge = createMockEdge('agent-5', 'task-5');

      const mockResponse = createMockJobResponse();
      (apiClient.post as Mock).mockResolvedValue({ data: mockResponse });

      await service.executeJob([agentNode, taskNode], [edge]);

      expect(apiClient.post).toHaveBeenCalledTimes(1);
      const callArgs = (apiClient.post as Mock).mock.calls[0];
      const config = callArgs[1];

      // null ?? true should result in true
      expect(config.agents_yaml['agent_agent-5'].inject_date).toBe(true);
    });
  });

  describe('date_format field handling', () => {
    it('should include date_format when provided in agentData', async () => {
      const agentNode = createMockAgentNode('agent-6', {
        role: 'Reporter',
        goal: 'Create reports',
        backstory: 'Report creator',
        inject_date: true,
        date_format: '%B %d, %Y',
      });
      const taskNode = createMockTaskNode('task-6');
      const edge = createMockEdge('agent-6', 'task-6');

      const mockResponse = createMockJobResponse();
      (apiClient.post as Mock).mockResolvedValue({ data: mockResponse });

      await service.executeJob([agentNode, taskNode], [edge]);

      expect(apiClient.post).toHaveBeenCalledTimes(1);
      const callArgs = (apiClient.post as Mock).mock.calls[0];
      const config = callArgs[1];

      expect(config.agents_yaml['agent_agent-6'].date_format).toBe('%B %d, %Y');
    });

    it('should not include date_format in final config when not provided (undefined values are cleaned)', async () => {
      const agentNode = createMockAgentNode('agent-7', {
        role: 'Scheduler',
        goal: 'Schedule tasks',
        backstory: 'Task scheduler',
        inject_date: true,
        // date_format is NOT provided
      });
      const taskNode = createMockTaskNode('task-7');
      const edge = createMockEdge('agent-7', 'task-7');

      const mockResponse = createMockJobResponse();
      (apiClient.post as Mock).mockResolvedValue({ data: mockResponse });

      await service.executeJob([agentNode, taskNode], [edge]);

      expect(apiClient.post).toHaveBeenCalledTimes(1);
      const callArgs = (apiClient.post as Mock).mock.calls[0];
      const config = callArgs[1];
      const agentConfig = config.agents_yaml['agent_agent-7'];

      // date_format should be undefined and thus cleaned from the config
      expect(agentConfig.date_format).toBeUndefined();
      // Verify it's not present as a key after cleanup
      expect('date_format' in agentConfig).toBe(false);
    });

    it('should include custom ISO date format', async () => {
      const agentNode = createMockAgentNode('agent-8', {
        role: 'Logger',
        goal: 'Log events',
        backstory: 'Event logger',
        inject_date: true,
        date_format: '%Y-%m-%d',
      });
      const taskNode = createMockTaskNode('task-8');
      const edge = createMockEdge('agent-8', 'task-8');

      const mockResponse = createMockJobResponse();
      (apiClient.post as Mock).mockResolvedValue({ data: mockResponse });

      await service.executeJob([agentNode, taskNode], [edge]);

      expect(apiClient.post).toHaveBeenCalledTimes(1);
      const callArgs = (apiClient.post as Mock).mock.calls[0];
      const config = callArgs[1];

      expect(config.agents_yaml['agent_agent-8'].date_format).toBe('%Y-%m-%d');
    });

    it('should include date format with time components', async () => {
      const agentNode = createMockAgentNode('agent-9', {
        role: 'Timestamp Agent',
        goal: 'Track timestamps',
        backstory: 'Timestamp tracker',
        inject_date: true,
        date_format: '%Y-%m-%d %H:%M:%S',
      });
      const taskNode = createMockTaskNode('task-9');
      const edge = createMockEdge('agent-9', 'task-9');

      const mockResponse = createMockJobResponse();
      (apiClient.post as Mock).mockResolvedValue({ data: mockResponse });

      await service.executeJob([agentNode, taskNode], [edge]);

      expect(apiClient.post).toHaveBeenCalledTimes(1);
      const callArgs = (apiClient.post as Mock).mock.calls[0];
      const config = callArgs[1];

      expect(config.agents_yaml['agent_agent-9'].date_format).toBe('%Y-%m-%d %H:%M:%S');
    });
  });

  describe('inject_date and date_format combined scenarios', () => {
    it('should include both inject_date and date_format when both are provided', async () => {
      const agentNode = createMockAgentNode('agent-10', {
        role: 'Time-Aware Agent',
        goal: 'Perform time-sensitive tasks',
        backstory: 'Agent with date awareness',
        inject_date: true,
        date_format: '%A, %B %d, %Y',
      });
      const taskNode = createMockTaskNode('task-10');
      const edge = createMockEdge('agent-10', 'task-10');

      const mockResponse = createMockJobResponse();
      (apiClient.post as Mock).mockResolvedValue({ data: mockResponse });

      await service.executeJob([agentNode, taskNode], [edge]);

      expect(apiClient.post).toHaveBeenCalledTimes(1);
      const callArgs = (apiClient.post as Mock).mock.calls[0];
      const config = callArgs[1];
      const agentConfig = config.agents_yaml['agent_agent-10'];

      expect(agentConfig.inject_date).toBe(true);
      expect(agentConfig.date_format).toBe('%A, %B %d, %Y');
    });

    it('should handle inject_date false with date_format provided (date_format should still be included)', async () => {
      const agentNode = createMockAgentNode('agent-11', {
        role: 'Non-Date Agent',
        goal: 'Perform tasks without date',
        backstory: 'Agent without date injection',
        inject_date: false,
        date_format: '%Y-%m-%d', // Even though inject_date is false, date_format is still saved
      });
      const taskNode = createMockTaskNode('task-11');
      const edge = createMockEdge('agent-11', 'task-11');

      const mockResponse = createMockJobResponse();
      (apiClient.post as Mock).mockResolvedValue({ data: mockResponse });

      await service.executeJob([agentNode, taskNode], [edge]);

      expect(apiClient.post).toHaveBeenCalledTimes(1);
      const callArgs = (apiClient.post as Mock).mock.calls[0];
      const config = callArgs[1];
      const agentConfig = config.agents_yaml['agent_agent-11'];

      expect(agentConfig.inject_date).toBe(false);
      expect(agentConfig.date_format).toBe('%Y-%m-%d');
    });

    it('should handle multiple agents with different date settings', async () => {
      const agent1 = createMockAgentNode('agent-12', {
        role: 'Agent with date',
        goal: 'Goal 1',
        backstory: 'Backstory 1',
        inject_date: true,
        date_format: '%B %d, %Y',
      });
      const agent2 = createMockAgentNode('agent-13', {
        role: 'Agent without date',
        goal: 'Goal 2',
        backstory: 'Backstory 2',
        inject_date: false,
      });
      const agent3 = createMockAgentNode('agent-14', {
        role: 'Agent with default date',
        goal: 'Goal 3',
        backstory: 'Backstory 3',
        // inject_date not provided - should default to true
      });
      const task1 = createMockTaskNode('task-12');
      const task2 = createMockTaskNode('task-13');
      const task3 = createMockTaskNode('task-14');
      const edge1 = createMockEdge('agent-12', 'task-12');
      const edge2 = createMockEdge('agent-13', 'task-13');
      const edge3 = createMockEdge('agent-14', 'task-14');

      const mockResponse = createMockJobResponse();
      (apiClient.post as Mock).mockResolvedValue({ data: mockResponse });

      await service.executeJob(
        [agent1, agent2, agent3, task1, task2, task3],
        [edge1, edge2, edge3]
      );

      expect(apiClient.post).toHaveBeenCalledTimes(1);
      const callArgs = (apiClient.post as Mock).mock.calls[0];
      const config = callArgs[1];

      // Agent 1: inject_date true, date_format provided
      expect(config.agents_yaml['agent_agent-12'].inject_date).toBe(true);
      expect(config.agents_yaml['agent_agent-12'].date_format).toBe('%B %d, %Y');

      // Agent 2: inject_date false, no date_format
      expect(config.agents_yaml['agent_agent-13'].inject_date).toBe(false);
      expect('date_format' in config.agents_yaml['agent_agent-13']).toBe(false);

      // Agent 3: inject_date defaults to true, no date_format
      expect(config.agents_yaml['agent_agent-14'].inject_date).toBe(true);
      expect('date_format' in config.agents_yaml['agent_agent-14']).toBe(false);
    });
  });

  describe('full execution config validation', () => {
    it('should include inject_date and date_format in complete agents_yaml configuration', async () => {
      const agentNode = createMockAgentNode('agent-15', {
        role: 'Complete Agent',
        goal: 'Complete goal',
        backstory: 'Complete backstory',
        tools: ['tool1', 'tool2'],
        llm: 'gpt-4',
        memory: true,
        verbose: true,
        allow_delegation: false,
        inject_date: true,
        date_format: '%d/%m/%Y',
        max_iter: 10,
        max_rpm: 60,
      });
      const taskNode = createMockTaskNode('task-15');
      const edge = createMockEdge('agent-15', 'task-15');

      const mockResponse = createMockJobResponse();
      (apiClient.post as Mock).mockResolvedValue({ data: mockResponse });

      await service.executeJob([agentNode, taskNode], [edge]);

      expect(apiClient.post).toHaveBeenCalledTimes(1);
      const callArgs = (apiClient.post as Mock).mock.calls[0];
      const config = callArgs[1];
      const agentConfig: AgentYaml = config.agents_yaml['agent_agent-15'];

      // Verify all fields are present and correct
      expect(agentConfig.role).toBe('Complete Agent');
      expect(agentConfig.goal).toBe('Complete goal');
      expect(agentConfig.backstory).toBe('Complete backstory');
      expect(agentConfig.tools).toEqual(['tool1', 'tool2']);
      expect(agentConfig.llm).toBe('gpt-4');
      expect(agentConfig.memory).toBe(true);
      expect(agentConfig.verbose).toBe(true);
      expect(agentConfig.allow_delegation).toBe(false);
      expect(agentConfig.inject_date).toBe(true);
      expect(agentConfig.date_format).toBe('%d/%m/%Y');
      expect(agentConfig.max_iter).toBe(10);
      expect(agentConfig.max_rpm).toBe(60);
    });

    it('should send correct config to /executions endpoint', async () => {
      const agentNode = createMockAgentNode('agent-16', {
        role: 'API Test Agent',
        goal: 'Test API',
        backstory: 'API tester',
        inject_date: true,
        date_format: '%Y/%m/%d',
      });
      const taskNode = createMockTaskNode('task-16');
      const edge = createMockEdge('agent-16', 'task-16');

      const mockResponse = createMockJobResponse();
      (apiClient.post as Mock).mockResolvedValue({ data: mockResponse });

      await service.executeJob([agentNode, taskNode], [edge]);

      // Verify the endpoint
      expect(apiClient.post).toHaveBeenCalledWith('/executions', expect.any(Object));

      // Verify the config structure
      const callArgs = (apiClient.post as Mock).mock.calls[0];
      expect(callArgs[0]).toBe('/executions');

      const config = callArgs[1];
      expect(config).toHaveProperty('agents_yaml');
      expect(config).toHaveProperty('tasks_yaml');
      expect(config).toHaveProperty('inputs');
      expect(config).toHaveProperty('planning');
      expect(config).toHaveProperty('execution_type');
    });
  });

  describe('edge cases and boundary conditions', () => {
    it('should handle empty string date_format', async () => {
      const agentNode = createMockAgentNode('agent-17', {
        role: 'Empty Format Agent',
        goal: 'Test empty format',
        backstory: 'Tester',
        inject_date: true,
        date_format: '',
      });
      const taskNode = createMockTaskNode('task-17');
      const edge = createMockEdge('agent-17', 'task-17');

      const mockResponse = createMockJobResponse();
      (apiClient.post as Mock).mockResolvedValue({ data: mockResponse });

      await service.executeJob([agentNode, taskNode], [edge]);

      const callArgs = (apiClient.post as Mock).mock.calls[0];
      const config = callArgs[1];

      // Empty string should be kept (not cleaned as undefined/null)
      expect(config.agents_yaml['agent_agent-17'].inject_date).toBe(true);
      // Empty string is falsy but not undefined/null, behavior depends on cleanup logic
      // Based on the code: if (agentConfig[k] === undefined || agentConfig[k] === null)
      // Empty string will NOT be removed
      expect(config.agents_yaml['agent_agent-17'].date_format).toBe('');
    });

    it('should handle special characters in date_format', async () => {
      const agentNode = createMockAgentNode('agent-18', {
        role: 'Special Format Agent',
        goal: 'Test special format',
        backstory: 'Tester',
        inject_date: true,
        date_format: '%Y-%m-%dT%H:%M:%S%z', // ISO 8601 with timezone
      });
      const taskNode = createMockTaskNode('task-18');
      const edge = createMockEdge('agent-18', 'task-18');

      const mockResponse = createMockJobResponse();
      (apiClient.post as Mock).mockResolvedValue({ data: mockResponse });

      await service.executeJob([agentNode, taskNode], [edge]);

      const callArgs = (apiClient.post as Mock).mock.calls[0];
      const config = callArgs[1];

      expect(config.agents_yaml['agent_agent-18'].date_format).toBe('%Y-%m-%dT%H:%M:%S%z');
    });

    it('should preserve inject_date when other agent fields are missing', async () => {
      const agentNode = createMockAgentNode('agent-19', {
        role: 'Minimal Agent',
        goal: '',
        backstory: '',
        inject_date: true,
      });
      const taskNode = createMockTaskNode('task-19');
      const edge = createMockEdge('agent-19', 'task-19');

      const mockResponse = createMockJobResponse();
      (apiClient.post as Mock).mockResolvedValue({ data: mockResponse });

      await service.executeJob([agentNode, taskNode], [edge]);

      const callArgs = (apiClient.post as Mock).mock.calls[0];
      const config = callArgs[1];

      expect(config.agents_yaml['agent_agent-19'].inject_date).toBe(true);
      expect(config.agents_yaml['agent_agent-19'].role).toBe('Minimal Agent');
    });
  });

  describe('flow execution (should not include date fields in flow config)', () => {
    it('should not process agent nodes in flow execution mode', async () => {
      const crewNode: Node = {
        id: 'crew-1',
        type: 'crewNode',
        position: { x: 0, y: 0 },
        data: {
          label: 'Test Crew',
          crewId: 'crew-123',
        },
      };

      const mockResponse = createMockJobResponse();
      (apiClient.post as Mock).mockResolvedValue({ data: mockResponse });

      await service.executeJob([crewNode], [], false, undefined, 'flow');

      expect(apiClient.post).toHaveBeenCalledTimes(1);
      const callArgs = (apiClient.post as Mock).mock.calls[0];
      const config = callArgs[1];

      // In flow execution, agents_yaml should be empty (loaded from database)
      expect(config.agents_yaml).toEqual({});
      expect(config.execution_type).toBe('flow');
    });
  });

  // ===========================================================================
  // LLM Guardrail Execution Config Tests
  // ===========================================================================

  describe('llm_guardrail execution config', () => {
    it('should include llm_guardrail in tasks_yaml when toggle is ON', async () => {
      const agentNode = createMockAgentNode('agent-gr-1', {
        role: 'Researcher',
        goal: 'Research topics',
        backstory: 'Expert researcher',
      });
      const taskNode = createMockTaskNode('task-gr-1', {
        description: 'Research AI trends',
        expected_output: 'A report with 5 sources',
        config: {
          llm_guardrail: {
            description: 'Validate output contains at least 5 sources',
            llm_model: 'databricks-claude-sonnet-4-5'
          }
        }
      });
      const edge = createMockEdge('agent-gr-1', 'task-gr-1');

      const mockResponse = createMockJobResponse();
      (apiClient.post as Mock).mockResolvedValue({ data: mockResponse });

      await service.executeJob([agentNode, taskNode], [edge]);

      expect(apiClient.post).toHaveBeenCalledTimes(1);
      const callArgs = (apiClient.post as Mock).mock.calls[0];
      const config = callArgs[1];
      const taskName = Object.keys(config.tasks_yaml).find(k => k.includes('task-gr-1') || k.includes('task_gr'));

      expect(taskName).toBeDefined();
      expect(config.tasks_yaml[taskName!].llm_guardrail).toEqual({
        description: 'Validate output contains at least 5 sources',
        llm_model: 'databricks-claude-sonnet-4-5'
      });
    });

    it('should NOT include llm_guardrail when toggle is OFF (null in config)', async () => {
      const agentNode = createMockAgentNode('agent-gr-2', {
        role: 'Writer',
        goal: 'Write content',
        backstory: 'Content writer',
      });
      const taskNode = createMockTaskNode('task-gr-2', {
        description: 'Write an article',
        expected_output: 'A well-written article',
        config: {
          llm_guardrail: null
        }
      });
      const edge = createMockEdge('agent-gr-2', 'task-gr-2');

      const mockResponse = createMockJobResponse();
      (apiClient.post as Mock).mockResolvedValue({ data: mockResponse });

      await service.executeJob([agentNode, taskNode], [edge]);

      const callArgs = (apiClient.post as Mock).mock.calls[0];
      const config = callArgs[1];
      const taskName = Object.keys(config.tasks_yaml).find(k => k.includes('task-gr-2') || k.includes('task_gr'));

      expect(taskName).toBeDefined();
      expect(config.tasks_yaml[taskName!].llm_guardrail).toBeUndefined();
    });

    it('should NOT include llm_guardrail when config has no llm_guardrail key', async () => {
      const agentNode = createMockAgentNode('agent-gr-3', {
        role: 'Analyst',
        goal: 'Analyze data',
        backstory: 'Data analyst',
      });
      const taskNode = createMockTaskNode('task-gr-3', {
        description: 'Analyze market trends',
        expected_output: 'Analysis report',
        config: {
          cache_response: false,
          retry_on_fail: true
          // No llm_guardrail key at all
        }
      });
      const edge = createMockEdge('agent-gr-3', 'task-gr-3');

      const mockResponse = createMockJobResponse();
      (apiClient.post as Mock).mockResolvedValue({ data: mockResponse });

      await service.executeJob([agentNode, taskNode], [edge]);

      const callArgs = (apiClient.post as Mock).mock.calls[0];
      const config = callArgs[1];
      const taskName = Object.keys(config.tasks_yaml).find(k => k.includes('task-gr-3') || k.includes('task_gr'));

      expect(taskName).toBeDefined();
      expect(config.tasks_yaml[taskName!].llm_guardrail).toBeUndefined();
    });

    it('should handle tasks with code guardrail and llm_guardrail independently', async () => {
      const agentNode = createMockAgentNode('agent-gr-4', {
        role: 'Validator',
        goal: 'Validate data',
        backstory: 'Data validator',
      });
      const taskNode = createMockTaskNode('task-gr-4', {
        description: 'Validate customer data',
        expected_output: 'Validation report',
        config: {
          guardrail: '{"type": "data_quality", "checks": ["completeness"]}',
          llm_guardrail: {
            description: 'Validate report is comprehensive',
            llm_model: 'databricks-claude-sonnet-4-5'
          }
        }
      });
      const edge = createMockEdge('agent-gr-4', 'task-gr-4');

      const mockResponse = createMockJobResponse();
      (apiClient.post as Mock).mockResolvedValue({ data: mockResponse });

      await service.executeJob([agentNode, taskNode], [edge]);

      const callArgs = (apiClient.post as Mock).mock.calls[0];
      const config = callArgs[1];
      const taskName = Object.keys(config.tasks_yaml).find(k => k.includes('task-gr-4') || k.includes('task_gr'));

      expect(taskName).toBeDefined();
      // Both guardrails should be present
      expect(config.tasks_yaml[taskName!].guardrail).toEqual({
        type: 'data_quality',
        checks: ['completeness']
      });
      expect(config.tasks_yaml[taskName!].llm_guardrail).toEqual({
        description: 'Validate report is comprehensive',
        llm_model: 'databricks-claude-sonnet-4-5'
      });
    });
  });

  describe('getJobStatus', () => {
    it('should fetch job status correctly', async () => {
      const mockResponse = createMockJobResponse({
        job_id: 'job-test-status',
        status: 'completed',
      });
      (apiClient.get as Mock).mockResolvedValue({ data: mockResponse });

      const result = await service.getJobStatus('job-test-status');

      expect(apiClient.get).toHaveBeenCalledWith('/executions/job-test-status');
      expect(result.job_id).toBe('job-test-status');
      expect(result.status).toBe('completed');
    });
  });

  describe('error handling', () => {
    it('should throw error when no agents are configured for crew execution', async () => {
      const taskNode = createMockTaskNode('task-only');

      await expect(
        service.executeJob([taskNode], [], false, undefined, 'crew')
      ).rejects.toThrow('No agents configured');
    });

    it('should throw error when no tasks are configured for crew execution', async () => {
      const agentNode = createMockAgentNode('agent-only', {
        inject_date: true,
      });

      await expect(
        service.executeJob([agentNode], [], false, undefined, 'crew')
      ).rejects.toThrow('No tasks configured');
    });
  });
});
