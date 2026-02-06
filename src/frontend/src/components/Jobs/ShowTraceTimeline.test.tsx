/**
 * Unit tests for ShowTraceTimeline component.
 *
 * Tests the guardrail event clickability and display logic.
 */
import { describe, it, expect } from 'vitest';

/**
 * Test the event clickability determination logic used in ShowTraceTimeline.
 * This logic determines which trace events are clickable based on their type and output.
 */
describe('ShowTraceTimeline Event Clickability', () => {
  /**
   * Helper function that replicates the isClickable logic from ShowTraceTimeline.
   */
  const isEventClickable = (event: { type: string; output?: unknown }): boolean => {
    const hasOutput = !!event.output;
    return hasOutput && (
      event.type === 'llm' ||
      event.type === 'llm_request' ||
      event.type === 'llm_response' ||
      event.type === 'agent_complete' ||
      event.type === 'agent_output' ||
      event.type === 'tool_result' ||
      event.type === 'task_complete' ||
      event.type === 'memory_operation' ||
      event.type === 'memory_write' ||
      event.type === 'memory_retrieval' ||
      event.type === 'tool_usage' ||
      event.type === 'knowledge_operation' ||
      event.type === 'agent_execution' ||
      event.type === 'guardrail' ||
      event.type === 'agent_reasoning' ||
      // Also check for underscore versions and partial matches
      event.type.includes('memory') ||
      event.type.includes('tool') ||
      event.type.includes('knowledge') ||
      event.type.includes('guardrail') ||
      event.type.includes('reasoning')
    );
  };

  describe('Guardrail event clickability', () => {
    it('should make guardrail events with output clickable', () => {
      const guardrailEvent = {
        type: 'guardrail',
        output: 'Guardrail validation passed'
      };

      expect(isEventClickable(guardrailEvent)).toBe(true);
    });

    it('should make llm_guardrail events with output clickable', () => {
      const guardrailEvent = {
        type: 'llm_guardrail',
        output: 'Validation result'
      };

      expect(isEventClickable(guardrailEvent)).toBe(true);
    });

    it('should not make guardrail events without output clickable', () => {
      const guardrailEvent = {
        type: 'guardrail',
        output: undefined
      };

      expect(isEventClickable(guardrailEvent)).toBe(false);
    });

    it('should not make guardrail events with empty output clickable', () => {
      const guardrailEvent = {
        type: 'guardrail',
        output: ''
      };

      expect(isEventClickable(guardrailEvent)).toBe(false);
    });
  });

  describe('Agent reasoning event clickability', () => {
    it('should make agent_reasoning events with output clickable', () => {
      const reasoningEvent = {
        type: 'agent_reasoning',
        output: 'Agent reasoning plan and analysis'
      };

      expect(isEventClickable(reasoningEvent)).toBe(true);
    });

    it('should make reasoning events via includes match clickable', () => {
      const reasoningEvent = {
        type: 'agent_reasoning_started',
        output: 'Starting reasoning process'
      };

      expect(isEventClickable(reasoningEvent)).toBe(true);
    });

    it('should make agent_reasoning_error events clickable', () => {
      const reasoningEvent = {
        type: 'agent_reasoning_error',
        output: 'Reasoning failed: timeout'
      };

      expect(isEventClickable(reasoningEvent)).toBe(true);
    });

    it('should not make agent_reasoning events without output clickable', () => {
      const reasoningEvent = {
        type: 'agent_reasoning',
        output: undefined
      };

      expect(isEventClickable(reasoningEvent)).toBe(false);
    });

    it('should not make agent_reasoning events with empty output clickable', () => {
      const reasoningEvent = {
        type: 'agent_reasoning',
        output: ''
      };

      expect(isEventClickable(reasoningEvent)).toBe(false);
    });
  });

  describe('Other event types clickability', () => {
    it('should make llm events with output clickable', () => {
      const llmEvent = {
        type: 'llm',
        output: 'LLM response content'
      };

      expect(isEventClickable(llmEvent)).toBe(true);
    });

    it('should make tool_usage events with output clickable', () => {
      const toolEvent = {
        type: 'tool_usage',
        output: { tool_name: 'search', result: 'data' }
      };

      expect(isEventClickable(toolEvent)).toBe(true);
    });

    it('should make memory_operation events with output clickable', () => {
      const memoryEvent = {
        type: 'memory_operation',
        output: 'Memory saved successfully'
      };

      expect(isEventClickable(memoryEvent)).toBe(true);
    });

    it('should not make unrecognized event types clickable', () => {
      const unknownEvent = {
        type: 'unknown_event_type',
        output: 'Some output'
      };

      expect(isEventClickable(unknownEvent)).toBe(false);
    });
  });
});

/**
 * Test the guardrail extra data extraction and display logic.
 */
describe('ShowTraceTimeline Guardrail Extra Data', () => {
  /**
   * Helper to extract guardrail display data from extraData.
   */
  const extractGuardrailData = (extraData: Record<string, unknown> | undefined) => {
    if (!extraData) return null;

    return {
      success: extraData.success,
      validationValid: extraData.validation_valid,
      validationMessage: extraData.validation_message,
      guardrailDescription: extraData.guardrail_description,
      taskName: extraData.task_name,
      retryCount: extraData.retry_count
    };
  };

  /**
   * Helper to determine guardrail status from extraData.
   */
  const getGuardrailStatus = (extraData: Record<string, unknown> | undefined): 'passed' | 'failed' | 'unknown' => {
    if (!extraData) return 'unknown';

    const success = extraData.success;
    const validationValid = extraData.validation_valid;

    if (success === true || validationValid === true) return 'passed';
    if (success === false || validationValid === false) return 'failed';
    return 'unknown';
  };

  describe('Guardrail data extraction', () => {
    it('should extract all guardrail fields from extraData', () => {
      const extraData = {
        success: true,
        validation_valid: true,
        validation_message: 'Output meets quality standards',
        guardrail_description: 'Ensure response is helpful and accurate',
        task_name: 'Research Task',
        retry_count: 0
      };

      const result = extractGuardrailData(extraData);

      expect(result).toEqual({
        success: true,
        validationValid: true,
        validationMessage: 'Output meets quality standards',
        guardrailDescription: 'Ensure response is helpful and accurate',
        taskName: 'Research Task',
        retryCount: 0
      });
    });

    it('should return null for undefined extraData', () => {
      const result = extractGuardrailData(undefined);
      expect(result).toBeNull();
    });

    it('should handle partial extraData', () => {
      const extraData = {
        success: false,
        task_name: 'Analysis Task'
      };

      const result = extractGuardrailData(extraData);

      expect(result?.success).toBe(false);
      expect(result?.taskName).toBe('Analysis Task');
      expect(result?.validationMessage).toBeUndefined();
    });
  });

  describe('Guardrail status determination', () => {
    it('should return "passed" when success is true', () => {
      const extraData = { success: true };
      expect(getGuardrailStatus(extraData)).toBe('passed');
    });

    it('should return "passed" when validation_valid is true', () => {
      const extraData = { validation_valid: true };
      expect(getGuardrailStatus(extraData)).toBe('passed');
    });

    it('should return "failed" when success is false', () => {
      const extraData = { success: false };
      expect(getGuardrailStatus(extraData)).toBe('failed');
    });

    it('should return "failed" when validation_valid is false', () => {
      const extraData = { validation_valid: false };
      expect(getGuardrailStatus(extraData)).toBe('failed');
    });

    it('should return "unknown" when no success indicators present', () => {
      const extraData = { retry_count: 2 };
      expect(getGuardrailStatus(extraData)).toBe('unknown');
    });

    it('should return "unknown" for undefined extraData', () => {
      expect(getGuardrailStatus(undefined)).toBe('unknown');
    });

    it('should use OR logic for success indicators', () => {
      // If either success or validation_valid is true, return 'passed'
      const extraData1 = { success: true, validation_valid: false };
      expect(getGuardrailStatus(extraData1)).toBe('passed');

      // If validation_valid is true, even if success is false, return 'passed' (OR logic)
      const extraData2 = { success: false, validation_valid: true };
      expect(getGuardrailStatus(extraData2)).toBe('passed');

      // Both false should return 'failed'
      const extraData3 = { success: false, validation_valid: false };
      expect(getGuardrailStatus(extraData3)).toBe('failed');
    });
  });

  describe('Retry count handling', () => {
    it('should identify events with retries', () => {
      const extraData = { retry_count: 3 };
      const hasRetries = extraData.retry_count !== undefined && Number(extraData.retry_count) > 0;
      expect(hasRetries).toBe(true);
    });

    it('should identify events without retries', () => {
      const extraData = { retry_count: 0 };
      const hasRetries = extraData.retry_count !== undefined && Number(extraData.retry_count) > 0;
      expect(hasRetries).toBe(false);
    });

    it('should handle missing retry_count', () => {
      const extraData: Record<string, unknown> = { success: true };
      const hasRetries = extraData.retry_count !== undefined && Number(extraData.retry_count) > 0;
      expect(hasRetries).toBe(false);
    });
  });
});

/**
 * Test the event object construction with extraData.
 */
describe('ShowTraceTimeline Event Object Construction', () => {
  /**
   * Simulates the event object construction logic from processTraces.
   */
  const constructEvent = (trace: {
    event_type: string;
    output?: unknown;
    extra_data?: Record<string, unknown>;
  }) => {
    const eventType = trace.event_type === 'llm_guardrail' ? 'guardrail' : trace.event_type;
    const extraData = trace.extra_data && typeof trace.extra_data === 'object'
      ? trace.extra_data
      : undefined;

    return {
      type: eventType,
      description: 'Test event',
      output: trace.output,
      extraData
    };
  };

  it('should include extraData in constructed event', () => {
    const trace = {
      event_type: 'llm_guardrail',
      output: 'Guardrail passed',
      extra_data: {
        success: true,
        task_name: 'Research'
      }
    };

    const event = constructEvent(trace);

    expect(event.extraData).toEqual({
      success: true,
      task_name: 'Research'
    });
  });

  it('should map llm_guardrail type to guardrail', () => {
    const trace = {
      event_type: 'llm_guardrail',
      output: 'Test',
      extra_data: {}
    };

    const event = constructEvent(trace);
    expect(event.type).toBe('guardrail');
  });

  it('should handle traces without extra_data', () => {
    const trace = {
      event_type: 'llm_guardrail',
      output: 'Test'
    };

    const event = constructEvent(trace);
    expect(event.extraData).toBeUndefined();
  });

  it('should handle traces with null extra_data', () => {
    const trace = {
      event_type: 'llm_guardrail',
      output: 'Test',
      extra_data: undefined
    };

    const event = constructEvent(trace);
    expect(event.extraData).toBeUndefined();
  });
});

/**
 * Test the agent-level events separation logic.
 * Agent reasoning events should be displayed at the agent level, not nested under tasks.
 */
describe('ShowTraceTimeline Agent-Level Events', () => {
  /**
   * Helper to determine if a trace should be treated as an agent-level event.
   */
  const isAgentLevelEvent = (trace: {
    event_type: string;
    trace_metadata?: Record<string, unknown> | string;
    output?: { extra_data?: Record<string, unknown> };
  }): boolean => {
    if (trace.event_type !== 'agent_reasoning' && trace.event_type !== 'agent_reasoning_error') {
      return false;
    }

    // Parse metadata
    let metadata: Record<string, unknown> | null = null;
    if (trace.trace_metadata) {
      if (typeof trace.trace_metadata === 'string') {
        try {
          metadata = JSON.parse(trace.trace_metadata);
        } catch {
          metadata = null;
        }
      } else if (typeof trace.trace_metadata === 'object') {
        metadata = trace.trace_metadata;
      }
    }

    // Also check extra_data inside output
    const extraData = trace.output?.extra_data || null;
    const operation = metadata?.operation || extraData?.operation;

    // Skip "reasoning_started" events
    return operation !== 'reasoning_started';
  };

  describe('Agent-level event identification', () => {
    it('should identify agent_reasoning completed events as agent-level', () => {
      const trace = {
        event_type: 'agent_reasoning',
        trace_metadata: { operation: 'reasoning_completed' },
        output: undefined
      };

      expect(isAgentLevelEvent(trace)).toBe(true);
    });

    it('should NOT identify agent_reasoning started events as agent-level', () => {
      const trace = {
        event_type: 'agent_reasoning',
        trace_metadata: { operation: 'reasoning_started' },
        output: undefined
      };

      expect(isAgentLevelEvent(trace)).toBe(false);
    });

    it('should identify agent_reasoning_error events as agent-level', () => {
      const trace = {
        event_type: 'agent_reasoning_error',
        trace_metadata: { operation: 'reasoning_completed' },
        output: undefined
      };

      expect(isAgentLevelEvent(trace)).toBe(true);
    });

    it('should NOT identify other event types as agent-level', () => {
      const trace = {
        event_type: 'llm_call',
        trace_metadata: {},
        output: undefined
      };

      expect(isAgentLevelEvent(trace)).toBe(false);
    });

    it('should handle JSON string metadata', () => {
      const trace = {
        event_type: 'agent_reasoning',
        trace_metadata: JSON.stringify({ operation: 'reasoning_completed' }),
        output: undefined
      };

      expect(isAgentLevelEvent(trace)).toBe(true);
    });

    it('should handle operation in output extra_data', () => {
      const trace = {
        event_type: 'agent_reasoning',
        trace_metadata: undefined,
        output: { extra_data: { operation: 'reasoning_completed' } }
      };

      expect(isAgentLevelEvent(trace)).toBe(true);
    });
  });

  describe('Agent event description determination', () => {
    /**
     * Helper to determine description for agent-level events.
     */
    const getAgentEventDescription = (output: string | undefined): string => {
      if (!output) return 'Agent Reasoning';

      if (output.toLowerCase().includes('plan')) {
        return 'Agent Planning';
      } else if (output.length > 100) {
        return 'Agent Reasoning';
      }
      return 'Agent Reasoning';
    };

    it('should describe planning events correctly', () => {
      const output = 'Here is my plan for the task: 1. Research 2. Analyze 3. Report';
      expect(getAgentEventDescription(output)).toBe('Agent Planning');
    });

    it('should describe reasoning events with long content', () => {
      const output = 'A'.repeat(150); // Long content without "plan"
      expect(getAgentEventDescription(output)).toBe('Agent Reasoning');
    });

    it('should default to Agent Reasoning for short content', () => {
      const output = 'Short reasoning';
      expect(getAgentEventDescription(output)).toBe('Agent Reasoning');
    });

    it('should default to Agent Reasoning for undefined output', () => {
      expect(getAgentEventDescription(undefined)).toBe('Agent Reasoning');
    });
  });
});

/**
 * Test the crew-level planning events separation logic.
 * Task Execution Planner events should be displayed at the crew level, not as a regular agent.
 */
describe('ShowTraceTimeline Crew Planning Events', () => {
  /**
   * Helper to determine if a trace is a crew planning trace.
   */
  const isCrewPlanningTrace = (trace: { event_source: string }): boolean => {
    return trace.event_source === 'Task Execution Planner';
  };

  /**
   * Helper to determine if a crew planning trace should be included in display.
   * We only show meaningful events (LLM response with the plan, or task completion).
   */
  const shouldIncludeCrewPlanningTrace = (trace: { event_source: string; event_type: string }): boolean => {
    if (!isCrewPlanningTrace(trace)) return false;
    return trace.event_type === 'llm_response' || trace.event_type === 'task_completed';
  };

  describe('Crew planning trace identification', () => {
    it('should identify Task Execution Planner as crew planning', () => {
      const trace = { event_source: 'Task Execution Planner' };
      expect(isCrewPlanningTrace(trace)).toBe(true);
    });

    it('should NOT identify regular agents as crew planning', () => {
      const trace = { event_source: 'News Article Collector' };
      expect(isCrewPlanningTrace(trace)).toBe(false);
    });

    it('should NOT identify crew events as crew planning', () => {
      const trace = { event_source: 'crew' };
      expect(isCrewPlanningTrace(trace)).toBe(false);
    });
  });

  describe('Crew planning trace filtering', () => {
    it('should include llm_response events from Task Execution Planner', () => {
      const trace = {
        event_source: 'Task Execution Planner',
        event_type: 'llm_response'
      };
      expect(shouldIncludeCrewPlanningTrace(trace)).toBe(true);
    });

    it('should include task_completed events from Task Execution Planner', () => {
      const trace = {
        event_source: 'Task Execution Planner',
        event_type: 'task_completed'
      };
      expect(shouldIncludeCrewPlanningTrace(trace)).toBe(true);
    });

    it('should NOT include task_started events from Task Execution Planner', () => {
      const trace = {
        event_source: 'Task Execution Planner',
        event_type: 'task_started'
      };
      expect(shouldIncludeCrewPlanningTrace(trace)).toBe(false);
    });

    it('should NOT include llm_request events from Task Execution Planner', () => {
      const trace = {
        event_source: 'Task Execution Planner',
        event_type: 'llm_request'
      };
      expect(shouldIncludeCrewPlanningTrace(trace)).toBe(false);
    });

    it('should NOT include events from other agents', () => {
      const trace = {
        event_source: 'News Article Collector',
        event_type: 'llm_response'
      };
      expect(shouldIncludeCrewPlanningTrace(trace)).toBe(false);
    });
  });

  describe('Crew planning event description', () => {
    /**
     * Helper to get crew planning event description.
     */
    const getCrewPlanningDescription = (eventType: string, outputLength: number): string => {
      if (eventType === 'llm_response') {
        return `Execution Plan (${outputLength.toLocaleString()} chars)`;
      } else if (eventType === 'task_completed') {
        return 'Planning Complete';
      }
      return 'Crew Planning';
    };

    it('should describe llm_response as Execution Plan with character count', () => {
      const description = getCrewPlanningDescription('llm_response', 3040);
      expect(description).toBe('Execution Plan (3,040 chars)');
    });

    it('should describe task_completed as Planning Complete', () => {
      const description = getCrewPlanningDescription('task_completed', 0);
      expect(description).toBe('Planning Complete');
    });

    it('should default to Crew Planning for other event types', () => {
      const description = getCrewPlanningDescription('unknown', 0);
      expect(description).toBe('Crew Planning');
    });
  });
});
