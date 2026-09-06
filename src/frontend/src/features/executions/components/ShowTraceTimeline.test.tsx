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
      // Also check for underscore versions and partial matches
      event.type.includes('memory') ||
      event.type.includes('tool') ||
      event.type.includes('knowledge') ||
      event.type.includes('guardrail')
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
