/**
 * Starting a run from the chat: crews, generated crews, flows, and refinement.
 *
 * Each entry point does the same three things in its own way — build the config,
 * post the user-facing message, then hand the job to the run stream. The
 * variable-detection dialog sits in front of the crew/generated paths, so the
 * pending-execution state it parks a run in lives here too; nothing outside
 * reads it.
 *
 * Extracted from ChatWorkspace, which the JSX only ever needed five handlers
 * from.
 */
import { useState, useCallback } from 'react';
import { createExecution } from '../api/executions';
import { useSessionStore } from '../store/sessionStore';
import { useExecutionStore } from '../store/executionStore';
import { useAppStore } from '../store/appStore';
import { PlanData, FlowData } from '../hooks/useDispatcher';
import { GenerationCompleteData } from '../types/dispatcher';
import { buildCrewConfig, buildFlowConfig, buildCrewConfigFromGenerated } from '../utils/crewConfigBuilder';
import { detectVariablesFromNodes, detectVariablesFromGenerated } from '../utils/variableDetector';
import { getSessionPreview } from '../db/sessionApi';

interface UseChatExecutionActionsArgs {
  /** From useChatRunStream — hands a started job to the SSE/polling pipeline. */
  handleStartExecutionStream: (
    jobId: string,
    ownerSessionId?: string,
    opts?: { preservePreview?: boolean },
  ) => void;
}

export function useChatExecutionActions({
  handleStartExecutionStream,
}: UseChatExecutionActionsArgs) {

  const addMessage = useSessionStore((s) => s.addMessage);
  const selectedModel = useAppStore((s) => s.selectedModel);

  // Input variables dialog state: a run parked until the user supplies the
  // variables the crew/flow declares. Nothing outside this hook reads it.
  const [pendingExecution, setPendingExecution] = useState<{
    type: 'crew' | 'generated';
    plan?: PlanData;
    data?: GenerationCompleteData;
    spaceId?: string;
    originSession?: string | null;
  } | null>(null);

  const doExecuteCrew = useCallback(
    async (plan: PlanData, inputs?: Record<string, string>) => {
      // Capture the session ID NOW, before the async createExecution call.
      // If the user switches sessions during the API call, currentSessionId
      // will have changed, but originSessionId preserves the correct owner.
      const originSessionId = useSessionStore.getState().currentSessionId;

      const execStore = useExecutionStore.getState();
      execStore.setIsLoading(true);
      try {
        const nodes = (plan.nodes || []) as { type: string; data: Record<string, unknown> }[];
        const agentNames = nodes
          .filter((n) => n.type === 'agentNode' || n.type === 'agent')
          .map((n) => ({ name: (n.data.role as string) || (n.data.name as string) || 'Agent' }));
        const taskNames = nodes
          .filter((n) => n.type === 'taskNode' || n.type === 'task')
          .map((n) => ({ name: (n.data.name as string) || (n.data.description as string)?.slice(0, 40) || 'Task' }));
        execStore.setExecutionContext({
          crewName: plan.name || 'Crew',
          agents: agentNames,
          tasks: taskNames,
        });

        // Reflect the chat's current memory toggle on the loaded plan, so
        // disabling memory in the chat runs the loaded crew without memory.
        const crewConfig = buildCrewConfig(
          plan,
          selectedModel || undefined,
          inputs,
          useExecutionStore.getState().memoryEnabled,
          // Agent Bricks endpoints picked in the "+" menu — equip + configure the
          // tool on this loaded crew so it has the endpoint (else "not configured").
          useExecutionStore.getState().selectedAgentBricksEndpoints,
        );
        const execution = await createExecution(crewConfig);
        const jobId = execution.job_id || execution.execution_id;
        if (jobId) {
          handleStartExecutionStream(jobId, originSessionId || undefined);
        } else {
          addMessage('assistant', 'Execution started but no job ID was returned.');
          execStore.setExecutionContext(null);
          execStore.setIsLoading(false);
        }
      } catch (error) {
        const errMsg = error instanceof Error ? error.message : 'Failed to start execution';
        addMessage('assistant', `Execution failed: ${errMsg}`);
        execStore.setExecutionContext(null);
        execStore.setIsLoading(false);
      }
    },
    [addMessage, handleStartExecutionStream, selectedModel],
  );

  const handleExecuteCrew = useCallback(
    async (plan: PlanData) => {
      const vars = detectVariablesFromNodes(plan.nodes || []);
      if (vars.length > 0) {
        setPendingExecution({ type: 'crew', plan });
        addMessage('assistant', 'This crew needs input variables before it can run.', {
          resultType: 'input_variables',
          resultData: { variables: vars },
        });
        return;
      }
      doExecuteCrew(plan);
    },
    [doExecuteCrew],
  );

  const doExecuteGenerated = useCallback(
    async (
      data: GenerationCompleteData,
      spaceId?: string,
      inputs?: Record<string, string>,
      opts?: { preservePreview?: boolean; originSession?: string | null },
    ) => {
      // The run belongs to the session that started it. On auto-run after
      // generation, that's the generation's origin (passed via opts) — NOT the
      // session currently on screen, which the user may have switched to.
      const originSessionId = opts?.originSession || useSessionStore.getState().currentSessionId;

      const execStore = useExecutionStore.getState();
      execStore.setIsLoading(true);
      try {
        const agentNames = (data.agents || []).map((a) => ({
          name: (a.name as string) || (a.role as string) || 'Agent',
          role: (a.role as string) || undefined,
        }));
        const taskNames = (data.tasks || []).map((t) => ({
          name: (t.name as string) || (t.description as string)?.slice(0, 40) || 'Task',
        }));
        execStore.setExecutionContext({
          crewName: 'Generated Crew',
          agents: agentNames,
          tasks: taskNames,
        });

        // If a Genie space was selected, pass tool_configs with the spaceId
        // (the selector only shows when GenieTool is already in the crew's tools)
        const agents = data.agents;
        const taskList = data.tasks;
        // Agent Bricks mirrors the Genie mechanism EXACTLY: configure the tool by
        // NAME in tool_configs, and buildCrewConfigFromGenerated → applicableToolConfigs
        // attaches it to whichever agent/task already lists the tool (the generator
        // equips AgentBricksTool, just like GenieTool). The backend skips the tool if
        // no endpoint resolves, so an unselected/empty pick never aborts the run.
        const selectedAgentBricks =
          useExecutionStore.getState().selectedAgentBricksEndpoints || [];
        const toolConfigs: Record<string, Record<string, unknown>> = {};
        if (spaceId) toolConfigs.GenieTool = { spaceId };
        if (selectedAgentBricks.length > 0) {
          toolConfigs.AgentBricksTool = { endpointName: selectedAgentBricks };
        }
        const toolConfigsArg =
          Object.keys(toolConfigs).length > 0 ? toolConfigs : undefined;

        // NOTE: "Predefined UI" emission is enforced in the BACKEND
        // (crew_preparation → ui_emission) so every channel — chat, Crew mode,
        // API, schedules — behaves the same. No frontend injection here.
        const crewConfig = buildCrewConfigFromGenerated(
          agents,
          taskList,
          selectedModel || undefined,
          toolConfigsArg,
          inputs,
          useAppStore.getState().toolNameMap,
          originSessionId,
          // Read the recall scope from the store at execution time so the value
          // is always the user's current choice (not a stale closure capture).
          useExecutionStore.getState().workspaceMemory,
          // "No memory" mode → agents are built without memory.
          useExecutionStore.getState().memoryEnabled,
          // MCP servers picked via the chat input's "+" menu.
          useExecutionStore.getState().selectedMcpServers,
          // The chat prompt that asked for this crew — appended to the task
          // descriptions so the run answers it (instead of running a generic
          // mission with no actual question).
          data.user_request,
          // Agent Bricks endpoints picked via the chat input's "+" menu.
          useExecutionStore.getState().selectedAgentBricksEndpoints,
          // Answer mode → reasoning (research/deep) so a manually re-run crew
          // matches what the mode (and save) produced.
          useExecutionStore.getState().chatModeType === 'research' ||
            useExecutionStore.getState().chatModeType === 'deep',
          // …and the mode itself, so the backend applies the REST of what the
          // mode means (guardrail retries, execution budget, and deep's JSON
          // envelope + gate). A re-run from here skips generation entirely, so
          // without this it would run ungated while the identical
          // auto-executed run was gated.
          useExecutionStore.getState().chatModeType,
        );
        const execution = await createExecution(crewConfig);
        const jobId = execution.job_id || execution.execution_id;
        if (jobId) {
          handleStartExecutionStream(jobId, originSessionId || undefined, opts);
        } else {
          addMessage('assistant', 'Execution started but no job ID was returned.');
          execStore.setExecutionContext(null);
          execStore.setIsLoading(false);
        }
      } catch (error) {
        const errMsg = error instanceof Error ? error.message : 'Failed to start execution';
        addMessage('assistant', `Execution failed: ${errMsg}`);
        execStore.setExecutionContext(null);
        execStore.setIsLoading(false);
      }
    },
    [addMessage, handleStartExecutionStream, selectedModel],
  );

  const handleExecuteGenerated = useCallback(
    async (data: GenerationCompleteData, spaceId?: string, originSession?: string | null) => {
      const vars = detectVariablesFromGenerated(data.agents || [], data.tasks || []);
      if (vars.length > 0) {
        setPendingExecution({ type: 'generated', data, spaceId, originSession });
        addMessage('assistant', 'This crew needs input variables before it can run.', {
          resultType: 'input_variables',
          resultData: { variables: vars },
        });
        return;
      }
      doExecuteGenerated(data, spaceId, undefined, { originSession });
    },
    [doExecuteGenerated],
  );

  // --- Refine the current artifact instead of generating a brand-new crew ---
  // Builds a single "editor" agent + task whose input is the previous artifact
  // plus the user's instruction, then runs it through the normal execution path
  // so the revised artifact streams straight back into the preview pane.
  const handleRefine = useCallback(
    async (instruction: string) => {
      const trimmed = instruction.trim();
      if (!trimmed) return;

      // Resolve the artifact currently shown (or last persisted) for this session.
      let artifact = useExecutionStore.getState().previewContent?.data || '';
      if (!artifact) {
        const sid = useSessionStore.getState().currentSessionId;
        if (sid) {
          const stored = await getSessionPreview(sid);
          artifact = stored?.data || '';
        }
      }
      if (!artifact) {
        addMessage(
          'assistant',
          'There is no result to refine yet. Run a crew first, then use the Refine button or `/refine <instruction>`.',
        );
        return;
      }

      addMessage('user', `Refine: ${trimmed}`);
      // Give the refine run its own activity section — same treatment as a
      // regular prompt: this trace anchors the collapsible run container
      // right under the Refine message, ABOVE the refined result, and it
      // persists there after the run finishes.
      useSessionStore.getState().addMessage('assistant', '', {
        resultType: 'trace',
        resultData: {
          label: 'Refining artifact',
          sublabel: trimmed.length > 80 ? `${trimmed.slice(0, 77)}…` : trimmed,
          source: 'refine',
          kind: 'event',
          timestamp: Date.now(),
        },
      });

      const editorAgents = [
        {
          id: 'refiner',
          role: 'Content Editor',
          goal: 'Revise the provided artifact according to the user instruction, preserving correctness and returning the complete updated artifact.',
          backstory:
            'You are an expert editor and front-end developer who refines documents and HTML, keeping the output valid, self-contained and ready to render.',
          tools: [],
          // Pin the editor to the user-selected model. Without an explicit llm
          // the backend defaults this hand-built agent to gpt-4o, which fails in
          // Databricks environments with no OpenAI key.
          ...(selectedModel ? { llm: selectedModel } : {}),
          // A refine is a single-shot edit, not a research crew. Disabling memory
          // (the only agent → disables crew memory entirely) skips the memory
          // memory search/save flow; no delegation keeps it to one LLM pass.
          memory: false,
          allow_delegation: false,
        },
      ];
      const editorTasks = [
        {
          id: 'refine_task',
          name: 'Refine artifact',
          agent_id: 'refiner',
          // The instruction and artifact are passed as crew inputs (below) and
          // referenced via {instruction}/{artifact} placeholders. CrewAI runs a
          // single-pass {var} interpolation over the description, so the artifact
          // must NOT be inlined: an HTML/CSS/JS artifact routinely contains brace
          // tokens (e.g. a JS template literal `${spread}` → `{spread}`) that
          // CrewAI would otherwise read as missing template variables and fail
          // ("Template variable 'spread' not found in inputs dictionary"). The
          // substituted input values are not re-scanned, so their braces are safe.
          description:
            `Improve the artifact below based on this instruction.\n\n` +
            `INSTRUCTION:\n{instruction}\n\n` +
            `CURRENT ARTIFACT:\n{artifact}\n\n` +
            `Return ONLY the complete revised artifact (e.g. the full HTML document) with no commentary and no markdown code fences.`,
          expected_output: 'The complete revised artifact, ready to render.',
          tools: [],
        },
      ];

      // doExecuteGenerated runs immediately (no variable-detection dialog).
      // preservePreview keeps the current artifact + history visible so the
      // refined version is appended (scroll back to compare), not wiped.
      doExecuteGenerated(
        { agents: editorAgents, tasks: editorTasks },
        undefined,
        { instruction: trimmed, artifact },
        { preservePreview: true },
      );
    },
    [addMessage, doExecuteGenerated, selectedModel],
  );

  const handleExecuteFlow = useCallback(
    async (flow: FlowData) => {
      // Capture the session ID NOW, before the async createExecution call.
      const originSessionId = useSessionStore.getState().currentSessionId;

      const execStore = useExecutionStore.getState();
      execStore.setIsLoading(true);
      try {
        execStore.setExecutionContext({
          crewName: flow.name || 'Flow',
          agents: [],
          tasks: [],
        });

        const flowConfig = buildFlowConfig(flow, selectedModel || undefined);
        const execution = await createExecution(flowConfig);
        const jobId = execution.job_id || execution.execution_id;
        if (jobId) {
          handleStartExecutionStream(jobId, originSessionId || undefined);
        } else {
          addMessage('assistant', 'Execution started but no job ID was returned.');
          execStore.setExecutionContext(null);
          execStore.setIsLoading(false);
        }
      } catch (error) {
        const errMsg = error instanceof Error ? error.message : 'Failed to start execution';
        addMessage('assistant', `Execution failed: ${errMsg}`);
        execStore.setExecutionContext(null);
        execStore.setIsLoading(false);
      }
    },
    [addMessage, handleStartExecutionStream, selectedModel],
  );

  // --- Inline input-variables prompt (genie-style, in the chat flow) ---
  const handleVariablesSubmit = useCallback(
    (_messageId: string, inputs: Record<string, string>) => {
      const pending = pendingExecution;
      setPendingExecution(null);

      if (!pending) {
        // Prompt outlived its parked run (e.g. page reload) — ask for a re-run.
        addMessage('assistant', 'This run prompt has expired — run the crew again to use these variables.');
        return;
      }
      // pending is always a crew (carrying a plan) or a generated crew (carrying data).
      if (pending.type === 'crew') {
        doExecuteCrew(pending.plan as PlanData, inputs);
      } else {
        doExecuteGenerated(pending.data as GenerationCompleteData, pending.spaceId, inputs, { originSession: pending.originSession });
      }
    },
    [pendingExecution, doExecuteCrew, doExecuteGenerated, addMessage],
  );

  return {
    handleExecuteCrew,
    handleExecuteGenerated,
    handleRefine,
    handleExecuteFlow,
    handleVariablesSubmit,
  };
}
