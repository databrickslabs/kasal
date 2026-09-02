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
import { GenerationCompleteData, RoutedRunFields } from '../types/dispatcher';
import { buildCrewConfig, buildFlowConfig, buildCrewConfigFromGenerated } from '../utils/crewConfigBuilder';
import {
  DetectedVariable,
  detectVariablesFromNodes,
  detectVariablesFromGenerated,
} from '../../../utils/variableDetector';
import { deriveFlowInputs } from '../../../utils/flowInputs';
import { getSessionPreview } from '../db/sessionApi';
import { registerResultTransform } from '../utils/resultTransforms';
import { latestDeck, parseSlideEdit, planSlideEdit, refinerAgent } from '../utils/slideRefine';
import { ensureDeckFence, fenceDeck, isDeck, splitSlides } from '../utils/htmlDeck';
import { DeckService } from '../../../api/chat/DeckService';

/**
 * What the user still has to be asked for.
 *
 * Three sources, in priority order, and the order is the whole point:
 *
 * 1. `input_schema.required` when the publication has one — AUTHORITATIVE.
 *    It is the only place a human said "quarter is required, format is not";
 *    the `{placeholder}` syntax carries no optionality at all.
 * 2. otherwise every detected placeholder, all treated as required. This is not
 *    a defensive nicety: every publication that predates the schema editor has
 *    `input_schema: null`, and re-publishing is the only thing that fills it, so
 *    this is the live path for the whole back catalogue.
 * 3. minus whatever the router already bound from the user's own sentence.
 *
 * An ABSENT `required` array is not an empty one — absent means nobody has said
 * (fall back), empty means the publisher said nothing is required.
 */
export function resolveMissingVariables(
  nodes: unknown[],
  routed?: RoutedRunFields,
  detect: (nodes: unknown[]) => DetectedVariable[] = detectVariablesFromNodes,
): DetectedVariable[] {
  const bound = routed?.extractedInputs ?? {};
  const declared = routed?.inputSchema?.required;

  const required: DetectedVariable[] =
    declared === undefined
      ? detect(nodes)
      : declared.map((name) => ({ name, required: true }));

  return required.filter((v) => bound[v.name] === undefined);
}

/**
 * The inputs a routed run actually sends.
 *
 * Three sources, and each is a different KIND of thing, which is why they are
 * separate keys rather than merged into one blob:
 *   - the crew's own declared variables, bound from the prompt;
 *   - `user_request`, the sentence this run exists to answer (what memory
 *     recall queries on);
 *   - `referenced_answer`, the earlier answer this run works FROM, when the
 *     request acts on one ("turn this into a deck").
 *
 * Absent keys stay absent — an unrouted run sends exactly what it sent before.
 */
export function buildRunInputs(
  inputs?: Record<string, string>,
  request?: string,
  referencedAnswer?: string | null,
): Record<string, string> | undefined {
  if (!request && !referencedAnswer) return inputs;
  return {
    ...(inputs || {}),
    ...(request ? { user_request: request } : {}),
    ...(referencedAnswer ? { referenced_answer: referencedAnswer } : {}),
  };
}

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
    type: 'crew' | 'generated' | 'flow';
    plan?: PlanData;
    flow?: FlowData;
    data?: GenerationCompleteData;
    spaceId?: string;
    originSession?: string | null;
    /**
     * Values the router already bound from the user's prompt. Held here so a
     * routed run that stops to ask for ONE missing field does not lose the
     * three it already had — the card only ever collects what was missing.
     */
    boundInputs?: Record<string, string>;
    /** The routed capability, carried across the input-variables prompt. */
    capability?: string;
    /** The prompt that selected this capability — see doExecuteCrew. */
    request?: string;
    /** The earlier answer this run works from — see doExecuteCrew. */
    referencedAnswer?: string | null;
  } | null>(null);

  const doExecuteCrew = useCallback(
    async (
      plan: PlanData,
      inputs?: Record<string, string>,
      request?: string,
      referencedAnswer?: string | null,
    ) => {
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
          // The run's own request rides along as `user_request`. It is what
          // memory recall queries on — a saved crew's task description is
          // identical on every run, so without it recall matches the crew's
          // own history rather than what was actually asked for.
          buildRunInputs(inputs, request, referencedAnswer),
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
    async (plan: PlanData, routed?: RoutedRunFields) => {
      const missing = resolveMissingVariables(plan.nodes || [], routed);

      if (missing.length > 0) {
        setPendingExecution({
          type: 'crew',
          plan,
          // What the router already bound survives the detour through the card,
          // so the user is never asked again for something they already said.
          boundInputs: routed?.extractedInputs,
          request: routed?.request,
          referencedAnswer: routed?.referencedAnswer,
        });
        addMessage('assistant', 'This crew needs input variables before it can run.', {
          resultType: 'input_variables',
          resultData: { variables: missing },
        });
        return;
      }
      doExecuteCrew(
        plan,
        routed?.extractedInputs,
        routed?.request,
        routed?.referencedAnswer,
      );
    },
    [doExecuteCrew, addMessage],
  );

  const doExecuteGenerated = useCallback(
    async (
      data: GenerationCompleteData,
      spaceId?: string,
      inputs?: Record<string, string>,
      opts?: {
        preservePreview?: boolean;
        originSession?: string | null;
        /** Turns the run's final text into what the chat should show (see utils/resultTransforms). */
        resultTransform?: (text: string) => string;
      },
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
          if (opts?.resultTransform) registerResultTransform(jobId, opts.resultTransform);
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
  // so the revised artifact streams straight back.
  //
  // A SLIDE edit ("make the chart bigger on slide 3", "delete slide 5") is the
  // exception: it targets the latest deck in this conversation and touches only
  // that slide — the model writes ONE <section> and the rest of the deck is
  // spliced around it (utils/slideRefine). Structural edits need no model.
  const handleRefine = useCallback(
    async (instruction: string) => {
      const trimmed = instruction.trim();
      if (!trimmed) return;
      const clip = (t: string) => (t.length > 80 ? `${t.slice(0, 77)}…` : t);
      const traceStep = (label: string) =>
        useSessionStore.getState().addMessage('assistant', '', {
          resultType: 'trace',
          resultData: { label, sublabel: clip(trimmed), source: 'refine', kind: 'event', timestamp: Date.now() },
        });

      const deck = latestDeck(useSessionStore.getState().messages);
      const edit = deck ? parseSlideEdit(trimmed, splitSlides(deck.code).length) : null;
      if (deck && edit) {
        const plan = planSlideEdit(edit, deck.code);
        addMessage('user', `Refine: ${trimmed}`);
        if (plan.kind === 'instant') {
          // No model involved: the deck is re-posted with the slide moved,
          // removed or copied, open on the slide that changed.
          addMessage('assistant', fenceDeck(plan.deck));
          return;
        }
        // One generation call sized to one slide — not a crew run. It shows
        // as a generation: the "Thinking" container with the edit as its live
        // step, which carries the run's id once the call returns so the trace
        // (the LLM call) opens from the activity. Routed to the session that
        // asked, so switching sessions mid-edit posts nothing elsewhere.
        const sessionStore = useSessionStore.getState();
        const owner = sessionStore.currentSessionId || undefined;
        const post = (content: string, extra?: Parameters<typeof addMessage>[2]) =>
          owner
            ? sessionStore.addMessageToTargetSession(owner, 'assistant', content, extra)
            : sessionStore.addMessage('assistant', content, extra);
        const model = selectedModel || undefined;
        const startedAt = Date.now();
        useExecutionStore.getState().startGeneration(owner);
        const stepId = post('', {
          resultType: 'trace',
          resultData: {
            kind: 'tool_call',
            label: plan.summary,
            sublabel: model ? `${clip(trimmed)} · ${model}` : clip(trimmed),
            source: 'refine',
            timestamp: Date.now(),
          },
        });
        const setStep = (resultData: unknown, executionId?: string) => {
          const updates = { resultType: 'trace', resultData, ...(executionId ? { executionId } : {}) };
          if (owner) sessionStore.updateMessageInTargetSession(owner, stepId, updates);
          else sessionStore.updateMessage(stepId, updates);
        };
        try {
          const res = await DeckService.refineSlide({ ...plan.request, model: model || null });
          if (!res.section) throw new Error(res.error || 'The model did not return a slide.');
          const calls = res.attempts && res.attempts > 1 ? `${res.attempts} LLM calls` : '1 LLM call';
          setStep(
            {
              kind: 'tool_result',
              label: plan.done,
              sublabel: [res.model, calls].filter(Boolean).join(' · '),
              durationMs: Math.max(0, Date.now() - startedAt),
              source: 'refine',
              timestamp: Date.now(),
            },
            res.job_id || undefined,
          );
          post(fenceDeck(plan.apply(res.section)));
        } catch (error) {
          const detail = (error as { response?: { data?: { detail?: unknown } } })?.response?.data?.detail;
          const errMsg =
            typeof detail === 'string' ? detail : error instanceof Error ? error.message : 'The edit failed';
          setStep({
            kind: 'event',
            label: `⚠ ${clip(`Could not ${plan.summary.toLowerCase()}: ${errMsg}`)}`,
            detail: errMsg,
            durationMs: Math.max(0, Date.now() - startedAt),
            source: 'refine',
            timestamp: Date.now(),
          });
          post(`Could not ${plan.summary.toLowerCase()}: ${errMsg}`);
        } finally {
          useExecutionStore.getState().completeGeneration(owner);
        }
        return;
      }

      // Whole-artifact refine: the pane's artifact (or its persisted copy),
      // else the latest deck in the chat.
      let artifact = useExecutionStore.getState().previewContent?.data || '';
      if (!artifact) {
        const sid = useSessionStore.getState().currentSessionId;
        if (sid) {
          const stored = await getSessionPreview(sid);
          artifact = stored?.data || '';
        }
      }
      if (!artifact && deck) artifact = deck.code;
      if (!artifact) {
        addMessage(
          'assistant',
          'There is no result to refine yet. Run a crew first, then use the Refine button or `/refine <instruction>`.',
        );
        return;
      }

      addMessage('user', `Refine: ${trimmed}`);
      traceStep('Refining artifact');

      const editorAgents = [refinerAgent(selectedModel || undefined)];
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
        {
          preservePreview: true,
          // "No code fences" is right for a document and wrong for a deck, which
          // the chat only pages when it arrives fenced — put the fence back.
          ...(isDeck(artifact) ? { resultTransform: ensureDeckFence } : {}),
        },
      );
    },
    [addMessage, doExecuteGenerated, selectedModel],
  );

  const doExecuteFlow = useCallback(
    async (
      flow: FlowData,
      inputs?: Record<string, string>,
      userMessage?: string,
      routedCapability?: string,
    ) => {
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

        // The session and the user's line make this run a TURN: the backend
        // derives the flow's checkpoint lineage from the session, so a second
        // message continues the first run's state instead of starting over.
        // Recorded for the answer message: the backend router reads it back
        // next turn to know this capability is mid-conversation.
        execStore.setRoutedCapability(routedCapability ?? null);
        const flowConfig = buildFlowConfig(
          flow,
          selectedModel || undefined,
          inputs,
          originSessionId,
          userMessage,
        );
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

  const handleExecuteFlow = useCallback(
    async (flow: FlowData, routed?: RoutedRunFields) => {
      // A flow got NO gate at all until now, unlike the crew path: it ran with
      // whatever its state defaulted to, and a declared input was never asked
      // for because nothing ever looked.
      // Derived differently from a crew: a flow reads most of its inputs in
      // router conditions on EDGES (`state.region == "DACH"`), which are not
      // {placeholders} and which a placeholder scan cannot see.
      const missing = resolveMissingVariables(flow.nodes || [], routed, (nodes) =>
        deriveFlowInputs(nodes, flow.edges || []),
      );

      if (missing.length > 0) {
        setPendingExecution({
          type: 'flow',
          flow,
          boundInputs: routed?.extractedInputs,
          capability: routed?.capability,
          request: routed?.request,
          referencedAnswer: routed?.referencedAnswer,
        });
        addMessage('assistant', 'This flow needs input variables before it can run.', {
          resultType: 'input_variables',
          resultData: { variables: missing },
        });
        return;
      }
      doExecuteFlow(flow, routed?.extractedInputs, routed?.request, routed?.capability);
    },
    [doExecuteFlow, addMessage],
  );

  // --- Inline input-variables prompt (genie-style, in the chat flow) ---
  const handleVariablesSubmit = useCallback(
    (_messageId: string, inputs: Record<string, string>) => {
      const pending = pendingExecution;
      setPendingExecution(null);

      if (!pending) {
        // Prompt outlived its parked run (e.g. page reload) — ask for a re-run.
        addMessage('assistant', 'This run prompt has expired — start the run again to use these variables.');
        return;
      }
      if (pending.type === 'flow') {
        doExecuteFlow(
          pending.flow as FlowData,
          { ...(pending.boundInputs ?? {}), ...inputs },
          pending.request,
          pending.capability,
        );
      } else if (pending.type === 'crew') {
        // Anything the router bound comes FIRST, so a value the user typed into
        // the card wins on a key collision — they are looking at the field.
        doExecuteCrew(
          pending.plan as PlanData,
          { ...(pending.boundInputs ?? {}), ...inputs },
          pending.request,
          pending.referencedAnswer,
        );
      } else {
        doExecuteGenerated(pending.data as GenerationCompleteData, pending.spaceId, inputs, { originSession: pending.originSession });
      }
    },
    [pendingExecution, doExecuteCrew, doExecuteGenerated, doExecuteFlow, addMessage],
  );

  return {
    handleExecuteCrew,
    handleExecuteGenerated,
    handleRefine,
    handleExecuteFlow,
    handleVariablesSubmit,
  };
}
