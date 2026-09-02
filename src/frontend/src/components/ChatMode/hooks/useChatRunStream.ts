/**
 * The chat's side of a run's event stream.
 *
 * Owns the SSE wiring, the translation of trace events into chat messages, the
 * once-only completion/failure/approval handling, and the reconnect + dead-job
 * recovery that Databricks Apps (where SSE dies) depends on.
 *
 * Extracted from ChatWorkspace, where it was ~470 lines that the JSX never
 * touched — pure plumbing that only fed itself. It owns the ten bookkeeping refs
 * that no other part of the workspace reads, and subscribes to the stores
 * directly, so the whole surface is one param in and two values out.
 *
 * `pendingActionsRef` is the one exception: it is WRITTEN by
 * handleStartGenerationStream, which has to stay in the component (useDispatcher
 * consumes it, and its body reads `dispatcher` back — a declaration cycle the
 * component documents), and READ here.
 */
import React, { useCallback, useEffect, useRef } from 'react';
import { pendingActionsBelongTo } from '../utils/pendingActions';
import { ExecutionStatus } from '../types/execution';
import { getExecutionStatus } from '../api/executions';
import { useSessionStore } from '../store/sessionStore';
import { rememberTaskOutputMessage, useExecutionStore } from '../store/executionStore';
import { readActiveExecution, clearActiveExecution } from '../store/activeExecutionMarker';
import { useExecutionStream } from './useExecutionStream';
import { stopAllGenerationStreams } from '../utils/generationStreamManager';
import { generateId } from '../utils/markdown';
import { parsePreviewContent } from '../components/Preview/PreviewPanel';
import { applyResultTransform, dropResultTransform } from '../utils/resultTransforms';
import type { A2uiMessage } from '../../../shared/a2ui/stream';
import type { Surface } from '../../../shared/a2ui';
import { buildTraceEntry } from '../utils/traceActivity';
import { cleanTaskLabel, taskHeaderLabel, summarizeTaskOutput } from '../utils/taskChatRendering';
import { extractResultText, extractA2uiSurface } from '../utils/resultExtraction';
import { GenerationCompleteData } from '../types/dispatcher';

interface UseChatRunStreamArgs {
  /** Written by the generation stream in ChatWorkspace, drained here. */
  pendingActionsRef: React.MutableRefObject<{
    data: GenerationCompleteData;
    ownerSession: string | null;
    /** The run this row is for. Set from generation_complete's execution_id
     *  when present, else bound when that session's run stream starts. */
    jobId?: string | null;
    mode?: string;
    usedWorkspaceMemory?: boolean;
    capability?: string;
  } | null>;
}

export function useChatRunStream({ pendingActionsRef }: UseChatRunStreamArgs) {

  const currentSessionId = useSessionStore((s) => s.currentSessionId);

  // Collapse paired tool events (tool_usage + matching *_run) into a single
  // chat pill keyed by matchKey, regardless of which order they arrive in.
  const traceMessageIdsRef = useRef<Map<string, { messageId: string; resolved: boolean }>>(
    new Map(),
  );

  // Trace DB ids already rendered this run. The live SSE stream and the REST
  // polling fallback (Job-History style) can both deliver the same trace; this
  // guarantees each trace renders exactly once regardless of transport.
  const seenTraceIdsRef = useRef<Set<number>>(new Set());

  // jobId -> label of the last task header posted, so a trace redelivered by the
  // polling fallback does not post a duplicate header (or split the bubble).
  const taskHeadersRef = useRef<Map<string, string>>(new Map());

  // Task names that already got a header, so task_completed renders only the
  // body instead of repeating the name it was announced under.
  const headedTasksRef = useRef<Set<string>>(new Set());

  // jobId → the session that STARTED that job. Every trace/output carries its
  // job_id, so we attribute it to the right session even when runs overlap or
  // the user switched away — instead of trusting the single global "current
  // owner" slot, which mis-routes one session's output into another.
  const jobOwnerRef = useRef<Map<string, string>>(new Map());

  // The job currently bound to the single live SSE stream (startStream replaces,
  // so only the latest foreground run streams here). The SSE onComplete/onError
  // carry no job id, so we stamp completion with this; backgrounded runs finalize
  // via the REST poller's window events, which DO carry an explicit job id.
  const sseJobIdRef = useRef<string | undefined>(undefined);

  // Render a task's output: surface previewable content in the preview pane
  // (scoped to the owning session) and append a concise chat message. Shared by
  // the live SSE stream and the REST polling fallback.
  const handleTaskOutput = useCallback((taskName: string, output: string, ownerSession: string | null, jobId?: string) => {
    const execState = useExecutionStore.getState();
    const sessionStore = useSessionStore.getState();

    // Try to extract renderable content from various result shapes
    let displayContent = output;
    try {
      // The output may be JSON with a "content" field wrapping the actual result
      const parsed = typeof output === 'string' && output.trim().startsWith('{')
        ? JSON.parse(output) as Record<string, unknown>
        : null;
      if (parsed?.content && typeof parsed.content === 'string') {
        displayContent = parsed.content;
      }
    } catch { /* not JSON, use raw */ }

    // Check if the content is previewable (HTML, structured markdown, A2UI
    // surface, …). A UI document renders as a dashboard in the PREVIEW pane.
    const preview = parsePreviewContent(displayContent);
    const currentSession = sessionStore.currentSessionId;
    if (preview) {
      if (currentSession === ownerSession) {
        execState.setPreviewContent(preview);
      } else if (ownerSession) {
        // This run's session is off screen — park the preview into ITS snapshot
        // (not the live slot, which belongs to whatever's on screen now) so it's
        // there on switch-back. Without this the preview reached only IndexedDB
        // and a later null-preview completion snapshot hid it.
        execState.stashSessionPreview(ownerSession, preview);
      }
      // NOT persisted per task output: each PUT re-uploaded the full artifact
      // (multi-100KB surfaces) mid-run. Durable persistence happens ONCE at
      // completion (executionStore.completeExecution) — and after a mid-run
      // refresh the deliverable derives from execution.result anyway.
    }

    // Build a concise chat-message body. Raw task output (HTML dumps, status
    // pings like "Calling tools.", or echoed task descriptions) clutters the
    // chat; the real content lives in the preview pane.
    const chatBody = summarizeTaskOutput(displayContent, preview, taskName);
    // This task's tokens already streamed into a bubble on screen, so the trace
    // body would be a SECOND copy of the same answer. Skip it and let the bubble
    // stand — completeExecution finalizes it in place (see `streamBubbleId`
    // there). When nothing streamed this is false and the body posts as before,
    // which is the only copy: streaming off, a non-streaming model, or the owner
    // session off screen.
    const alreadyStreamed = Boolean(jobId) && execState.hasStreamedTaskText(jobId!);
    // The run already finalized, so this trace's output has been superseded by
    // the final answer that is already on screen. `_relay_task_events` broadcasts
    // task_completed from its own queue with NO DB id, so it escapes the trace
    // de-dupe and routinely lands after completion — which printed the answer a
    // second time, below the copy the reader had been reading.
    const runFinalized = Boolean(jobId) && execState.isRunFinalized(jobId!);
    if (chatBody !== null && !alreadyStreamed && !runFinalized) {
      // No label prefix: task_started already posted a header above this task's
      // streamed tokens. Repeating it here is what produced
      // "**<80 chars of prompt>** — <the same prompt again>".
      const alreadyHeaded = headedTasksRef.current.has(taskName);
      const withHeader = (text: string) =>
        alreadyHeaded ? text : `**${cleanTaskLabel(taskName)}** — ${text}`;
      const msg = withHeader(chatBody);
      // Carry the uncapped text when this line is a PREVIEW. `chatBody` is what
      // summarizeTaskOutput kept; when they differ, this line is a preview of
      // something longer, and ChatMessage renders the uncapped text directly.
      // It gets the SAME header as the preview — without that, showing the full
      // text dropped the "**Task** — " prefix off every un-headed line.
      const capped = chatBody !== displayContent.trim();
      const extra = capped
        ? { fullContent: withHeader(displayContent.trim()) }
        : undefined;
      const messageId = ownerSession
        ? sessionStore.addMessageToTargetSession(ownerSession, 'assistant', msg, extra)
        : sessionStore.addMessage('assistant', msg, extra);
      // This line is a PREVIEW when the output was long enough to cap. Register
      // it so completion replaces THIS message with the full answer instead of
      // hunting for it by prefix-matching the on-screen list — a scan that kept
      // missing (trace pills crowding its window; a backgrounded session's
      // messages never entering the in-memory array at all) and left the capped
      // line sitting above a second, complete copy.
      if (jobId && messageId) rememberTaskOutputMessage(jobId, messageId);
    }

    // NOTE: we intentionally do NOT auto-complete the execution on a timer.
    // The crew keeps running in the UI (appending each task's output to the
    // preview) until the real completion/error arrives — via SSE or, when the
    // Databricks Apps HTTP/2 proxy kills SSE, via the polling fallback's
    // 'jobCompleted'/'jobFailed' window events.
  }, []);

  // Render a single trace event (tool pill / memory / task output) into the
  // chat. Deduped by trace DB id so the live SSE stream and the REST polling
  // fallback never render the same trace twice. This is THE seam that lets
  // memory + Genie + tool traces appear in chat even when SSE is dead
  // (Databricks Apps), exactly like crew-mode Job History does via polling.
  const processTrace = useCallback((message: string, data?: Record<string, unknown>) => {
    const traceId = data?.id;
    if (typeof traceId === 'number') {
      if (seenTraceIdsRef.current.has(traceId)) return;
      seenTraceIdsRef.current.add(traceId);
    }

    // Attribute this trace to the session that STARTED its job (from job_id),
    // not the session currently on screen — so a still-running run's output
    // never lands in the session you switched to.
    const jobId = data?.job_id as string | undefined;
    const ownerSession =
      (jobId && jobOwnerRef.current.get(jobId)) ||
      useExecutionStore.getState().executionOwnerSessionId;

    const trace = buildTraceEntry(message, data);
    if (trace) {
      // The run-activity timeline (preview pane) is derived from these trace
      // messages — see `runActivitySteps`. tool_result traces carry the step's
      // label / query / context the agent pulled in.
      const sessionStore = useSessionStore.getState();
      let handled = false;

      if (trace.matchKey) {
        const existing = traceMessageIdsRef.current.get(trace.matchKey);
        if (existing) {
          // Already have a pill for this key. The tool_result is the richer
          // event (has duration + content), so promote the pill to it; drop
          // any later tool_call for an already-resolved key.
          if (trace.kind === 'tool_result' && !existing.resolved) {
            // Re-send resultType too: the persistence layer OVERWRITES
            // generation_result with packExtras(updates), so omitting resultType
            // would drop it from the stored row — and on refresh the promoted
            // tool step would no longer be a 'trace' (its context vanishes from
            // the run activity). Keeping it here makes the tool context survive
            // a reload (it's restored from the persisted message, not just live).
            const updates = { resultType: 'trace', resultData: trace };
            if (ownerSession) {
              sessionStore.updateMessageInTargetSession(ownerSession, existing.messageId, updates);
            } else {
              sessionStore.updateMessage(existing.messageId, updates);
            }
            traceMessageIdsRef.current.set(trace.matchKey, {
              messageId: existing.messageId,
              resolved: true,
            });
          }
          handled = true;
        }
      }

      if (!handled) {
        const extra = { resultType: 'trace', resultData: trace };
        const id = ownerSession
          ? sessionStore.addMessageToTargetSession(ownerSession, 'assistant', '', extra)
          : sessionStore.addMessage('assistant', '', extra);
        if (trace.matchKey) {
          traceMessageIdsRef.current.set(trace.matchKey, {
            messageId: id,
            resolved: trace.kind === 'tool_result',
          });
        }
      }
    }

    // task_started announces WHO is about to work, BEFORE their tokens stream.
    // Previously this event was dropped as noise and the only identity in the
    // chat came from task_completed — i.e. after everything that task had
    // already streamed, so a crew read as one wall of text followed by a stack
    // of labels. Closing the open bubble here is what keeps the next task's
    // tokens under its own header.
    if ((data?.event_type as string) === 'task_started') {
      const metadata = data?.trace_metadata as Record<string, unknown> | undefined;
      const taskName = (metadata?.task_name as string) || (data?.event_context as string) || '';
      const label = taskHeaderLabel((data?.event_source as string) || '', taskName);
      if (label && jobId) {
        const store = useSessionStore.getState();
        const already = taskHeadersRef.current.get(jobId);
        // One header per task; a re-delivered trace (SSE + poller) must not
        // split the bubble again mid-answer.
        if (already !== label) {
          taskHeadersRef.current.set(jobId, label);
          headedTasksRef.current.add(taskName);
          useExecutionStore.getState().closeStreamBubble(jobId);
          const header = `**${label}**`;
          if (ownerSession) {
            store.addMessageToTargetSession(ownerSession, 'assistant', header);
          } else {
            store.addMessage('assistant', header);
          }
        }
      }
    }

    // task_completed carries a task's full output — render it (preview + chat).
    if ((data?.event_type as string) === 'task_completed') {
      const metadata = data?.trace_metadata as Record<string, unknown> | undefined;
      const taskName = (metadata?.task_name as string) || (data?.event_context as string) || 'Task';
      const rawOutput = data?.output ?? data?.result ?? message;
      const taskOutput = typeof rawOutput === 'string' ? rawOutput : JSON.stringify(rawOutput);
      handleTaskOutput(taskName, taskOutput, ownerSession, jobId);
    }
  }, [handleTaskOutput]);

  // De-dupe completion across the SSE path and the polling fallback so a run is
  // finished exactly once (completeExecution/failExecution are NOT idempotent —
  // a double call double-posts the result and to the wrong session). Keyed by
  // job id; a no-op when there is no active job, so the callbacks still fire in
  // isolation.
  const finishedJobsRef = useRef<Set<string>>(new Set());

  const finishOnce = useCallback((jobId: string | undefined, run: () => void) => {
    // Key by the COMPLETING job's id (passed in), not the global slot's active
    // job — with parallel sessions the slot may hold a different (foreground)
    // run, and keying off it would let a backgrounded job slip the de-dupe or
    // block the wrong one.
    if (jobId) {
      if (finishedJobsRef.current.has(jobId)) return;
      finishedJobsRef.current.add(jobId);
    }
    run();
  }, []);

  const postPendingActionsRow = useCallback((jobId?: string) => {
    const pending = pendingActionsRef.current;
    if (!pending) return;
    // Only for its own run: a completion for ANOTHER job (the previous run of
    // a re-sent question finishing late, a stale poller event) must leave the
    // row parked, or it lands under a bubble that is still streaming.
    const owner = jobId ? useExecutionStore.getState().jobOwnerOf(jobId) : null;
    if (!pendingActionsBelongTo(pending, jobId, owner)) return;
    pendingActionsRef.current = null;
    const sessionStore = useSessionStore.getState();
    // Anchor the row to this run's execution id so the actions bar can offer a
    // "Memory graph" link scoped to exactly this run's memory. Carry
    // the run's answer mode so the bar can hide crew-catalog actions for plain
    // 'chat' turns (there is no crew worth cataloging).
    const extra = {
      id: generateId(),
      resultType: 'crew_actions',
      // Carry the run's session so an answer-mode (chat) bar can distill a crew
      // from THIS conversation, even after the user switches sessions.
      resultData: {
        ...pending.data,
        chatModeType: pending.mode,
        sessionId: pending.ownerSession ?? useSessionStore.getState().currentSessionId,
      },
      executionId: jobId,
      // Per-run snapshot (captured at generation, not the live toggle) so the
      // "Memory graph" action only appears for runs that used workspace memory.
      usedWorkspaceMemory: pending.usedWorkspaceMemory,
      capability: pending.capability,
    };
    if (pending.ownerSession) sessionStore.addMessageToTargetSession(pending.ownerSession, 'assistant', '', extra);
    else sessionStore.addMessage('assistant', '', extra);
    // pendingActionsRef is a prop now rather than a local ref, so eslint wants it
    // listed. Ref identities are stable, so this does not change when the
    // callback is recreated.
  }, [pendingActionsRef]);

  const completeExecutionOnce = useCallback((jobId: string | undefined, resultText: string, surface?: Surface | null) => {
    // The crew subprocess announces a run TWICE on purpose: the plain answer
    // the moment the crew has it, then a second one carrying the A2UI surface,
    // which the parent can only compose after the subprocess exits (44s on one
    // measured deck). The first announcement finalizes the run; without this
    // the second is discarded as a duplicate and the deck arrives as raw
    // markdown. So a LATE completion that brings a surface is not a duplicate —
    // it is the half of the answer that was not ready yet.
    if (surface && jobId && finishedJobsRef.current.has(jobId)) {
      // The text rides along: a flow's second announcement carries the answer
      // AND the surface composed from it, and they are different content.
      useExecutionStore.getState().attachSurface(jobId, surface, resultText);
      return;
    }
    finishOnce(jobId, () => {
      const store = useExecutionStore.getState();
      // A run that answered with a PIECE of what the reader should see (a slide
      // refine returns one <section>) becomes the whole here — the run that
      // started it registered how (utils/resultTransforms). Applied once, before
      // the store reads the text.
      const text = applyResultTransform(jobId, resultText);
      // Only thread the surface arg when a rich one was composed — a plain chat
      // turn calls with the original (text, jobId) shape.
      if (surface) store.completeExecution(text, jobId, surface);
      else store.completeExecution(text, jobId);
      // Result is in — now surface the bookmark/feedback row beneath it.
      postPendingActionsRow(jobId);
    });
  }, [finishOnce, postPendingActionsRow]);

  const failExecutionOnce = useCallback((jobId: string | undefined, error: string) => {
    dropResultTransform(jobId);
    finishOnce(jobId, () => useExecutionStore.getState().failExecution(error, jobId));
  }, [finishOnce]);

  // Keep an undecided approval line BELOW the latest activity row: when a new
  // trace lands for a job with a pending approval message, bump it to the end.
  const bumpPendingApproval = useCallback((jobId?: string) => {
    if (!jobId) return;
    const sessionStore = useSessionStore.getState();
    const pending = sessionStore.messages.find(
      (m) =>
        m.resultType === 'hitl_approval' &&
        !(m.resultData as { decided?: string } | undefined)?.decided &&
        (m.resultData as { job_id?: string } | undefined)?.job_id === jobId,
    );
    if (pending) sessionStore.moveMessageToEnd(pending.id);
  }, []);

  // --- Execution Stream ---
  const executionStream = useExecutionStream({
    onTrace: (msg, data) => {
      processTrace(msg, data);
      bumpPendingApproval((data?.job_id as string) || sseJobIdRef.current || undefined);
    },
    onChunk: (chunk, data) => {
      // Route by the job id stamped on the event (parallel-session safe),
      // falling back to the stream this workspace opened.
      const jobId = (data.job_id as string) || sseJobIdRef.current;
      if (jobId) useExecutionStore.getState().appendStreamChunk(jobId, chunk);
    },
    onSurfaceDelta: (message, data) => {
      // Same job-id routing as onChunk: a delta belongs to the run that emitted
      // it, not to whichever run happens to be in the foreground.
      const jobId = (data.job_id as string) || sseJobIdRef.current;
      if (jobId) {
        // The seq rides along so a reconnect's replay cannot regress a surface
        // this client has already built past.
        useExecutionStore
          .getState()
          .applySurfaceDelta(jobId, message as A2uiMessage, data.seq as number | undefined);
      }
    },
    onStatusChange: (status) => {
      useExecutionStore.getState().updateExecutionStatus(status as ExecutionStatus);
    },
    onComplete: (data) => {
      // The event names its own job when it can; the stream ref is the fallback.
      const jobId = (data?.job_id as string) || (data?.execution_id as string) || sseJobIdRef.current;
      completeExecutionOnce(jobId, extractResultText(data), extractA2uiSurface(data));
    },
    onError: (error) => {
      failExecutionOnce(sseJobIdRef.current, error);
    },
  });

  // REST polling fallback (Job-History style). The globally-mounted
  // useTracePolling (SSEConnectionManager) polls /traces + /executions for any
  // job announced via the 'jobCreated' event (dispatched in
  // handleStartExecutionStream) and re-emits the results as window events.
  // ChatMode consumes them here so memory/Genie/tool traces + completion show
  // up even when the Databricks Apps HTTP/2 proxy kills the SSE stream.
  useEffect(() => {
    const onTraceUpdate = (e: Event) => {
      const { jobId, trace } = (e as CustomEvent).detail || {};
      // Route by the job's OWNER, not the single live slot — like the job
      // completion events below. A backgrounded session's task output (which
      // carries the rendered deliverable) arrives here when SSE is dead
      // (Databricks Apps) or after its stream was closed by a newer run; gating
      // on the live `activeExecution` dropped it, so the preview was never
      // stashed into its snapshot and vanished on switch-back. jobOwnerOf returns
      // null once a job finalizes, so a late re-poll is still dropped.
      if (!jobId || !trace || !useExecutionStore.getState().jobOwnerOf(jobId)) return;
      const msg =
        (trace.message as string) ||
        (trace.trace as string) ||
        JSON.stringify(trace);
      processTrace(msg, trace as Record<string, unknown>);
      bumpPendingApproval(jobId as string);
    };
    // Route by the job's OWNER, not the single live slot: a backgrounded
    // session's run must still finalize (and land in ITS session) even though
    // the slot currently holds a different, foreground run. jobOwnerOf returns
    // null once a job has finalized, so this also drops late duplicates.
    const onJobCompleted = (e: Event) => {
      const { jobId, result } = (e as CustomEvent).detail || {};
      if (!jobId) return;
      const surface = extractA2uiSurface({ result });
      // jobOwnerOf returns null both for a job that has FINALIZED and for one
      // this workspace never tracked. Only the first can still take a surface:
      // the crew subprocess announces twice on purpose (plain text, then the
      // composed surface), and that second announcement must still land.
      //
      // The distinction matters. Letting an UNTRACKED job through would run
      // finishOnce — marking it finished — while completeExecution bails on the
      // missing owner without posting anything, so the real completion that
      // followed was then blocked as a duplicate and the answer vanished.
      const isLateSurface = !!surface && finishedJobsRef.current.has(jobId);
      if (!useExecutionStore.getState().jobOwnerOf(jobId) && !isLateSurface) return;
      completeExecutionOnce(jobId, extractResultText({ result }), surface);
    };
    const onJobFailed = (e: Event) => {
      const { jobId, error } = (e as CustomEvent).detail || {};
      if (!jobId || !useExecutionStore.getState().jobOwnerOf(jobId)) return;
      failExecutionOnce(jobId, (error as string) || 'Execution failed');
    };
    const onJobStopped = (e: Event) => {
      const { jobId } = (e as CustomEvent).detail || {};
      if (!jobId || !useExecutionStore.getState().jobOwnerOf(jobId)) return;
      failExecutionOnce(jobId, 'Execution stopped');
    };
    // The poller hit a definitive 404 loop: the job's row no longer exists for
    // this workspace (deleted, or a different group). Abandon it — drop the
    // running banner + the durable reconnect marker — so neither the poller nor a
    // refresh resurrects it. Routed by owner like the completion events, and a
    // no-op once the job is untracked (e.g. the reconnect backstop already
    // abandoned it). Deliberately posts NO chat message: the run isn't a failure.
    const onJobNotFound = (e: Event) => {
      const { jobId } = (e as CustomEvent).detail || {};
      if (!jobId || !useExecutionStore.getState().jobOwnerOf(jobId)) return;
      jobOwnerRef.current.delete(jobId);
      useExecutionStore.getState().abandonExecution(jobId);
    };
    window.addEventListener('traceUpdate', onTraceUpdate as EventListener);
    window.addEventListener('jobCompleted', onJobCompleted as EventListener);
    window.addEventListener('jobFailed', onJobFailed as EventListener);
    window.addEventListener('jobStopped', onJobStopped as EventListener);
    window.addEventListener('jobNotFound', onJobNotFound as EventListener);
    return () => {
      window.removeEventListener('traceUpdate', onTraceUpdate as EventListener);
      window.removeEventListener('jobCompleted', onJobCompleted as EventListener);
      window.removeEventListener('jobFailed', onJobFailed as EventListener);
      window.removeEventListener('jobStopped', onJobStopped as EventListener);
      window.removeEventListener('jobNotFound', onJobNotFound as EventListener);
    };
  }, [processTrace, completeExecutionOnce, failExecutionOnce, bumpPendingApproval]);

  // --- Inline tool-approval cards ---
  // Chat never shows modal dialogs: an hitl_request for a job owned by a chat
  // session renders as an approval card in that session's conversation (the
  // global ToolApprovalListener skips chat-owned jobs). Deduped by approval id
  // so an SSE replay on reconnect doesn't post the card twice.
  const seenApprovalsRef = useRef<Set<string>>(new Set());

  useEffect(() => {
    const onHitlRequest = (e: Event) => {
      const detail = (e as CustomEvent).detail as Record<string, unknown> | undefined;
      const jobId = detail?.job_id as string | undefined;
      const approvalId = detail?.approval_id;
      if (!jobId || approvalId === undefined || approvalId === null) return;
      const owner = useExecutionStore.getState().jobOwnerOf(jobId);
      if (!owner) return;
      const key = String(approvalId);
      if (seenApprovalsRef.current.has(key)) return;
      seenApprovalsRef.current.add(key);
      useSessionStore.getState().addMessageToTargetSession(owner, 'assistant', '', {
        id: `hitl-${key}`,
        resultType: 'hitl_approval',
        resultData: detail,
      });
    };
    window.addEventListener('hitlRequest', onHitlRequest);
    return () => window.removeEventListener('hitlRequest', onHitlRequest);
  }, []);

  // --- Execution handlers ---
  const handleStartExecutionStream = useCallback(
    (jobId: string, sessionId?: string, opts?: { preservePreview?: boolean }) => {
      const origin = sessionId || useSessionStore.getState().currentSessionId;
      // Remember which session owns this job, so its traces/output are routed
      // back to it by job_id even if the user switches sessions mid-run.
      if (origin) jobOwnerRef.current.set(jobId, origin);
      // A parked actions row that does not know its run yet (generate-only
      // turns carry no execution_id) belongs to the run its session starts now.
      const pending = pendingActionsRef.current;
      if (pending && !pending.jobId && (!pending.ownerSession || !origin || pending.ownerSession === origin)) {
        pending.jobId = jobId;
      }
      useExecutionStore.getState().startExecution(jobId, origin || undefined, opts);
      // Only seize the single live SSE stream when the run's OWNER is on screen.
      // A backgrounded run (a generation that finished for another session while
      // you're elsewhere) must not take over the viewed session's stream — its
      // traces/completion still arrive via the global poller (jobCreated below),
      // routed back by job owner.
      const viewingOwner = !origin || origin === useSessionStore.getState().currentSessionId;
      if (viewingOwner) {
        traceMessageIdsRef.current.clear();
        seenTraceIdsRef.current.clear();
        // This job now owns the single live SSE stream.
        sseJobIdRef.current = jobId;
        executionStream.startStream(jobId);
      }
      // Announce the job so the globally-mounted useTracePolling
      // (SSEConnectionManager) starts its REST polling fallback for it. When the
      // Databricks Apps HTTP/2 proxy kills SSE, the poller delivers traces +
      // completion via 'traceUpdate'/'jobCompleted' window events (see the
      // listener effect above), so memory/Genie/tool traces still render.
      // groupId is required: runStatus drops jobCreated events without it
      // (workspace-isolation check), and a dropped run never enters activeRuns
      // — so the 10s reconciliation loop can't finalize it if the poller is
      // retargeted to a newer job before the first status flip is observed.
      window.dispatchEvent(new CustomEvent('jobCreated', {
        detail: { jobId, groupId: localStorage.getItem('selectedGroupId') || undefined },
      }));
    },
    [executionStream],
  );

  // Close any open generation streams when ChatMode is fully torn down. It's kept
  // mounted across app-mode switches (so streams survive those), so this fires
  // only on a real unmount (leaving the workspace) — not on mode toggles.
  useEffect(() => () => stopAllGenerationStreams(), []);

  // Reconnect to a still-running crew after a page refresh. The in-memory store
  // is wiped on reload, so without this the Stop button (and live updates)
  // vanish even though the backend job is still running. The running job is
  // persisted per session in IndexedDB; when a session becomes active we read
  // it (async), re-attach the SSE stream + execution state, and verify status.
  // Attempted once per session id (covers refresh on the running session, and
  // switching to it afterwards).
  // In-flight guard so a re-render can't fire a duplicate reconnect for the SAME
  // session while its (async) marker read is pending. Unlike a once-ever Set,
  // this RESETS after each attempt, so switching BACK to a still-running session
  // re-detects and restores it — the bug where switch-away/return lost the
  // monitoring while a refresh (fresh component) brought it back.
  const reconnectingRef = useRef<string | null>(null);

  // Job ids proven gone (a 404 during the reconnect backstop, or finalized as
  // already-finished). Without this the effect would re-attach the SAME dead job
  // on every re-render: handleStartExecutionStream re-persists the IndexedDB
  // marker, abandonExecution clears activeExecution (removing the re-entry guard
  // below) and async-clears the marker — so a re-run that reads the not-yet-
  // cleared marker re-attaches → 404 → abandon → loop (tight render loop, screen
  // flicker, 404 storm). A dead job id never returns (UUIDs), so we never clear it.
  const deadJobsRef = useRef<Set<string>>(new Set());

  useEffect(() => {
    const sid = currentSessionId;
    if (!sid || reconnectingRef.current === sid) return;
    const st = useExecutionStore.getState();
    // Already showing THIS session's run as active (snapshot restore handled it)
    // — nothing to reconnect.
    if (st.executionOwnerSessionId === sid && (st.isExecuting || st.isGenerating)) return;
    // A DIFFERENT run holds the live slot — don't clobber it.
    if (st.activeExecution) return;
    // Detect a running job for this session: the Zustand store first (survives
    // session switches in memory), then the IndexedDB marker (covers refresh,
    // where the in-memory store was wiped).
    const known = st.runningJobBySession[sid];
    reconnectingRef.current = sid;
    // Cancelled on unmount so an in-flight reconnect never re-attaches after the
    // component is gone (also keeps tests isolated: a prior render's pending async
    // can't fire startExecution into the next test).
    let cancelled = false;
    (async () => {
      const jobId = known || (await readActiveExecution(sid));
      // Re-check after the async read: still on this session, still nothing active.
      if (!jobId || useSessionStore.getState().currentSessionId !== sid) {
        reconnectingRef.current = null;
        return;
      }
      if (useExecutionStore.getState().activeExecution) {
        reconnectingRef.current = null;
        return;
      }
      // Already proven gone this session — don't re-attach it (would loop: the
      // marker clear is async, so a stale re-read could resurrect it otherwise).
      if (deadJobsRef.current.has(jobId)) {
        reconnectingRef.current = null;
        return;
      }

      // Restore the running state OPTIMISTICALLY so the Stop button reappears
      // immediately, and re-attach the SSE stream (its replay buffer + future
      // events drive live updates and completion). We deliberately do NOT gate
      // this on a status fetch — if that fetch failed or returned an unexpected
      // shape, the Stop button would vanish even though the crew is still
      // running, which is exactly the bug we're fixing.
      if (cancelled || useSessionStore.getState().currentSessionId !== sid) {
        reconnectingRef.current = null;
        return;
      }
      handleStartExecutionStream(jobId, sid, { preservePreview: true });

      // Backstop: if the job had ALREADY finished before the refresh, drop the
      // (now stale) running state so the Stop button doesn't linger. Only acts
      // on a definitively-terminal status; anything else keeps the optimistic
      // state and lets the SSE stream resolve it.
      try {
        const exec = await getExecutionStatus(jobId);
        const status = String(exec?.status || '').toLowerCase();
        const finished = ['completed', 'failed', 'stopped', 'cancelled', 'error'].includes(status);
        if (finished && useExecutionStore.getState().activeExecution?.jobId === jobId) {
          deadJobsRef.current.add(jobId);
          executionStream.stopStream();
          useExecutionStore.setState({
            isExecuting: false,
            isLoading: false,
            activeExecution: null,
            executionOwnerSessionId: null,
          });
          clearActiveExecution(sid);
          // We finalized this job directly (it was already done) — drop its
          // owner mapping so a late poller event can't re-post a completion, and
          // drop the Zustand switch-back entry so we don't re-detect a dead run.
          useExecutionStore.getState().clearJobOwner(jobId);
          useExecutionStore.setState((s) => {
            if (!(sid in s.runningJobBySession)) return {};
            const next = { ...s.runningJobBySession };
            delete next[sid];
            return { runningJobBySession: next };
          });
        }
      } catch (err) {
        // A 404 means the run no longer exists for this workspace (deleted, or it
        // belongs to a group you no longer have selected). Without this, the
        // optimistic running state + the IndexedDB reconnect marker persist, so
        // the global poller hammers /executions + /traces every 2s with 404s and
        // the NEXT refresh re-detects the dead job and resumes the storm. Treat it
        // as terminal: stop the stream and abandon the job (clears the marker +
        // the running banner). Any OTHER error (offline / 5xx / transient) keeps
        // the optimistic state — the SSE stream / next poll stays the source of truth.
        const httpStatus = (err as { response?: { status?: number } })?.response?.status;
        if (httpStatus === 404) {
          deadJobsRef.current.add(jobId);
          executionStream.stopStream();
          jobOwnerRef.current.delete(jobId);
          useExecutionStore.getState().abandonExecution(jobId);
          // handleStartExecutionStream above dispatched 'jobCreated', arming the
          // global poller's grace timer. Tell it to stand down now so it never
          // starts hammering this dead job (no residual 404 tail).
          window.dispatchEvent(new CustomEvent('jobNotFound', { detail: { jobId } }));
        }
        // else: offline / transient — keep optimistic state; SSE/next poll resolves it.
      } finally {
        // Allow a future switch-back to re-detect (the guard is per-attempt, not
        // once-ever) — this is what makes switching away and returning restore
        // the monitoring, like a refresh does.
        reconnectingRef.current = null;
      }
    })();
    return () => { cancelled = true; reconnectingRef.current = null; };
  }, [currentSessionId, handleStartExecutionStream, executionStream]);

  return { executionStream, handleStartExecutionStream };
}
