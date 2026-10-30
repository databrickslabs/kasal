import { SkillService, type TranscriptTurn } from '../../../api/tools/SkillService';
import type { ChatMessage } from '../types/chat';
import {
  draftFailedStep,
  draftMessage,
  draftedStep,
  draftingStep,
  type SkillCommand,
} from './skillCommand';
import type { TraceEntry } from './traceActivity';

/** Where the draft's messages go — the session that asked, bound by the caller. */
export interface SkillDraftIO {
  /** Post an assistant message; its id. */
  post: (content: string, extra?: Partial<ChatMessage>) => string;
  /** Update a message posted earlier. */
  update: (id: string, updates: Partial<ChatMessage>) => void;
}

/**
 * Draft a skill as a run the user can open.
 *
 * The drafting call is ONE step in the prompt's run activity. The backend
 * answers with the run's job id before the model is called, and that id goes
 * onto the step at once: the activity (RunProgress) reads a run's trace by the
 * `executionId` of its trace messages, so expanding it shows the LLM request
 * while the model is still working, then the response — the same rows, from the
 * same OTel bridge, as any other run.
 */
export async function runSkillDraft(
  cmd: SkillCommand,
  transcript: TranscriptTurn[] | undefined,
  model: string | undefined,
  io: SkillDraftIO,
): Promise<void> {
  const startedAt = Date.now();
  const drafting = draftingStep(cmd, transcript?.length ?? 0, model);
  const stepId = io.post('', { resultType: 'trace', resultData: drafting });
  let jobId: string | undefined;
  const setStep = (resultData: TraceEntry) =>
    io.update(stepId, { resultType: 'trace', resultData, ...(jobId ? { executionId: jobId } : {}) });
  try {
    const draft = await SkillService.draftWithTrace(cmd.request, transcript, model, (started) => {
      jobId = started;
      setStep(drafting);
    });
    jobId = draft.job_id || jobId;
    setStep(draftedStep(draft, startedAt));
    io.post(draftMessage(draft));
  } catch (error) {
    const errMsg = errorText(error);
    // A run that FAILED still has its trace — keep it openable.
    setStep(draftFailedStep(errMsg, startedAt));
    io.post(`Could not draft the skill: ${errMsg}`);
  }
}

function errorText(error: unknown): string {
  const detail = (error as { response?: { data?: { detail?: unknown } } })?.response?.data?.detail;
  if (typeof detail === 'string') return detail;
  return error instanceof Error ? error.message : 'Failed to draft the skill';
}
