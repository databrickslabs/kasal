/**
 * Whether the parked actions row (Save to catalog / Schedule / Memory graph)
 * belongs to the run that just completed.
 *
 * The row is armed at generation time and posted when a completion arrives —
 * but completions can arrive for OTHER runs: re-sending a question while the
 * previous run is still finishing, or a late poller event for a job whose
 * stream was already replaced. Posting the row on any completion put it under
 * a bubble that was still streaming. The row must only post for its own run.
 */
export interface PendingActionsBinding {
  /** The run the row belongs to, once known (null while unknown). */
  jobId?: string | null;
  /** The session that dispatched the run. */
  ownerSession?: string | null;
}

export function pendingActionsBelongTo(
  pending: PendingActionsBinding,
  completedJobId: string | undefined,
  ownerOfCompletedJob: string | null | undefined,
): boolean {
  if (pending.jobId && completedJobId && pending.jobId !== completedJobId) return false;
  if (pending.ownerSession && ownerOfCompletedJob && pending.ownerSession !== ownerOfCompletedJob) {
    return false;
  }
  return true;
}
