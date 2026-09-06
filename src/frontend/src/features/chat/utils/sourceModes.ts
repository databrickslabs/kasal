/**
 * Where the next answer comes FROM: something built now, or something saved.
 *
 * A different question from the answer-mode pill, which asks what SHAPE to
 * build. Kept in its own file for that reason — `answerModes.ts` is deliberately
 * untouched by this feature. The moment reuse becomes a fourth answer mode it is
 * a value that invalidates its own neighbours: the catalogue only stores crews,
 * so reuse can never honour 'chat', and a reuse mode that matched nothing would
 * quietly turn into Research.
 */

export type SourceModeId = 'build' | 'existing';

export const SOURCE_MODES: {
  id: SourceModeId;
  label: string;
  short: string;
  hint: string;
}[] = [
  {
    id: 'build',
    label: 'Build new',
    short: 'Build new',
    hint: 'Create a crew for this question and run it',
  },
  {
    id: 'existing',
    label: 'Use existing',
    short: 'Use existing',
    hint: 'Run a crew or flow already published to chat',
  },
];

/**
 * True when the workspace is KNOWN to have nothing published to chat.
 *
 * Deliberately not `count === 0`: a count that has not loaded yet is `null`, and
 * an unloaded list must NOT disable the control. Same stance as
 * `modelLacksReasoning` in `answerModes.ts`, for the same reason — a false
 * "unavailable" is worse than briefly offering something that turns out to be a
 * no-op, because the user believes it and stops looking.
 */
export function nothingPublishedToChat(count: number | null): boolean {
  return count !== null && count === 0;
}

/** Why "Use existing" is unavailable — a signpost, not a dead end. */
export const NOTHING_PUBLISHED_REASON =
  'Publish a crew or flow to chat to use this.';

/**
 * Why the answer-mode pill goes quiet while "Use existing" is selected.
 *
 * The disabled state is the point of the two-control design. A saved crew
 * carries its own agents, tasks, process and model, so there is nothing left for
 * the effort dial to act on — and saying so makes the dependency visible, where
 * a fourth answer-mode chip would have made it silent.
 */
export const ANSWER_MODE_LOCKED_REASON =
  'The saved crew defines its own agents and model, so there is nothing to choose here.';
