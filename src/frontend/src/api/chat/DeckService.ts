import { apiClient } from '../../config/api/ApiConfig';

const BASE = '/decks';

/** Revise one slide (`refine`) or write a new one between two (`add`). */
export interface SlideRefineRequest {
  mode: 'refine' | 'add';
  instruction: string;
  /** The slide to revise (refine). */
  slide?: string;
  /** A slide whose design to match. */
  reference?: string;
  /** The neighbours of a new slide (add). */
  before?: string;
  after?: string;
  /** Where it sits, e.g. "3 of 8". */
  position?: string;
  /** Model key from the chat picker. */
  model?: string | null;
}

/** One `<section class="slide">`, or `error` when no slide came back. */
export interface SlideRefineResult {
  section: string | null;
  error?: string | null;
  /** The model that served the call (resolved). */
  model?: string | null;
  /** LLM calls made: 2 when the first reply held no slide. */
  attempts?: number;
  /** The run recording the call — its trace is the run activity. */
  job_id?: string | null;
  duration_ms?: number;
}

export const DeckService = {
  /**
   * One focused generation call for ONE slide. The backend records it as a
   * run (traced LLM call); the caller splices the section into the deck.
   */
  async refineSlide(req: SlideRefineRequest): Promise<SlideRefineResult> {
    const { data } = await apiClient.post<SlideRefineResult>(`${BASE}/slides/refine`, {
      ...req,
      model: req.model || null,
    });
    return data;
  },
};

export default DeckService;
