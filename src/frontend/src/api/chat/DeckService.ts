import { useExecutionStore } from '../../features/chat/store/executionStore';
import { apiClient } from '../../shared/api/client';

const BASE = '/decks';

/** Revise one slide (`refine`) or write a new one between two (`add`). */
export interface SlideRefineRequest {
  mode: 'refine' | 'add';
  tools?: string[];
  mcp_servers?: string[];
  agentbricks_endpoints?: string[];
  skills?: string[];
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
  /** Launch the shared light agent; expose its trace immediately, then await one slide. */
  async refineSlide(req: SlideRefineRequest, onStarted?: (jobId: string) => void): Promise<SlideRefineResult> {
    const selected = useExecutionStore.getState();
    const { data } = await apiClient.post<{ job_id: string }>(`${BASE}/slides/refine/start`, {
      mcp_servers: selected.selectedMcpServers,
      agentbricks_endpoints: selected.selectedAgentBricksEndpoints,
      skills: selected.selectedSkills,
      ...req,
      model: req.model || null,
    }, { timeout: 30000 });
    const jobId = data.job_id;
    onStarted?.(jobId);
    const deadline = Date.now() + 15 * 60 * 1000;
    while (Date.now() < deadline) {
      try {
        const { data: run } = await apiClient.get<{
          status: string; result?: { section?: string; model?: string }; error?: string;
        }>(`/executions/${encodeURIComponent(jobId)}`, { timeout: 15000 });
        const status = run.status.toUpperCase();
        if (status === 'COMPLETED') {
          return { job_id: jobId, section: run.result?.section || null, model: run.result?.model,
            error: run.result?.section ? null : 'The agent did not return a complete slide. Check the execution trace.' };
        }
        if (['FAILED', 'CANCELLED', 'CANCELED', 'STOPPED'].includes(status)) {
          return { job_id: jobId, section: null, error: run.error || `Slide edit ${status.toLowerCase()}.` };
        }
      } catch (error) {
        const status = (error as { response?: { status?: number } }).response?.status;
        if (status && status >= 400 && status < 500 && status !== 429) {
          return { job_id: jobId, section: null, error: 'Cannot read the slide-edit run. Check the execution trace.' };
        }
        // Retry reads after a transient interruption; never launch a duplicate run.
      }
      await new Promise(resolve => setTimeout(resolve, 2000));
    }
    return { job_id: jobId, section: null, error: 'Still waiting for the slide-edit run. Check its execution trace before retrying.' };
  },
};

export default DeckService;
