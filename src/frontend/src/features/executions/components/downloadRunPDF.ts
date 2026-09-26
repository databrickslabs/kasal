import toast from 'react-hot-toast';
import type { Run } from '../../../api/execution/ExecutionHistoryService';
import { isChunkLoadError } from '../../../shared/errors/chunkErrors';

/**
 * Generate and download a run's PDF, telling the user when it fails.
 *
 * `@react-pdf/renderer` is large, so the generator is a lazy chunk loaded on
 * first use. The call sites used to `void` the promise: a failed chunk load
 * (typically after a redeploy) or a render error then did nothing visible.
 * This never rejects; every failure becomes an error toast.
 */
export async function downloadRunPDF(run: Run): Promise<void> {
  try {
    const { generateRunPDF } = await import('../../../utils/pdfGenerator');
    await generateRunPDF(run);
  } catch (error) {
    console.error('Could not generate the run PDF', error);
    toast.error(
      isChunkLoadError(error)
        ? 'The PDF generator could not be loaded, usually because Kasal was updated. Reload the page and try again.'
        : 'Could not generate the PDF for this run.',
    );
  }
}
