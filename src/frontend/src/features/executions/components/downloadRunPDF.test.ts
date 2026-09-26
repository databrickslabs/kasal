import { describe, it, expect, vi, beforeEach } from 'vitest';
import toast from 'react-hot-toast';
import type { Run } from '../../../api/execution/ExecutionHistoryService';

const generateRunPDF = vi.fn();
vi.mock('../../../utils/pdfGenerator', () => ({ generateRunPDF: (run: Run) => generateRunPDF(run) }));
vi.mock('react-hot-toast', () => ({ default: { error: vi.fn() } }));

import { downloadRunPDF } from './downloadRunPDF';

const RUN = { id: '1', job_id: 'job-1', run_name: 'Run' } as unknown as Run;

describe('downloadRunPDF', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    vi.spyOn(console, 'error').mockImplementation(() => undefined);
  });

  it('generates the PDF without a toast when it works', async () => {
    generateRunPDF.mockResolvedValue(undefined);
    await downloadRunPDF(RUN);
    expect(generateRunPDF).toHaveBeenCalledWith(RUN);
    expect(toast.error).not.toHaveBeenCalled();
  });

  it('never rejects: a generation failure becomes an error toast', async () => {
    generateRunPDF.mockRejectedValue(new Error('render failed'));
    await expect(downloadRunPDF(RUN)).resolves.toBeUndefined();
    expect(toast.error).toHaveBeenCalledWith('Could not generate the PDF for this run.');
  });

  it('tells the user to reload when the generator chunk could not be loaded', async () => {
    generateRunPDF.mockRejectedValue(new TypeError('Failed to fetch dynamically imported module'));
    await downloadRunPDF(RUN);
    expect(vi.mocked(toast.error).mock.calls[0][0]).toMatch(/Reload the page/);
  });
});
