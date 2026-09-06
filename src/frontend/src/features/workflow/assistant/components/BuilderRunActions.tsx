import React, { useEffect, useState } from 'react';
import { Box, Dialog, IconButton, Typography } from '@mui/material';
import CloseIcon from '@mui/icons-material/Close';
import { load } from 'js-yaml';
import { runService } from '../../../../api/execution/ExecutionHistoryService';
import { useThemeStore } from '../../../../store/theme';
import type { Run } from '../../../../types/execution/run';
import CompletedRunActions from '../../../chat/components/Cards/CompletedRunActions';
import MemoryPane from '../../../chat/components/Preview/MemoryPane';
import '../../../chat/chat.css';

/** Read the saved execution configuration, never the current canvas's settings. */
export function runUsedMemory(run: Run): boolean {
  const config = run.inputs;
  if (config?.disable_memory === true || config?.memory === false) return false;
  if (config?.memory === true) return true;
  try {
    const agents = config?.agents_yaml ?? load(run.agents_yaml || '{}');
    return !!agents && typeof agents === 'object' && Object.values(agents).some(
      (agent) => agent && typeof agent === 'object' && agent.memory === true,
    );
  } catch { return false; }
}

const BuilderRunActions: React.FC<{ jobId: string }> = ({ jobId }) => {
  const dark = useThemeStore((s) => s.isDarkMode);
  const [run, setRun] = useState<Run | null>(null);
  const [memoryOpen, setMemoryOpen] = useState(false);
  useEffect(() => {
    let active = true;
    setRun(null);
    setMemoryOpen(false);
    void runService.getRunByJobId(jobId).then((result) => {
      if (active) setRun(result);
    }).catch(() => { /* A missing historical execution cannot be scheduled. */ });
    return () => { active = false; };
  }, [jobId]);

  if (!run || !['completed', 'complete', 'success', 'succeeded'].includes(run.status.toLowerCase())) return null;
  return (
    <div className="kasal-chat-root" data-theme={dark ? 'dark' : 'light'}>
      <div className="flex items-center gap-2 flex-wrap mt-2">
        <CompletedRunActions
          executionId={jobId}
          defaultName={`${run.run_name || 'Crew'} schedule`}
          usedWorkspaceMemory={runUsedMemory(run)}
          onOpenMemory={() => setMemoryOpen(true)}
        />
      </div>
      <Dialog open={memoryOpen} onClose={() => setMemoryOpen(false)} maxWidth="md" fullWidth
        aria-labelledby="builder-run-memory-title"
        PaperProps={{ className: 'kasal-chat-root', 'data-theme': dark ? 'dark' : 'light',
          sx: { borderRadius: 3, backgroundImage: 'none', bgcolor: 'background.paper' } } as React.ComponentProps<typeof Dialog>['PaperProps']}>
        <Box sx={{ display: 'flex', alignItems: 'center', px: 2.5, pt: 1.5 }}>
          <Typography id="builder-run-memory-title" sx={{ flex: 1, fontWeight: 600 }}>Run memory</Typography>
          <IconButton aria-label="Close memory" onClick={() => setMemoryOpen(false)}><CloseIcon fontSize="small" /></IconButton>
        </Box>
        <Box sx={{ overflowY: 'auto' }}><MemoryPane runId={jobId} /></Box>
      </Dialog>
    </div>
  );
};

export default BuilderRunActions;
