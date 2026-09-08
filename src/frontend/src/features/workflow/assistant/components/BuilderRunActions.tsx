import React, { useContext, useEffect, useState } from 'react';
import { Box, Dialog, IconButton, Typography } from '@mui/material';
import CloseIcon from '@mui/icons-material/Close';
import { apiClient } from '../../../../shared/api/client';
import { parseRunConfig, runConfiguration, runEntities } from '../utils/runConfiguration';
import { runService } from '../../../../api/execution/ExecutionHistoryService';
import { useThemeStore } from '../../../../store/theme';
import type { Run } from '../../../../types/execution/run';
import CompletedRunActions from '../../../chat/components/Cards/CompletedRunActions';
import MemoryPane from '../../../chat/components/Preview/MemoryPane';
import { BuilderPreviewContext } from './BuilderPreviewContext';
import BuilderOptimizeAction from './BuilderOptimizeAction';
import '../../../chat/chat.css';

/** Read the saved execution configuration, never the current canvas's settings. */
export function runUsedMemory(run: Run): boolean {
  const config = runConfiguration(run);
  const inputs = parseRunConfig(config.inputs);
  if (config.disable_memory === true || inputs.disable_memory === true || config.memory === false) return false;
  if (config.memory === true || inputs.memory === true) return true;
  const agents = runEntities(config.agents_yaml, run.agents_yaml);
  return Object.values(agents).some(agent => agent?.memory === true || agent?.memory === 'true');
}

const BuilderRunActions: React.FC<{ jobId: string }> = ({ jobId }) => {
  const openPreview = useContext(BuilderPreviewContext);
  const dark = useThemeStore((s) => s.isDarkMode);
  const [run, setRun] = useState<Run | null>(null);
  const [memoryOpen, setMemoryOpen] = useState(false);
  const [hasMemoryTrace, setHasMemoryTrace] = useState(false);
  useEffect(() => {
    let active = true;
    setRun(null);
    setMemoryOpen(false);
    setHasMemoryTrace(false);
    const controller = new AbortController();
    // Flow records often omit agent YAML; actual memory events are authoritative.
    void apiClient.get(`/traces/job/${jobId}`, { params: { limit: 1, event_type_prefix: 'memory_' }, signal: controller.signal }).then(({ data }) => {
      if (active) setHasMemoryTrace(Boolean(data?.traces?.length));
    }).catch(() => { /* Stored configuration remains the fallback. */ });
    void runService.getRunByJobId(jobId).then((result) => {
      if (active) setRun(result);
    }).catch(() => { /* A missing historical execution cannot be scheduled. */ });
    return () => { active = false; controller.abort(); };
  }, [jobId]);

  if (!run || !['completed', 'complete', 'success', 'succeeded'].includes(run.status.toLowerCase())) return null;
  return (
    <div className="kasal-chat-root" data-theme={dark ? 'dark' : 'light'}>
      <div className="flex items-center gap-2 flex-wrap mt-2">
        <CompletedRunActions
          executionId={jobId}
          defaultName={`${run.run_name || 'Crew'} schedule`}
          usedWorkspaceMemory={hasMemoryTrace || runUsedMemory(run)}
          onOpenSchedule={openPreview?.openSchedule}
          onOpenMemory={() => openPreview ? openPreview.openMemory(jobId) : setMemoryOpen(true)}
        />
        <BuilderOptimizeAction run={run} />
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
