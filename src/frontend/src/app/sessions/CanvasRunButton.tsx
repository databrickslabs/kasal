import React, { useEffect, useState } from 'react';
import { CircularProgress, IconButton, Tooltip } from '@mui/material';
import { Play, Square } from 'lucide-react';
import type { Edge } from 'reactflow';
import { useBuilderExecutionControls } from '../../store/builderExecutionControls';

interface Props {
  mode: 'crew' | 'flow';
  hasNodes: boolean;
  edges: Edge[];
  onRun: () => void;
}

export default function CanvasRunButton({ mode, hasNodes, edges, onRun }: Props) {
  const execution = useBuilderExecutionControls(state => state[mode]);
  const [ready, setReady] = useState(false);
  useEffect(() => {
    let timer: ReturnType<typeof setTimeout>;
    const highlight = () => {
      setReady(true);
      clearTimeout(timer);
      timer = setTimeout(() => setReady(false), 3000);
    };
    window.addEventListener('crew-ready', highlight);
    return () => {
      clearTimeout(timer);
      window.removeEventListener('crew-ready', highlight);
    };
  }, [mode]);

  const canRun = hasNodes && (mode === 'crew' || edges.every(edge =>
    edge.data?.listenToTaskIds?.length > 0 && edge.data?.targetTaskIds?.length > 0));
  const name = mode === 'flow' ? 'Flow' : 'Crew';
  const label = execution
    ? (execution.stopping ? 'Stopping execution' : `Stop ${name}`)
    : (!hasNodes ? `No ${name} to Execute` : !canRun ? 'Configure flow connections to run' : `Run ${name}`);

  return <Tooltip title={label}>
    <span style={{ display: 'inline-flex' }}>
      <IconButton
        data-tour="play-execution"
        aria-label={label}
        disabled={execution ? execution.stopping : !canRun}
        onClick={execution ? execution.stop : onRun}
        sx={{
          width: 44, height: 44, borderRadius: 3, color: 'text.primary',
          bgcolor: 'transparent', boxShadow: 'none',
          opacity: ready || execution ? 1 : 0.85,
          transition: 'background-color 0.2s',
        }}
      >
        {execution?.stopping ? <CircularProgress size={16} color="inherit" />
          : execution ? <Square size={20} fill="currentColor" /> : <Play size={20} fill="currentColor" />}
      </IconButton>
    </span>
  </Tooltip>;
}
