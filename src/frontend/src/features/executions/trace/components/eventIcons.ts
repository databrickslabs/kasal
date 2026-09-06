import React from 'react';
import CheckCircleIcon from '@mui/icons-material/CheckCircle';
import HistoryIcon from '@mui/icons-material/History';
import SaveIcon from '@mui/icons-material/Save';
import PlayCircleIcon from '@mui/icons-material/PlayCircle';
import PreviewIcon from '@mui/icons-material/Preview';
import TerminalIcon from '@mui/icons-material/Terminal';
import RefreshIcon from '@mui/icons-material/Refresh';
import PlayArrowIcon from '@mui/icons-material/PlayArrow';
import WarningAmberIcon from '@mui/icons-material/WarningAmber';
import ErrorOutlineIcon from '@mui/icons-material/ErrorOutline';
import StorageIcon from '@mui/icons-material/Storage';
import TimelineIcon from '@mui/icons-material/Timeline';
import CompressIcon from '@mui/icons-material/Compress';
import DashboardIcon from '@mui/icons-material/Dashboard';
import ChecklistIcon from '@mui/icons-material/Checklist';

interface IconConfig {
  Component: React.ElementType;
  color: 'primary' | 'success' | 'error' | 'warning' | 'info' | 'action' | 'inherit';
}

export const ICON_CONFIG: Record<string, IconConfig> = {
  tool: { Component: TerminalIcon, color: 'primary' },
  tool_result: { Component: TerminalIcon, color: 'success' },
  tool_usage: { Component: TerminalIcon, color: 'action' },
  llm: { Component: PlayCircleIcon, color: 'primary' },
  llm_response: { Component: PlayCircleIcon, color: 'success' },
  agent_start: { Component: PlayArrowIcon, color: 'primary' },
  task_start: { Component: PlayArrowIcon, color: 'primary' },
  started: { Component: PlayArrowIcon, color: 'primary' },
  agent_complete: { Component: CheckCircleIcon, color: 'success' },
  task_complete: { Component: CheckCircleIcon, color: 'success' },
  completed: { Component: CheckCircleIcon, color: 'success' },
  agent_output: { Component: PreviewIcon, color: 'action' },
  agent_execution: { Component: PreviewIcon, color: 'action' },
  agent_processing: { Component: RefreshIcon, color: 'action' },
  memory_write: { Component: StorageIcon, color: 'primary' },
  memory_retrieval: { Component: StorageIcon, color: 'success' },
  memory_context: { Component: StorageIcon, color: 'info' },
  memory_operation: { Component: StorageIcon, color: 'action' },
  memory_backend_error: { Component: ErrorOutlineIcon, color: 'error' },
  knowledge_operation: { Component: TimelineIcon, color: 'action' },
  crew_started: { Component: PlayCircleIcon, color: 'primary' },
  crew_completed: { Component: CheckCircleIcon, color: 'success' },
  // A crew a resume RESTORED from a checkpoint rather than executed. Its own
  // icon and colour on purpose: the timeline shows the whole flow, but restored
  // work must not read as work that just ran.
  crew_checkpoint_restored: { Component: HistoryIcon, color: 'info' },
  task_checkpoint_restored: { Component: HistoryIcon, color: 'info' },
  // A checkpoint being WRITTEN. Muted on purpose — it is the bookkeeping a
  // resume depends on, not work the user asked for — but present, because
  // "nothing was written" and "it was written and ignored" were previously
  // indistinguishable without querying the database.
  flow_checkpoint_saved: { Component: SaveIcon, color: 'action' },
  // A completed unit written to the checkpoint — the CREW path's writes,
  // which had no icon at all and so never appeared in a crew's timeline.
  checkpoint_unit_saved: { Component: SaveIcon, color: 'action' },
  flow_started: { Component: PlayCircleIcon, color: 'primary' },
  flow_created: { Component: PlayCircleIcon, color: 'primary' },
  flow_completed: { Component: CheckCircleIcon, color: 'success' },
  mcp_connection: { Component: TerminalIcon, color: 'info' },
  mcp_tool: { Component: TerminalIcon, color: 'primary' },
  mcp_tool_result: { Component: TerminalIcon, color: 'success' },
  hitl_request: { Component: WarningAmberIcon, color: 'warning' },
  hitl_response: { Component: CheckCircleIcon, color: 'info' },
  tool_error: { Component: ErrorOutlineIcon, color: 'error' },
  rate_limit: { Component: WarningAmberIcon, color: 'warning' },
  task_failed: { Component: ErrorOutlineIcon, color: 'error' },
  flow_execution_failed: { Component: ErrorOutlineIcon, color: 'error' },
  error: { Component: ErrorOutlineIcon, color: 'error' },
  guardrail: { Component: CheckCircleIcon, color: 'warning' },
  llm_request: { Component: PlayCircleIcon, color: 'primary' },
  // Compaction is LOSSY — warning-coloured on purpose. A run that
  // compacts repeatedly is losing tool results it may still need.
  context_compaction: { Component: CompressIcon, color: 'warning' },
  // A composed surface is a deliverable; a skipped one is information, not a
  // fault — muted rather than warning-coloured.
  a2ui_surface: { Component: DashboardIcon, color: 'success' },
  a2ui_skipped: { Component: DashboardIcon, color: 'action' },
  // Progress, not an outcome — muted, so a plan row never reads as a step that
  // succeeded or failed.
  plan_updated: { Component: ChecklistIcon, color: 'action' },
};

/**
 * Get icon configuration for an event type
 */
export function getEventIcon(type: string): { Component: React.ElementType | null; color: string } {
  const config = ICON_CONFIG[type];
  if (config) {
    return { Component: config.Component, color: config.color };
  }
  return { Component: null, color: 'inherit' };
}
