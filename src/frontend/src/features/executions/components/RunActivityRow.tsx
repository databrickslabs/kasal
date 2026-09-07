import React from 'react';
import { Box, Button, Typography } from '@mui/material';
import { Bot, ChevronDown, GitBranch, MessageSquare } from 'lucide-react';
import type { Run } from '../../../types/execution/run';
import ExecutionStatusBadge from './ExecutionStatusBadge';
import ExecutionMemoryButton from './ExecutionMemoryButton';
import RecipeCurationButton from './RecipeCurationButton';
import RunDuration from './RunDuration';

function countEntries(value: unknown): number | null {
  try {
    const parsed = typeof value === 'string' ? JSON.parse(value) : value;
    return parsed && typeof parsed === 'object' ? Object.keys(parsed).length : null;
  } catch { return null; }
}
function startedAt(value: string): Date {
  return new Date(/(?:Z|[+-]\d\d:\d\d)$/i.test(value) ? value : `${value}Z`);
}

export default function RunActivityRow({ run, expanded, showSubmitter, onToggle, onStatusChange, actions }: {
  run: Run; expanded: boolean; showSubmitter: boolean; onToggle: () => void;
  onStatusChange: () => void; actions: React.ReactNode;
}) {
  const name = run.run_name?.replace(/^"|"$/g, '') || run.job_id;
  const flow = run.execution_type === 'flow' || run.inputs?.execution_type === 'flow' || Boolean(run.flow_id || run.inputs?.flow_id);
  const chat = run.execution_type === 'agent' || run.inputs?.execution_type === 'agent';
  const agents = countEntries(run.inputs?.agents_yaml ?? run.agents_yaml);
  const tasks = countEntries(run.inputs?.tasks_yaml ?? run.tasks_yaml);
  const nodes = Array.isArray(run.inputs?.nodes) ? run.inputs.nodes.length : null;
  const detail = chat ? 'Chat' : flow ? (nodes === null ? 'Flow' : `Flow · ${nodes} ${nodes === 1 ? 'node' : 'nodes'}`)
    : ['Crew', agents === null ? null : `${agents} ${agents === 1 ? 'agent' : 'agents'}`, tasks === null ? null : `${tasks} ${tasks === 1 ? 'task' : 'tasks'}`].filter(Boolean).join(' · ');
  const created = startedAt(run.created_at);
  const validDate = Number.isFinite(created.getTime());
  const failure = typeof run.error === 'string' ? run.error : typeof run.result?.error === 'string' ? run.result.error : null;
  return <Box component="li" sx={{ p: { xs: 1.25, sm: 1.75 }, mb: 0.75, borderRadius: 3,
    bgcolor: expanded ? 'action.hover' : 'transparent', '&:hover': { bgcolor: 'action.hover' }, transition: 'background-color 150ms' }}>
    <Box sx={{ display: 'grid', gridTemplateColumns: { xs: 'minmax(0, 1fr) auto', sm: 'minmax(0, 1fr) 120px 130px auto' }, alignItems: 'center', gap: { xs: 1, sm: 2 } }}>
      <Box sx={{ minWidth: 0 }}>
        <Button color="inherit" onClick={onToggle} aria-expanded={expanded} aria-label={`Details for ${name}`}
          sx={{ p: 0, minWidth: 0, maxWidth: '100%', gap: 0.75, textAlign: 'left', justifyContent: 'flex-start', textTransform: 'none', fontSize: 14, fontWeight: 600, lineHeight: 1.5 }}>
          <Box component="span" sx={{ overflow: 'hidden', textOverflow: 'ellipsis', display: '-webkit-box', WebkitLineClamp: 2, WebkitBoxOrient: 'vertical' }}>{name}</Box>
          <ChevronDown size={14} style={{ flexShrink: 0, transform: expanded ? 'rotate(180deg)' : undefined }} />
        </Button>
        <Box sx={{ display: 'flex', alignItems: 'center', gap: 0.75, mt: 0.5, color: 'text.secondary', fontSize: 12 }}>
          {chat ? <MessageSquare size={13} /> : flow ? <GitBranch size={13} /> : <Bot size={13} />} {detail}
        </Box>
        {showSubmitter && run.group_email && <Typography noWrap sx={{ fontSize: 11, color: 'text.secondary', mt: 0.5 }}>{run.group_email}</Typography>}
      </Box>
      <Box sx={{ justifySelf: { xs: 'end', sm: 'start' } }}>
        <ExecutionStatusBadge appearance="soft" showIcon={false} status={run.status} size="small" executionId={run.job_id} onApprovalComplete={onStatusChange} />
      </Box>
      <Box sx={{ display: 'flex', flexDirection: { xs: 'row', sm: 'column' }, flexWrap: 'wrap', gap: 0.5, color: 'text.secondary' }}>
        <Typography title={validDate ? created.toLocaleString() : undefined} sx={{ fontSize: 11 }}>
          {validDate ? `${created.toLocaleDateString(undefined, { month: 'short', day: 'numeric' })} · ${created.toLocaleTimeString(undefined, { hour: '2-digit', minute: '2-digit' })}` : 'Date unavailable'}
        </Typography>
        <RunDuration run={run} />
      </Box>
      {actions}
    </Box>
    {failure && <Typography sx={{ mt: 1, fontSize: 12, color: 'text.secondary', overflowWrap: 'anywhere', display: '-webkit-box', WebkitLineClamp: expanded ? 'unset' : 2, WebkitBoxOrient: 'vertical', overflow: 'hidden' }}>
      {failure}
    </Typography>}
    {expanded && <Box role="region" aria-label={`Details for ${name}`} sx={{ pt: 2, overflowWrap: 'anywhere' }}>
      <Box component="dl" sx={{ m: 0, display: 'grid', gridTemplateColumns: { xs: '1fr', sm: '1fr 1fr' }, gap: 2,
        '& dt': { fontSize: 11, color: 'text.secondary', mb: 0.25 }, '& dd': { m: 0, fontSize: 12 } }}>
        <Box><Typography component="dt">Framework</Typography><Typography component="dd">{run.harness === 'crewai' ? 'CrewAI' : run.harness === 'kasal' ? 'Kasal' : run.harness || 'Not recorded'}</Typography></Box>
        {run.inputs?.model && <Box><Typography component="dt">Model</Typography><Typography component="dd">{run.inputs.model}</Typography></Box>}
        <Box><Typography component="dt">Submitter</Typography><Typography component="dd">{run.group_email || 'Not recorded'}</Typography></Box>
        <Box><Typography component="dt">Execution ID</Typography><Typography component="dd" sx={{ fontFamily: 'monospace', userSelect: 'all' }}>{run.job_id}</Typography></Box>
      </Box>
      <Box sx={{ display: 'flex', flexWrap: 'wrap', gap: 1, mt: 1.5 }}><ExecutionMemoryButton jobId={run.job_id} /><RecipeCurationButton jobId={run.job_id} /></Box>
    </Box>}
  </Box>;
}
