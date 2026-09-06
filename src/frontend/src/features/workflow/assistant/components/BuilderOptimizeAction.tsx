import { useState } from 'react';
import { Menu, MenuItem, Typography } from '@mui/material';
import { WandSparkles } from 'lucide-react';
import type { Node } from 'reactflow';
import type { Run } from '../../../../types/execution/run';
import { FlowService } from '../../../../api/workflow/FlowService';
import { usePermissionStore } from '../../../../store/permissions';
import CrewOptimizeDialog from '../../crews/components/CrewOptimizeDialog';
import { runConfiguration } from '../utils/runConfiguration';

type CrewChoice = { id: string; name: string };
function flowCrews(nodes: Node[]): CrewChoice[] {
  if (!Array.isArray(nodes)) return [];
  return Array.from(new Map(nodes.filter(node => node.type === 'crewNode' && node.data?.crewId)
    .map(node => [String(node.data.crewId), { id: String(node.data.crewId), name: String(node.data.crewName || node.data.label || 'Crew') }])).values());
}

/** Optimize the saved definition used by this run, never an unrelated canvas. */
export default function BuilderOptimizeAction({ run }: { run: Run }) {
  const canEdit = usePermissionStore(state => state.allowAgentBuilder && state.userRole !== 'operator');
  const [selected, setSelected] = useState<CrewChoice | null>(null);
  const [choices, setChoices] = useState<CrewChoice[]>([]);
  const [anchor, setAnchor] = useState<HTMLElement | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState('');
  const config = runConfiguration(run);
  const flow = (run.execution_type || config.execution_type) === 'flow';
  const nodes = config.nodes || config.flow_config?.nodes || [];
  const savedFlowId = run.flow_id || config.flow_id;
  const available = flow ? Boolean(savedFlowId || flowCrews(nodes).length) : Boolean(run.crew_id);
  if (!canEdit) return null;
  const open = async (target: HTMLElement) => {
    setError('');
    if (!flow && run.crew_id) { setSelected({ id: run.crew_id, name: run.run_name }); return; }
    setLoading(true);
    try {
      let crews = flowCrews(nodes);
      if (!crews.length && savedFlowId) crews = flowCrews((await FlowService.getFlow(savedFlowId))?.nodes || []);
      if (crews.length === 1) setSelected(crews[0]);
      else if (crews.length > 1) { setChoices(crews); setAnchor(target); }
      else setError('This flow has no saved crews available to optimize.');
    } catch { setError('Could not load this flow’s crews. Please try again.'); }
    finally { setLoading(false); }
  };
  return <>
    <button type="button" disabled={!available || loading} onClick={event => void open(event.currentTarget)}
      title={available ? 'Improve the saved crew’s agent and task prompts' : 'Save the generated plan to the catalog before running it to enable optimization'}
      className="flex items-center gap-1.5 px-2.5 py-1.5 rounded-lg text-xs font-medium transition-all hover:opacity-80 disabled:opacity-50 disabled:cursor-not-allowed"
      style={{ background: 'transparent', border: 0, color: 'var(--text-secondary)' }}>
      <WandSparkles size={14} />{loading ? 'Loading crews…' : 'Optimize crew'}
    </button>
    {error && <Typography role="alert" variant="caption" color="text.secondary">{error}</Typography>}
    <Menu anchorEl={anchor} open={Boolean(anchor)} onClose={() => setAnchor(null)} PaperProps={{ sx: { borderRadius: 3 } }}>
      {choices.map(crew => <MenuItem key={crew.id} onClick={() => { setSelected(crew); setAnchor(null); }}>{crew.name}</MenuItem>)}
    </Menu>
    {selected && <CrewOptimizeDialog open crewId={selected.id} crewName={selected.name} onClose={() => setSelected(null)} />}
  </>;
}
