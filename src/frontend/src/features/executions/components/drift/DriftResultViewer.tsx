/**
 * DriftResultViewer — the UCMV Drift Monitor report.
 *
 * Deployed metric views are the verified baseline. This view answers, per view:
 * what changed in Power BI, what the proposal adds or updates (and whether the LLM
 * wrote that SQL), what was flagged but deliberately left alone, and the exact
 * proposed YAML with every line that is not in the deployed definition highlighted.
 *
 * Read-only: deploying goes through the "4 · Deploy Metric Views" crew.
 */
import React, { useMemo, useState } from 'react';
import {
  Accordion,
  AccordionDetails,
  AccordionSummary,
  Alert,
  Box,
  Button,
  Chip,
  FormControlLabel,
  Paper,
  Switch,
  Tab,
  Tabs,
  Typography,
} from '@mui/material';
import ExpandMoreIcon from '@mui/icons-material/ExpandMore';
import CompareArrowsIcon from '@mui/icons-material/CompareArrows';
import DownloadIcon from '@mui/icons-material/Download';
import ContentCopyIcon from '@mui/icons-material/ContentCopy';
import { DriftMeasureTable } from './DriftMeasureTable';
import {
  diffYamlLines,
  downloadText,
  needsAttention,
  sortMeasures,
  type DriftResult,
  type DriftView,
} from './driftResult';

const Tile: React.FC<{ value?: number; label: string; color?: string; title?: string }> = ({ value = 0, label, color, title }) => (
  <Paper variant="outlined" sx={{ p: 1.5, flex: 1, minWidth: 120, textAlign: 'center' }} title={title}>
    <Typography variant="h5" sx={{ fontWeight: 700, color: value > 0 ? color : 'text.secondary' }}>
      {value}
    </Typography>
    <Typography variant="caption" color="text.secondary">{label}</Typography>
  </Paper>
);

const YamlDiff: React.FC<{ baseline: string; proposed: string }> = ({ baseline, proposed }) => {
  const lines = useMemo(() => diffYamlLines(baseline, proposed), [baseline, proposed]);
  return (
    <Box
      component="pre"
      sx={{ m: 0, p: 1, fontSize: '0.72rem', fontFamily: 'monospace', maxHeight: 520, overflow: 'auto', bgcolor: 'action.hover', borderRadius: 1 }}
    >
      {lines.map((l, i) => (
        <Box
          key={i}
          component="div"
          sx={l.added ? { bgcolor: 'rgba(46, 125, 50, 0.16)', borderLeft: '3px solid', borderColor: 'success.main', pl: 0.5 } : { pl: '7px' }}
        >
          {l.text || ' '}
        </Box>
      ))}
    </Box>
  );
};

const ViewSection: React.FC<{ view: DriftView; ddl?: string }> = ({ view, ddl }) => {
  const [tab, setTab] = useState(0);
  const [showAll, setShowAll] = useState(false);
  const measures = useMemo(() => sortMeasures(view.measures || []), [view.measures]);
  const visible = showAll ? measures : measures.filter((m) => m.applied || needsAttention(m) || m.note);
  const c = view.counts;
  const hasProposal = (view.changes || []).length > 0;
  const flagged = measures.filter((m) => needsAttention(m) && !m.applied).length;

  return (
    <Accordion variant="outlined" disableGutters defaultExpanded={hasProposal || !!view.error} sx={{ '&:before': { display: 'none' } }}>
      <AccordionSummary expandIcon={<ExpandMoreIcon />}>
        <Box display="flex" alignItems="center" gap={1} flexWrap="wrap">
          <Typography variant="subtitle2" sx={{ fontFamily: 'monospace' }}>{view.full_name}</Typography>
          {view.error && <Chip size="small" color="error" label="unreadable" />}
          {c && c.measures_added > 0 && <Chip size="small" color="success" label={`+${c.measures_added} added`} />}
          {c && c.measures_updated > 0 && <Chip size="small" color="warning" label={`${c.measures_updated} updated`} />}
          {c && c.joins_added > 0 && <Chip size="small" color="info" label={`+${c.joins_added} join`} />}
          {flagged > 0 && <Chip size="small" variant="outlined" color="warning" label={`${flagged} flagged`} />}
          {!view.error && !hasProposal && flagged === 0 && <Chip size="small" variant="outlined" label="in sync" />}
        </Box>
      </AccordionSummary>
      <AccordionDetails sx={{ pt: 0 }}>
        {view.error && <Alert severity="error" sx={{ mb: 1 }}>Could not read the deployed definition: {view.error}</Alert>}
        {(view.invariant_problems || []).length > 0 && (
          <Alert severity="error" sx={{ mb: 1 }}>
            The patch was rejected because it would have changed the verified view:
            <ul style={{ margin: 0 }}>{view.invariant_problems!.map((p) => <li key={p}>{p}</li>)}</ul>
          </Alert>
        )}
        {!view.error && (
          <>
            <Tabs value={tab} onChange={(_e, v) => setTab(v)} sx={{ minHeight: 36, '& .MuiTab-root': { minHeight: 36 } }}>
              <Tab label={`Measures (${measures.length})`} />
              <Tab label="Proposed YAML" disabled={!hasProposal} />
              <Tab label="Deployed YAML" />
            </Tabs>
            {tab === 0 && (
              <Box>
                <FormControlLabel
                  sx={{ my: 0.5 }}
                  control={<Switch size="small" checked={showAll} onChange={(_e, v) => setShowAll(v)} />}
                  label={<Typography variant="caption">Show unchanged measures</Typography>}
                />
                {visible.length > 0 ? (
                  <DriftMeasureTable measures={visible} />
                ) : (
                  <Typography variant="body2" color="text.secondary" sx={{ p: 1 }}>No drift in this view.</Typography>
                )}
              </Box>
            )}
            {tab === 1 && hasProposal && (
              <Box display="flex" flexDirection="column" gap={1} sx={{ mt: 1 }}>
                <Typography variant="caption" color="text.secondary">
                  Highlighted lines are new or re-translated; every other line is the deployed definition, unchanged.
                </Typography>
                <YamlDiff baseline={view.baseline_yaml || ''} proposed={view.proposed_yaml || ''} />
                <Box display="flex" gap={1}>
                  <Button size="small" startIcon={<DownloadIcon />} onClick={() => downloadText(`${view.name}.yaml`, view.proposed_yaml || '')}>
                    Proposed YAML
                  </Button>
                  {ddl && (
                    <Button size="small" startIcon={<ContentCopyIcon />} onClick={() => navigator.clipboard?.writeText(ddl)}>
                      Copy deploy SQL
                    </Button>
                  )}
                </Box>
              </Box>
            )}
            {tab === 2 && (
              <Box component="pre" sx={{ m: 0, mt: 1, p: 1, fontSize: '0.72rem', maxHeight: 520, overflow: 'auto', bgcolor: 'action.hover', borderRadius: 1 }}>
                {view.baseline_yaml}
              </Box>
            )}
          </>
        )}
      </AccordionDetails>
    </Accordion>
  );
};

const DriftResultViewer: React.FC<{ result: DriftResult }> = ({ result }) => {
  const s = result.summary || {};
  const unassigned = result.unassigned || [];
  const proposals = Object.keys(result.yaml || {}).length;

  return (
    <Box display="flex" flexDirection="column" gap={2}>
      <Box display="flex" alignItems="center" gap={1} flexWrap="wrap">
        <CompareArrowsIcon color="primary" />
        <Typography variant="h6">UCMV Drift Monitor</Typography>
        {result.pbi?.dataset_id && <Chip size="small" variant="outlined" label={`dataset ${result.pbi.dataset_id}`} />}
        {s.pbi_measures !== undefined && <Chip size="small" variant="outlined" label={`${s.pbi_measures} PBI measures`} />}
      </Box>

      {result.error && <Alert severity="error">{result.error}</Alert>}
      {(result.warnings || []).map((w) => <Alert key={w} severity="warning">{w}</Alert>)}

      <Box display="flex" gap={1.5} flexWrap="wrap">
        <Tile value={s.changed_in_pbi} label="Changed in PBI" color="warning.main" title="DAX changed (fingerprint mismatch)" />
        <Tile value={s.possibly_changed} label="Suspected change" color="secondary.main" title="LLM judged the DAX may have changed (older views)" />
        <Tile value={s.new_in_pbi} label="New in PBI" color="info.main" />
        <Tile value={s.measures_added} label="Added to proposal" color="success.main" title="New measures translated and appended" />
        <Tile value={s.measures_updated} label="Updated in proposal" color="warning.main" />
        <Tile value={s.removed_from_pbi} label="Removed in PBI" color="error.main" title="Flagged only — kept in the view" />
        <Tile value={s.unassigned} label="Unassigned" title="New measures whose fact table is not monitored" />
        <Tile value={s.unchanged} label="Unchanged" />
      </Box>

      {!result.error && (
        proposals > 0 ? (
          <Alert severity="info">
            {proposals} view{proposals === 1 ? '' : 's'} with a proposed update. Nothing was deployed. Review the
            highlighted lines, then deploy with the <strong>4 · Deploy Metric Views</strong> crew — its post-deploy
            check confirms every measure landed.
          </Alert>
        ) : (
          <Alert severity="success">No changes to propose — the monitored views are in sync with Power BI.</Alert>
        )
      )}
      {(s.legacy_unverified ?? 0) > 0 && (
        <Alert severity="info" variant="outlined">
          {s.legacy_unverified} measure{s.legacy_unverified === 1 ? '' : 's'} were deployed before DAX
          fingerprinting and were not compared. Turn on the LLM check, or regenerate the view once so changes
          are detected exactly from then on.
        </Alert>
      )}

      {(result.views || []).map((v) => (
        <ViewSection key={v.full_name} view={v} ddl={result.deploy_ddl?.[v.full_name]} />
      ))}

      {unassigned.length > 0 && (
        <Paper variant="outlined">
          <Typography variant="subtitle2" sx={{ p: 1.5, pb: 0 }}>
            Unassigned new measures ({unassigned.length})
          </Typography>
          <Typography variant="caption" color="text.secondary" sx={{ px: 1.5, display: 'block' }}>
            New in Power BI, but their fact table is not one of the monitored views. Add that view to the
            monitor list, or generate it with the UC Metric View Generator.
          </Typography>
          <DriftMeasureTable measures={unassigned} showView />
        </Paper>
      )}
    </Box>
  );
};

export default DriftResultViewer;
