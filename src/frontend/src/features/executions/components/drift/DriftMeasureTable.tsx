/**
 * Per-measure drift table: what Power BI has today vs what is deployed vs what the
 * proposal would deploy, with a chip that separates "changed in PBI" from "added by
 * the LLM" so a reviewer can tell at a glance what is theirs to check.
 */
import React from 'react';
import { Box, Chip, Table, TableBody, TableCell, TableHead, TableRow, Typography } from '@mui/material';
import { describeMeasure, type DriftMeasure } from './driftResult';

const mono = { fontFamily: 'monospace', fontSize: '0.7rem', whiteSpace: 'pre-wrap', wordBreak: 'break-word' } as const;

export const DriftMeasureTable: React.FC<{ measures: DriftMeasure[]; showView?: boolean }> = ({
  measures,
  showView = false,
}) => (
  <Table size="small" sx={{ tableLayout: 'fixed' }}>
    <TableHead>
      <TableRow>
        <TableCell sx={{ fontWeight: 600, width: '15%' }}>Measure</TableCell>
        <TableCell sx={{ fontWeight: 600, width: '13%' }}>Status</TableCell>
        <TableCell sx={{ fontWeight: 600, width: '22%' }}>Power BI DAX (today)</TableCell>
        <TableCell sx={{ fontWeight: 600, width: '20%' }}>Deployed SQL</TableCell>
        <TableCell sx={{ fontWeight: 600, width: '20%' }}>Proposed SQL</TableCell>
        <TableCell sx={{ fontWeight: 600, width: '10%' }}>Note</TableCell>
      </TableRow>
    </TableHead>
    <TableBody>
      {measures.map((m, i) => {
        const chip = describeMeasure(m);
        const proposedColor = m.applied ? 'success.main' : 'text.secondary';
        return (
          <TableRow key={`${m.original_name}-${m.view}-${i}`} hover>
            <TableCell sx={{ ...mono, fontSize: '0.75rem' }}>
              {m.original_name}
              {m.measure_name && m.measure_name !== m.original_name && (
                <Typography variant="caption" display="block" color="text.secondary">
                  {m.measure_name}
                </Typography>
              )}
              {showView && m.allocations && m.allocations.length > 0 && (
                <Typography variant="caption" display="block" color="text.secondary">
                  fact: {m.allocations.join(', ')}
                </Typography>
              )}
            </TableCell>
            <TableCell>
              <Chip size="small" color={chip.color} label={chip.label} title={chip.title} sx={{ fontSize: '0.65rem' }} />
              {m.translation && m.proposed_expr && (
                <Chip
                  size="small"
                  variant="outlined"
                  label={m.translation === 'llm' ? 'LLM SQL' : 'rule SQL'}
                  title={m.translation === 'llm' ? 'SQL written by the LLM fallback — review it' : 'SQL from the deterministic DAX translator'}
                  sx={{ fontSize: '0.6rem', mt: 0.5 }}
                />
              )}
            </TableCell>
            <TableCell sx={mono}>{m.current_dax || '—'}</TableCell>
            <TableCell sx={{ ...mono, color: m.status === 'removed_from_pbi' ? 'error.main' : undefined }}>
              {m.baseline_expr || '—'}
            </TableCell>
            <TableCell sx={{ ...mono, color: proposedColor }}>
              {m.proposed_expr || '—'}
              {m.proposed_expr && !m.applied && (
                <Typography variant="caption" display="block" color="text.secondary">
                  (not in proposal)
                </Typography>
              )}
            </TableCell>
            <TableCell sx={{ fontSize: '0.7rem', wordBreak: 'break-word' }}>
              <Box>{m.note}</Box>
              {m.llm_reason && (
                <Typography variant="caption" color="text.secondary" display="block">
                  LLM: {m.llm_reason}
                </Typography>
              )}
            </TableCell>
          </TableRow>
        );
      })}
    </TableBody>
  </Table>
);
