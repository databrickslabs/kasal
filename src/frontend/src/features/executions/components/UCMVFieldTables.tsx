import React from 'react';
import { Box, Chip, Table, TableHead, TableBody, TableRow, TableCell } from '@mui/material';
import { extractReferencedBy, extractUsedOn, extractIndirectUsedOn } from './ucmvMeasureUsage';

export const FieldTable: React.FC<{
  fields: Array<{ name: string; expr?: string; comment?: string; format?: string }>;
  showFormat?: boolean;
}> = ({ fields, showFormat }) => {
  // Only show the "Used by" column for measures (showFormat) and only when at
  // least one measure carries a usage count — keeps dimension tables unchanged.
  const showUsage = !!showFormat && fields.some((f) => extractReferencedBy(f.comment) !== null);
  // Same, for the visual-usage "Used on" column (report pages the measure appears
  // on) — only for measures, only when at least one carries DIRECT or INDIRECT
  // (backtraced) usage, so a sub-KPI that's only referenced by a visual KPI
  // still shows up.
  const showUsedOn =
    !!showFormat &&
    fields.some(
      (f) =>
        extractUsedOn(f.comment).entries.length > 0 ||
        extractIndirectUsedOn(f.comment).length > 0,
    );
  // Sort measures so the highest-impact surface first: measures used on more
  // visuals rank above those used on fewer, then by how many other measures
  // reference them.
  const rows =
    showUsage || showUsedOn
      ? [...fields].sort((a, b) => {
          const va = extractUsedOn(a.comment).count;
          const vb = extractUsedOn(b.comment).count;
          if (va !== vb) return vb - va;
          return (extractReferencedBy(b.comment) ?? -1) - (extractReferencedBy(a.comment) ?? -1);
        })
      : fields;
  // Column widths depend on which optional columns are present (all measures
  // carry a Comment column via showFormat; dimensions never show usage/usedOn).
  const nameW = showUsedOn && showUsage ? '20%' : showUsage || showUsedOn ? '24%' : showFormat ? '30%' : '30%';
  const exprW = !showFormat ? '70%' : showUsedOn && showUsage ? '26%' : showUsage || showUsedOn ? '32%' : '40%';
  const commentW = showUsedOn && showUsage ? '22%' : showUsedOn ? '26%' : '30%';
  return (
    <Table size="small" sx={{ tableLayout: 'fixed' }}>
      <TableHead>
        <TableRow>
          <TableCell sx={{ fontWeight: 600, width: nameW }}>Name</TableCell>
          <TableCell sx={{ fontWeight: 600, width: exprW }}>Expression</TableCell>
          {showFormat && <TableCell sx={{ fontWeight: 600, width: commentW }}>Comment / Format</TableCell>}
          {showUsedOn && <TableCell sx={{ fontWeight: 600, width: '20%' }} title="Report page(s) this measure is drawn on or filtered by">Used on</TableCell>}
          {showUsage && <TableCell sx={{ fontWeight: 600, width: '12%' }} align="right" title="How many other measures reference this measure">Used by</TableCell>}
        </TableRow>
      </TableHead>
      <TableBody>
        {rows.map((f) => {
          const usage = extractReferencedBy(f.comment);
          const usedOn = extractUsedOn(f.comment);
          const indirect = extractIndirectUsedOn(f.comment);
          return (
            <TableRow key={f.name} hover>
              <TableCell sx={{ fontFamily: 'monospace', fontSize: '0.8rem', wordBreak: 'break-word' }}>
                {f.name}
              </TableCell>
              <TableCell sx={{ fontFamily: 'monospace', fontSize: '0.8rem', wordBreak: 'break-word' }}>
                {f.expr || '—'}
              </TableCell>
              {showFormat && (
                <TableCell sx={{ fontSize: '0.8rem', wordBreak: 'break-word' }}>
                  {f.comment || f.format || '—'}
                </TableCell>
              )}
              {showUsedOn && (
                <TableCell sx={{ fontSize: '0.8rem' }}>
                  {usedOn.entries.length > 0 || indirect.length > 0 ? (
                    <Box display="flex" flexDirection="column" gap={0.5} alignItems="flex-start">
                      {usedOn.entries.length > 0 && (
                        <>
                          {/* HOW OFTEN — visual-occurrence count. */}
                          <Chip
                            size="small"
                            color="info"
                            variant="filled"
                            label={`${usedOn.count} visual${usedOn.count !== 1 ? 's' : ''}`}
                            sx={{ height: 18, fontSize: '0.7rem' }}
                          />
                          {/* WHERE + HOW — page, visual type(s), and drawn vs filter. */}
                          <Box display="flex" gap={0.5} flexWrap="wrap">
                            {usedOn.entries.map((e) => {
                              // "×N" on the page chip so a count of 7 all on one page
                              // reads as "OTC Scorecard ×7" instead of looking like a
                              // mismatch with the total.
                              const tally = e.count > 1 ? ` ×${e.count}` : '';
                              const typeLabel = e.types.length ? ` · ${e.types.join('/')}` : '';
                              const roleWord =
                                e.role === 'filter' ? 'used as a filter' : e.role === 'drawn' ? 'drawn (shown)' : '';
                              return (
                                <Chip
                                  key={e.page}
                                  size="small"
                                  variant="outlined"
                                  color={e.role === 'filter' ? 'default' : 'info'}
                                  label={`${e.page}${tally}${typeLabel}`}
                                  title={[
                                    e.count > 1 ? `${e.count} visuals on this page` : '',
                                    roleWord,
                                    e.types.length ? `in ${e.types.join(', ')}` : '',
                                  ]
                                    .filter(Boolean)
                                    .join(' · ')}
                                  sx={{ height: 18, fontSize: '0.7rem', maxWidth: '100%' }}
                                />
                              );
                            })}
                          </Box>
                        </>
                      )}
                      {/* INDIRECT — backtraced: a visual-placed KPI depends on this
                          measure. Distinct style (dashed, muted, "↳ via …") so it
                          never reads as direct usage. */}
                      {indirect.map((ind) => (
                        <Chip
                          key={`via-${ind.via}`}
                          size="small"
                          variant="outlined"
                          label={`↳ via ${ind.via}`}
                          title={
                            `Indirectly used — [${ind.via}] references this measure` +
                            (ind.pages.length ? ` · shown on ${ind.pages.join(', ')}` : '')
                          }
                          sx={{
                            height: 18,
                            fontSize: '0.7rem',
                            maxWidth: '100%',
                            borderStyle: 'dashed',
                            color: 'text.secondary',
                          }}
                        />
                      ))}
                    </Box>
                  ) : (
                    '—'
                  )}
                </TableCell>
              )}
              {showUsage && (
                <TableCell sx={{ fontSize: '0.8rem' }} align="right">
                  {usage != null && usage > 0 ? usage : '—'}
                </TableCell>
              )}
            </TableRow>
          );
        })}
      </TableBody>
    </Table>
  );
};

/* ------------------------------------------------------------------ */
/*  Joins table                                                        */
/* ------------------------------------------------------------------ */

export const JoinsTable: React.FC<{
  joins: Array<{ name: string; source?: string; on?: string; using?: string[] }>;
}> = ({ joins }) => (
  <Table size="small">
    <TableHead>
      <TableRow>
        <TableCell sx={{ fontWeight: 600 }}>Name</TableCell>
        <TableCell sx={{ fontWeight: 600 }}>Source</TableCell>
        <TableCell sx={{ fontWeight: 600 }}>On / Using</TableCell>
      </TableRow>
    </TableHead>
    <TableBody>
      {joins.map((j) => (
        <TableRow key={j.name} hover>
          <TableCell sx={{ fontFamily: 'monospace', fontSize: '0.8rem' }}>{j.name}</TableCell>
          <TableCell sx={{ fontFamily: 'monospace', fontSize: '0.8rem' }}>{j.source || '—'}</TableCell>
          <TableCell sx={{ fontFamily: 'monospace', fontSize: '0.8rem' }}>
            {j.on || (j.using ? j.using.join(', ') : '—')}
          </TableCell>
        </TableRow>
      ))}
    </TableBody>
  </Table>
);
