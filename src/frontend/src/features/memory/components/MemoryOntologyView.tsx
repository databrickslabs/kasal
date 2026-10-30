/**
 * The Memory Browser's ontology view: the entities graph memory knows and the
 * facts between them. Replaced facts stay available as history, marked.
 */
import React, { useEffect, useMemo, useState } from 'react';
import {
  Alert,
  Box,
  Button,
  Chip,
  CircularProgress,
  Divider,
  FormControlLabel,
  Stack,
  Switch,
  Typography,
} from '@mui/material';
import ArrowRightAltIcon from '@mui/icons-material/ArrowRightAlt';

import { MemoryBackendService } from '../../../api/memory/MemoryBackendService';
import { apiClient } from '../../../shared/api/client';
import type {
  MemoryGraphEntity,
  MemoryGraphFact,
  MemoryGraphResponse,
} from '../../../types/config/memoryBackend';
import { ConceptForceGraph } from './ConceptForceGraph';
import type { RunMemoryMode, RunTraceFacts } from './memoryData';
import {
  factsForRun,
  isCurrent,
  isNote,
  KIND_LABELS,
  ONTOLOGY_LABELS,
  ontologyGraph,
  qualifierText,
  visibleFacts,
} from './ontologyData';

const ringColor = (): string => '#829C8D';

interface PanelProps {
  entities: MemoryGraphEntity[];
  facts: MemoryGraphFact[];
  height?: number;
  /** Called after a fact was forgotten, to reload what is shown. */
  onChanged?: () => void;
}

/** The graph and the fact list for a given set of facts — the whole teamspace
 *  in the Memory Browser, one run's in the chat's run memory pane. */
export const OntologyPanel: React.FC<PanelProps> = ({
  entities,
  facts: allFacts,
  height,
  onChanged,
}) => {
  const [pinned, setPinned] = useState<Set<string>>(new Set());
  const [failed, setFailed] = useState<string | null>(null);
  const forget = async (factId: string) => {
    try {
      await MemoryBackendService.forgetGraphFact(factId);
      setFailed(null);
      onChanged?.();
    } catch {
      setFailed('Could not forget that fact.');
    }
  };
  const [showReplaced, setShowReplaced] = useState(false);

  const graph = useMemo(() => ontologyGraph(entities, allFacts), [entities, allFacts]);
  const types = useMemo(
    () => new Map(entities.map((entity) => [entity.id, entity.type])),
    [entities],
  );
  const facts = useMemo(
    () => visibleFacts(allFacts, pinned, showReplaced),
    [allFacts, pinned, showReplaced],
  );

  const togglePinned = (id: string) =>
    setPinned((previous) => {
      const next = new Set(previous);
      if (next.has(id)) next.delete(id);
      else next.add(id);
      return next;
    });
  const replacedCount = allFacts.filter((fact) => !isCurrent(fact)).length;

  return (
    <Box>
      <ConceptForceGraph
        nodes={graph.nodes}
        edges={graph.edges}
        activeIds={pinned}
        onToggleNode={togglePinned}
        importanceColor={ringColor}
        labels={ONTOLOGY_LABELS}
        height={height}
      />
      <Divider sx={{ my: 2 }} />
      <Stack direction="row" alignItems="center" justifyContent="space-between" sx={{ mb: 1 }}>
        <Typography variant="subtitle2">
          {pinned.size > 0 ? 'Facts about the pinned entities' : 'Facts and run notes'} ({facts.length})
        </Typography>
        <FormControlLabel
          control={
            <Switch
              size="small"
              checked={showReplaced}
              onChange={(event) => setShowReplaced(event.target.checked)}
            />
          }
          label={`Show replaced facts (${replacedCount})`}
        />
      </Stack>
      {failed && (
        <Alert severity="error" sx={{ mb: 1 }}>
          {failed}
        </Alert>
      )}
      <Stack spacing={1}>
        {facts.map((fact) => (
          <Box
            key={fact.id}
            sx={{
              p: 1.25,
              borderRadius: 1,
              border: 1,
              borderColor: 'divider',
              opacity: isCurrent(fact) ? 1 : 0.6,
            }}
          >
            {isNote(fact) ? (
              <Chip size="small" variant="outlined" color="info" label="run note" />
            ) : (
            <Stack direction="row" alignItems="center" spacing={0.75} flexWrap="wrap" useFlexGap>
              <Chip size="small" label={fact.subject} title={types.get(fact.subject_id)} />
              <Typography variant="caption" color="text.secondary">
                {fact.relation.replace(/_/g, ' ')}
              </Typography>
              <ArrowRightAltIcon fontSize="small" color="disabled" />
              <Chip
                size="small"
                variant={fact.object_id ? 'filled' : 'outlined'}
                label={fact.object}
                title={fact.object_id ? types.get(fact.object_id) : 'value'}
              />
              {fact.kind && KIND_LABELS[fact.kind] && (
                <Chip size="small" variant="outlined" color="info" label={KIND_LABELS[fact.kind]} />
              )}
              {!isCurrent(fact) && <Chip size="small" color="warning" label="replaced" />}
            </Stack>
            )}
            {isCurrent(fact) && onChanged && (
              <Button
                size="small"
                color="inherit"
                sx={{ float: 'right', mt: -0.5, textTransform: 'none', opacity: 0.7 }}
                onClick={() => void forget(fact.id)}
                aria-label={`Forget: ${fact.statement}`}
              >
                Forget
              </Button>
            )}
            {qualifierText(fact) && (
              <Typography variant="caption" color="text.secondary" sx={{ display: 'block', mt: 0.5 }}>
                {qualifierText(fact)}
              </Typography>
            )}
            <Typography variant="body2" sx={{ mt: 0.75 }}>
              {fact.statement}
            </Typography>
          </Box>
        ))}
      </Stack>
    </Box>
  );
};

/** One run's slice of the graph: the facts it saved or the facts it recalled,
 *  by the same evidence the record views use. */
export interface OntologyRunScope {
  id: string;
  mode: RunMemoryMode;
  trace: RunTraceFacts;
}

export const MemoryOntologyView: React.FC<{ run?: OntologyRunScope }> = ({ run }) => {
  const [data, setData] = useState<MemoryGraphResponse | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [reloadKey, setReloadKey] = useState(0);

  useEffect(() => {
    let cancelled = false;
    const load = async () => {
      try {
        const response = await apiClient.get<MemoryGraphResponse>('/memory-backend/graph', {
          params: { limit: 2000 },
        });
        if (!cancelled) setData(response.data);
      } catch {
        if (!cancelled) setError('Could not load the memory graph.');
      }
    };
    void load();
    return () => {
      cancelled = true;
    };
  }, [reloadKey]);

  if (error) return <Alert severity="error">{error}</Alert>;
  if (!data) {
    return (
      <Box sx={{ display: 'flex', justifyContent: 'center', py: 6 }}>
        <CircularProgress size={24} aria-label="Loading memory graph" />
      </Box>
    );
  }
  if (!data.enabled) {
    return (
      <Alert severity="info">
        Graph memory is off for this teamspace. Turn on “Store memory as a fact graph” in
        Configuration → Memory to have facts extracted and shown here.
      </Alert>
    );
  }
  const facts = run ? factsForRun(data.facts, run.mode, run.trace, run.id) : data.facts;
  return (
    <>
      {run && facts.length === 0 && (
        <Alert severity="info" sx={{ mb: 2 }}>
          {run.mode === 'recalled'
            ? 'This run recalled no facts from memory.'
            : 'This run saved no facts or notes.'}
        </Alert>
      )}
      <OntologyPanel
        entities={data.entities}
        facts={facts}
        onChanged={() => setReloadKey((key) => key + 1)}
      />
    </>
  );
};

export default MemoryOntologyView;
