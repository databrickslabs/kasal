/**
 * Pure shaping for the Memory Browser's ontology view: graph memory's entities
 * and facts, turned into what the force graph and the fact list render.
 */
import type { MemoryGraphEntity, MemoryGraphFact } from '../../../types/config/memoryBackend';
import type { ConceptGraphEdge, ConceptGraphNode, GraphLabels } from './ConceptForceGraph';
import type { RunMemoryMode, RunTraceFacts } from './memoryData';

export const isCurrent = (fact: MemoryGraphFact): boolean => !fact.valid_to;

/** A run note: one dated sentence on what a run was asked and produced. It is
 *  listed with the facts but is not an edge of the ontology. */
export const isNote = (fact: MemoryGraphFact): boolean =>
  fact.kind === 'episodic' || fact.relation === 'run_note';

/** How a fact's kind reads in the list. A fact the user stated needs no label. */
export const KIND_LABELS: Partial<Record<NonNullable<MemoryGraphFact['kind']>, string>> = {
  procedural: 'always applied',
  finding: 'found by a run',
};

export interface OntologyGraph {
  nodes: ConceptGraphNode[];
  edges: ConceptGraphEdge[];
}

/** Graph id of the value a fact points at when that value is not an entity
 *  (a figure, a date, a format). One per fact, so two facts never share one. */
export const valueNodeId = (fact: MemoryGraphFact): string => `value:${fact.id}`;

const relationLabel = (relation: string): string => relation.replace(/_/g, ' ');

// A node is drawn big enough to hold its label, so a long label makes a big
// node whatever it stands for. Labels are cut here; the list has the full text.
const MAX_NODE_LABEL = 26;
export const nodeLabel = (text: string): string =>
  text.length > MAX_NODE_LABEL ? `${text.slice(0, MAX_NODE_LABEL - 1).trimEnd()}…` : text;

/** "role: Engineer · from: 2016" — what qualifies a fact's relation. */
export const qualifierText = (fact: MemoryGraphFact): string =>
  Object.entries(fact.qualifiers ?? {})
    .map(([key, value]) => `${key.replace(/_/g, ' ')}: ${value}`)
    .join(' · ');

/**
 * The ontology as a graph: every CURRENT fact is an edge labelled with its
 * relation, from its subject to its object. An object that is an entity is that
 * entity's node; an object that is a plain value ("CHF 9.0 bn", "tables") gets
 * its own small node, so five facts about one company draw five edges, not a
 * lone circle. A run note is drawn the same way, attached to what it is about.
 * Entities are sized by how many facts and notes mention them.
 */
export function ontologyGraph(
  entities: MemoryGraphEntity[],
  facts: MemoryGraphFact[],
): OntologyGraph {
  const counts = new Map<string, number>();
  const pairs = new Map<string, ConceptGraphEdge>();
  const values: ConceptGraphNode[] = [];
  const bump = (id: string) => counts.set(id, (counts.get(id) ?? 0) + 1);
  for (const fact of facts.filter(isCurrent)) {
    bump(fact.subject_id);
    const label = relationLabel(fact.relation);
    if (!fact.object_id) {
      // A run note hangs off what it is about as a small dated node; its
      // sentence is in the list below, too long to draw.
      const text = isNote(fact) ? `note ${fact.statement.split(':')[0]}` : fact.object;
      values.push({ id: valueNodeId(fact), label: nodeLabel(text), count: 1, avgImportance: 0 });
      pairs.set(valueNodeId(fact), {
        source: fact.subject_id,
        target: valueNodeId(fact),
        weight: 1,
        label,
      });
      continue;
    }
    if (fact.object_id === fact.subject_id) continue;
    bump(fact.object_id);
    const key = [fact.subject_id, fact.object_id].sort().join('|');
    const edge = pairs.get(key);
    if (edge) {
      edge.weight += 1;
      edge.label = `${edge.label}, ${label}`;
    } else {
      pairs.set(key, { source: fact.subject_id, target: fact.object_id, weight: 1, label });
    }
  }
  const nodes = entities
    .filter((entity) => counts.has(entity.id))
    .map((entity) => ({
      id: entity.id,
      label: nodeLabel(entity.name),
      count: counts.get(entity.id) ?? 0,
      avgImportance: 0,
    }));
  return { nodes: [...nodes, ...values], edges: [...pairs.values()] };
}

/** Facts to list: those touching a pinned entity (all when none is pinned),
 *  current first, replaced ones only on request. */
export function visibleFacts(
  facts: MemoryGraphFact[],
  pinned: Set<string>,
  showReplaced: boolean,
): MemoryGraphFact[] {
  return facts
    .filter((fact) => showReplaced || isCurrent(fact))
    .filter(
      (fact) =>
        pinned.size === 0 ||
        pinned.has(fact.subject_id) ||
        pinned.has(valueNodeId(fact)) ||
        (fact.object_id ? pinned.has(fact.object_id) : false),
    )
    .sort((a, b) => Number(!isCurrent(a)) - Number(!isCurrent(b)));
}

export const ONTOLOGY_LABELS: GraphLabels = {
  empty: 'No facts yet — they appear as conversations and runs state something durable.',
  tip: (node) => `${node.count} ${node.count === 1 ? 'fact' : 'facts'}`,
  connected: 'connected entities',
  legend: ['size = facts', 'line = a fact, labelled with its relation', 'click = filter facts'],
};

/**
 * The facts one run SAVED or RECALLED. Saved: the fact carries the run's id, or
 * the run's write traces name it. Recalled: the run's read traces name it.
 * Same evidence the record views use, so the two never disagree about a run.
 */
export function factsForRun(
  facts: MemoryGraphFact[],
  mode: RunMemoryMode,
  trace: RunTraceFacts,
  runId?: string,
): MemoryGraphFact[] {
  if (mode === 'recalled') return facts.filter((fact) => trace.recalledIds.has(fact.id));
  return facts.filter(
    (fact) => (runId && fact.execution_id === runId) || trace.savedIds.has(fact.id),
  );
}
