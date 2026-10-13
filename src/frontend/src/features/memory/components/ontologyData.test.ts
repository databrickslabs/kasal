import { describe, expect, it } from 'vitest';
import type { MemoryGraphFact } from '../../../types/config/memoryBackend';
import {
  factsForRun,
  nodeLabel,
  ONTOLOGY_LABELS,
  ontologyGraph,
  qualifierText,
  visibleFacts,
} from './ontologyData';

const entities = [
  { id: 'atlas', name: 'Atlas', type: 'project' },
  { id: 'dana', name: 'Dana', type: 'person' },
  { id: 'orphan', name: 'Orphan', type: 'concept' },
];
const fact = (over: Partial<MemoryGraphFact>): MemoryGraphFact => ({
  id: 'f',
  subject_id: 'atlas',
  subject: 'Atlas',
  relation: 'deadline_is',
  object: '30 July',
  statement: 'Atlas is due 30 July.',
  ...over,
});
const facts = [
  fact({ id: 'old', object: '15 June', valid_to: '2026-10-01T00:00:00', superseded_by: 'new' }),
  fact({ id: 'new' }),
  fact({ id: 'own', relation: 'owned_by', object: 'Dana', object_id: 'dana' }),
];

describe('ontologyGraph', () => {
  it('draws every current fact as a labelled edge, values included', () => {
    const { nodes, edges } = ontologyGraph(entities, facts);
    expect(nodes.map((n) => [n.label, n.count])).toEqual([
      ['Atlas', 2],
      ['Dana', 1],
      ['30 July', 1], // a plain value gets its own small node
    ]);
    expect(edges).toEqual([
      { source: 'atlas', target: 'value:new', weight: 1, label: 'deadline is' },
      { source: 'atlas', target: 'dana', weight: 1, label: 'owned by' },
    ]);
  });

  it('merges two relations between the same pair into one labelled edge', () => {
    const twice = [
      fact({ id: 'a', relation: 'owned_by', object: 'Dana', object_id: 'dana' }),
      fact({ id: 'b', relation: 'managed_by', object: 'Dana', object_id: 'dana' }),
    ];
    expect(ontologyGraph(entities, twice).edges).toEqual([
      { source: 'atlas', target: 'dana', weight: 2, label: 'owned by, managed by' },
    ]);
  });

  it('leaves out entities and values only a replaced fact mentions', () => {
    const { nodes, edges } = ontologyGraph(entities, [facts[0]]);
    expect(nodes).toEqual([]);
    expect(edges).toEqual([]);
  });
});

describe('visibleFacts', () => {
  it('hides replaced facts until asked, then lists them last', () => {
    expect(visibleFacts(facts, new Set(), false).map((f) => f.id)).toEqual(['new', 'own']);
    expect(visibleFacts(facts, new Set(), true).map((f) => f.id)).toEqual(['new', 'own', 'old']);
  });

  it('filters to facts touching a pinned entity, as subject or object', () => {
    expect(visibleFacts(facts, new Set(['dana']), false).map((f) => f.id)).toEqual(['own']);
  });

  it('filters to the one fact behind a pinned value', () => {
    expect(visibleFacts(facts, new Set(['value:new']), false).map((f) => f.id)).toEqual(['new']);
  });
});

it('describes nodes as facts, not records', () => {
  expect(ONTOLOGY_LABELS.tip({ id: 'a', label: 'A', count: 1, avgImportance: 0 })).toBe('1 fact');
  expect(ONTOLOGY_LABELS.tip({ id: 'a', label: 'A', count: 3, avgImportance: 0 })).toBe('3 facts');
});

describe('factsForRun', () => {
  const trace = { savedIds: new Set(['new']), recalledIds: new Set(['own']), savedContents: [] };
  const stamped = [...facts, fact({ id: 'mine', execution_id: 'run-1' })];

  it('saved: facts stamped with the run, or named by its write traces', () => {
    expect(factsForRun(stamped, 'saved', trace, 'run-1').map((f) => f.id)).toEqual(['new', 'mine']);
  });

  it('recalled: only facts its read traces name', () => {
    expect(factsForRun(stamped, 'recalled', trace, 'run-1').map((f) => f.id)).toEqual(['own']);
  });
});

it('draws a run note as a dated node on what it is about', () => {
  const note = fact({
    id: 'n',
    subject_id: 'atlas',
    subject: 'Atlas',
    relation: 'run_note',
    kind: 'episodic',
    object: 'Atlas status briefing: on track.',
    statement: '13 Oct 2026: Atlas status briefing: on track.',
  });
  const { nodes, edges } = ontologyGraph(entities, [note]);
  expect(nodes.map((n) => n.label)).toEqual(['Atlas', 'note 13 Oct 2026']);
  expect(edges).toEqual([{ source: 'atlas', target: 'value:n', weight: 1, label: 'run note' }]);
  expect(visibleFacts([note], new Set(['value:n']), false)).toEqual([note]);
});

describe('node labels and qualifiers', () => {
  it('cuts a long label so a long value is not the biggest node', () => {
    const long = 'Notre Dame University, Lebanon (Business Computing, Sep 2001 – Jan 2005)';
    expect(nodeLabel(long)).toBe('Notre Dame University, Le…');
    expect(nodeLabel('Cloudera')).toBe('Cloudera');
    const { nodes } = ontologyGraph(entities, [fact({ id: 'x', object: long })]);
    expect(nodes.map((n) => n.label.length <= 26)).toEqual([true, true]);
  });

  it('reads qualifiers as a short caption', () => {
    const job = fact({ qualifiers: { role: 'Engineer', from: 'Feb 2016', valid_to: '2023' } });
    expect(qualifierText(job)).toBe('role: Engineer · from: Feb 2016 · valid to: 2023');
    expect(qualifierText(fact({}))).toBe('');
  });
});
