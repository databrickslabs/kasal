import { nodeTypes } from './FlowCanvas';

// Regression test for "loading a flow from the workflow catalog loses the UI".
// FlowCanvas previously registered only `crewNode`, so agent/task/manager nodes
// loaded onto the flow canvas fell back to ReactFlow's bare default box. Every
// node type a saved flow can contain must be registered here, mapping to a real
// component (not undefined), or those nodes silently degrade to plain boxes.
describe('FlowCanvas nodeTypes registration', () => {
  it('registers every custom node type a loaded flow can contain', () => {
    expect(Object.keys(nodeTypes).sort()).toEqual([
      'agentNode',
      'crewNode',
      'managerNode',
      'taskNode',
    ]);
  });

  it('maps each node type to a defined component', () => {
    Object.values(nodeTypes).forEach((component) => {
      expect(component).toBeDefined();
    });
  });
});
