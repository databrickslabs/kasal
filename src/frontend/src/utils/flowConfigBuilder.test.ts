import { describe, it, expect } from 'vitest';
import { buildFlowConfiguration } from './flowConfigBuilder';
import { Node, Edge } from 'reactflow';

describe('flowConfigBuilder', () => {
  describe('buildFlowConfiguration', () => {
    it('returns configuration with empty collections for empty nodes and edges', () => {
      const result = buildFlowConfiguration([], [], 'Test Flow');

      expect(result.name).toBe('Test Flow');
      expect(result.type).toBe('default');
      expect(result.listeners).toEqual([]);
      expect(result.startingPoints).toEqual([]);
      expect(result.actions).toEqual([]);
      expect(result.routers).toBeUndefined();
      expect(result.id).toMatch(/^flow-\d+$/);
    });

    it('creates starting points from nodes without incoming edges', () => {
      const nodes: Node[] = [
        {
          id: 'node-1',
          type: 'crewNode',
          position: { x: 0, y: 0 },
          data: {
            label: 'Test Crew',
            crewId: 'crew-1',
            crewName: 'Test Crew',
            allTasks: [
              { id: 'task-1', name: 'Task 1', description: 'First task' },
              { id: 'task-2', name: 'Task 2', description: 'Second task' },
            ],
          },
        },
      ];

      // No edges means node-1 is a starting point
      const edges: Edge[] = [];

      const result = buildFlowConfiguration(nodes, edges, 'Test Flow');

      // All tasks become starting points when no edges are present
      expect(result.startingPoints.length).toBe(2);
      expect(result.startingPoints[0].crewId).toBe('crew-1');
      expect(result.startingPoints[0].taskId).toBe('task-1');
      expect(result.startingPoints[0].taskName).toBe('Task 1');
      expect(result.startingPoints[1].taskId).toBe('task-2');
    });

    it('creates starting points only for selected tasks when edges exist', () => {
      const nodes: Node[] = [
        {
          id: 'node-1',
          type: 'crewNode',
          position: { x: 0, y: 0 },
          data: {
            label: 'Test Crew',
            crewId: 'crew-1',
            crewName: 'Test Crew',
            allTasks: [
              { id: 'task-1', name: 'Task 1' },
              { id: 'task-2', name: 'Task 2' },
            ],
          },
        },
        {
          id: 'node-2',
          type: 'crewNode',
          position: { x: 200, y: 0 },
          data: {
            label: 'Crew 2',
            crewId: 'crew-2',
            crewName: 'Crew 2',
            allTasks: [{ id: 'task-3', name: 'Task 3' }],
          },
        },
      ];

      const edges: Edge[] = [
        {
          id: 'edge-1',
          source: 'node-1',
          target: 'node-2',
          data: {
            listenToTaskIds: ['task-1'], // Only task-1 triggers the next node
            targetTaskIds: ['task-3'],
          },
        },
      ];

      const result = buildFlowConfiguration(nodes, edges, 'Test Flow');

      // Only task-1 should be a starting point (selected in outgoing edge)
      expect(result.startingPoints.length).toBe(1);
      expect(result.startingPoints[0].taskId).toBe('task-1');
    });

    it('creates listeners for configured incoming edges', () => {
      const nodes: Node[] = [
        {
          id: 'node-1',
          type: 'crewNode',
          position: { x: 0, y: 0 },
          data: {
            label: 'Crew 1',
            crewId: 'crew-1',
            crewName: 'Crew 1',
            allTasks: [{ id: 'task-1', name: 'Task 1' }],
          },
        },
        {
          id: 'node-2',
          type: 'crewNode',
          position: { x: 200, y: 0 },
          data: {
            label: 'Crew 2',
            crewId: 'crew-2',
            crewName: 'Crew 2',
            allTasks: [{ id: 'task-2', name: 'Task 2' }],
          },
        },
      ];

      const edges: Edge[] = [
        {
          id: 'edge-1',
          source: 'node-1',
          target: 'node-2',
          data: {
            listenToTaskIds: ['task-1'],
            targetTaskIds: ['task-2'],
          },
        },
      ];

      const result = buildFlowConfiguration(nodes, edges, 'Test Flow');

      expect(result.listeners.length).toBe(1);
      expect(result.listeners[0].crewId).toBe('crew-2');
      expect(result.listeners[0].crewName).toBe('Crew 2');
      expect(result.listeners[0].listenToTaskIds).toEqual(['task-1']);
    });

    it('creates actions from edges with target task IDs', () => {
      const nodes: Node[] = [
        {
          id: 'node-1',
          type: 'crewNode',
          position: { x: 0, y: 0 },
          data: {
            label: 'Crew 1',
            crewId: 'crew-1',
            allTasks: [{ id: 'task-1', name: 'Task 1' }],
          },
        },
        {
          id: 'node-2',
          type: 'crewNode',
          position: { x: 200, y: 0 },
          data: {
            label: 'Crew 2',
            crewId: 'crew-2',
            crewName: 'Crew 2',
            allTasks: [{ id: 'task-2', name: 'Task 2' }],
          },
        },
      ];

      const edges: Edge[] = [
        {
          id: 'edge-1',
          source: 'node-1',
          target: 'node-2',
          data: {
            listenToTaskIds: ['task-1'],
            targetTaskIds: ['task-2'],
          },
        },
      ];

      const result = buildFlowConfiguration(nodes, edges, 'Test Flow');

      expect(result.actions.length).toBe(1);
      expect(result.actions[0].crewId).toBe('crew-2');
      expect(result.actions[0].taskId).toBe('task-2');
      expect(result.actions[0].taskName).toBe('Task 2');
    });

    it('handles router nodes with ROUTER logicType', () => {
      const nodes: Node[] = [
        {
          id: 'node-1',
          type: 'crewNode',
          position: { x: 0, y: 0 },
          data: {
            label: 'Crew 1',
            crewId: 'crew-1',
            allTasks: [{ id: 'task-1', name: 'Task 1' }],
          },
        },
        {
          id: 'node-2',
          type: 'crewNode',
          position: { x: 400, y: 0 },
          data: {
            label: 'Crew 2',
            crewId: 'crew-2',
            crewName: 'Crew 2',
            allTasks: [{ id: 'task-2', name: 'Task 2' }],
          },
        },
      ];

      const edges: Edge[] = [
        {
          id: 'edge-1',
          source: 'node-1',
          target: 'node-2',
          data: {
            logicType: 'ROUTER',
            listenToTaskIds: ['task-1'],
            targetTaskIds: ['task-2'],
            routerCondition: 'state.status == "approved"',
          },
        },
      ];

      const result = buildFlowConfiguration(nodes, edges, 'Test Flow');

      // ROUTER edges should create routers, not listeners
      expect(result.routers).toBeDefined();
      expect(result.routers?.length).toBe(1);
      expect(result.routers?.[0].name).toContain('router_node_1');
    });

    it('uses flow name provided', () => {
      const result = buildFlowConfiguration([], [], 'My Custom Flow');
      expect(result.name).toBe('My Custom Flow');
    });

    it('generates unique flow id with timestamp', () => {
      const result1 = buildFlowConfiguration([], [], 'Flow 1');
      const result2 = buildFlowConfiguration([], [], 'Flow 2');

      // Both should have valid id format
      expect(result1.id).toMatch(/^flow-\d+$/);
      expect(result2.id).toMatch(/^flow-\d+$/);
    });
  });
});
