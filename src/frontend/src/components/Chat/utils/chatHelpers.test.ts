/**
 * Tests for Chat helper utilities.
 *
 * Covers:
 * - hasCrewContent: Checks if nodes contain both agents and tasks
 * - isCrewGeneration: Detects multi-agent/multi-task crew generation
 * - handleNodesGenerated: Node/edge generation handling with deduplication
 * - isExecuteCommand: Execute command detection
 * - extractJobIdFromCommand: Job ID extraction from execute commands
 */

import { describe, it, expect, vi } from 'vitest';
import { Node, Edge } from 'reactflow';
import {
  hasCrewContent,
  isCrewGeneration,
  handleNodesGenerated,
  isExecuteCommand,
  extractJobIdFromCommand,
  SLASH_COMMANDS,
  filterSlashCommands,
} from './chatHelpers';

describe('chatHelpers', () => {
  describe('hasCrewContent', () => {
    it('returns true when nodes contain both agents and tasks', () => {
      const nodes: Node[] = [
        { id: 'agent-1', type: 'agentNode', position: { x: 0, y: 0 }, data: {} },
        { id: 'task-1', type: 'taskNode', position: { x: 100, y: 0 }, data: {} },
      ];
      expect(hasCrewContent(nodes)).toBe(true);
    });

    it('returns false when only agents are present', () => {
      const nodes: Node[] = [
        { id: 'agent-1', type: 'agentNode', position: { x: 0, y: 0 }, data: {} },
        { id: 'agent-2', type: 'agentNode', position: { x: 100, y: 0 }, data: {} },
      ];
      expect(hasCrewContent(nodes)).toBe(false);
    });

    it('returns false when only tasks are present', () => {
      const nodes: Node[] = [
        { id: 'task-1', type: 'taskNode', position: { x: 0, y: 0 }, data: {} },
        { id: 'task-2', type: 'taskNode', position: { x: 100, y: 0 }, data: {} },
      ];
      expect(hasCrewContent(nodes)).toBe(false);
    });

    it('returns false for empty nodes array', () => {
      expect(hasCrewContent([])).toBe(false);
    });

    it('returns false when nodes have different types (not agent/task)', () => {
      const nodes: Node[] = [
        { id: 'custom-1', type: 'customNode', position: { x: 0, y: 0 }, data: {} },
        { id: 'input-1', type: 'inputNode', position: { x: 100, y: 0 }, data: {} },
      ];
      expect(hasCrewContent(nodes)).toBe(false);
    });

    it('handles mixed node types correctly', () => {
      const nodes: Node[] = [
        { id: 'agent-1', type: 'agentNode', position: { x: 0, y: 0 }, data: {} },
        { id: 'task-1', type: 'taskNode', position: { x: 100, y: 0 }, data: {} },
        { id: 'custom-1', type: 'customNode', position: { x: 200, y: 0 }, data: {} },
      ];
      expect(hasCrewContent(nodes)).toBe(true);
    });
  });

  describe('isCrewGeneration', () => {
    it('returns true when multiple agents AND multiple tasks exist', () => {
      const nodes: Node[] = [
        { id: 'agent-1', type: 'agentNode', position: { x: 0, y: 0 }, data: {} },
        { id: 'agent-2', type: 'agentNode', position: { x: 0, y: 100 }, data: {} },
        { id: 'task-1', type: 'taskNode', position: { x: 100, y: 0 }, data: {} },
        { id: 'task-2', type: 'taskNode', position: { x: 100, y: 100 }, data: {} },
      ];
      expect(isCrewGeneration(nodes)).toBe(true);
    });

    it('returns false when only one agent exists', () => {
      const nodes: Node[] = [
        { id: 'agent-1', type: 'agentNode', position: { x: 0, y: 0 }, data: {} },
        { id: 'task-1', type: 'taskNode', position: { x: 100, y: 0 }, data: {} },
        { id: 'task-2', type: 'taskNode', position: { x: 100, y: 100 }, data: {} },
      ];
      expect(isCrewGeneration(nodes)).toBe(false);
    });

    it('returns false when only one task exists', () => {
      const nodes: Node[] = [
        { id: 'agent-1', type: 'agentNode', position: { x: 0, y: 0 }, data: {} },
        { id: 'agent-2', type: 'agentNode', position: { x: 0, y: 100 }, data: {} },
        { id: 'task-1', type: 'taskNode', position: { x: 100, y: 0 }, data: {} },
      ];
      expect(isCrewGeneration(nodes)).toBe(false);
    });

    it('returns false for empty nodes array', () => {
      expect(isCrewGeneration([])).toBe(false);
    });

    it('returns false when only multiple agents (no tasks)', () => {
      const nodes: Node[] = [
        { id: 'agent-1', type: 'agentNode', position: { x: 0, y: 0 }, data: {} },
        { id: 'agent-2', type: 'agentNode', position: { x: 0, y: 100 }, data: {} },
      ];
      expect(isCrewGeneration(nodes)).toBe(false);
    });

    it('returns false when only multiple tasks (no agents)', () => {
      const nodes: Node[] = [
        { id: 'task-1', type: 'taskNode', position: { x: 100, y: 0 }, data: {} },
        { id: 'task-2', type: 'taskNode', position: { x: 100, y: 100 }, data: {} },
      ];
      expect(isCrewGeneration(nodes)).toBe(false);
    });
  });

  describe('handleNodesGenerated', () => {
    it('replaces all nodes/edges for crew generation', () => {
      const existingNodes: Node[] = [
        { id: 'old-agent', type: 'agentNode', position: { x: 0, y: 0 }, data: {} },
      ];
      const existingEdges: Edge[] = [
        { id: 'old-edge', source: 'a', target: 'b' },
      ];

      const newNodes: Node[] = [
        { id: 'agent-1', type: 'agentNode', position: { x: 0, y: 0 }, data: {} },
        { id: 'agent-2', type: 'agentNode', position: { x: 0, y: 100 }, data: {} },
        { id: 'task-1', type: 'taskNode', position: { x: 100, y: 0 }, data: {} },
        { id: 'task-2', type: 'taskNode', position: { x: 100, y: 100 }, data: {} },
      ];
      const newEdges: Edge[] = [
        { id: 'edge-1', source: 'agent-1', target: 'task-1' },
        { id: 'edge-2', source: 'agent-2', target: 'task-2' },
      ];

      const setNodes = vi.fn();
      const setEdges = vi.fn();

      handleNodesGenerated(newNodes, newEdges, setNodes, setEdges);

      // For crew generation, should replace all
      expect(setNodes).toHaveBeenCalledWith(newNodes);
      expect(setEdges).toHaveBeenCalledWith(newEdges);
    });

    it('appends with deduplication for individual node generation', () => {
      const existingNodes: Node[] = [
        { id: 'existing-agent', type: 'agentNode', position: { x: 0, y: 0 }, data: {} },
      ];
      const existingEdges: Edge[] = [
        { id: 'existing-edge', source: 'a', target: 'b' },
      ];

      const newNodes: Node[] = [
        { id: 'new-agent', type: 'agentNode', position: { x: 100, y: 0 }, data: {} },
      ];
      const newEdges: Edge[] = [
        { id: 'new-edge', source: 'c', target: 'd' },
      ];

      const setNodes = vi.fn((updater) => {
        if (typeof updater === 'function') {
          return updater(existingNodes);
        }
        return updater;
      });
      const setEdges = vi.fn((updater) => {
        if (typeof updater === 'function') {
          return updater(existingEdges);
        }
        return updater;
      });

      handleNodesGenerated(newNodes, newEdges, setNodes, setEdges);

      // For individual generation, should call with updater function
      expect(setNodes).toHaveBeenCalled();
      expect(setEdges).toHaveBeenCalled();

      // Verify the updater function works correctly
      const nodeUpdater = setNodes.mock.calls[0][0];
      if (typeof nodeUpdater === 'function') {
        const result = nodeUpdater(existingNodes);
        expect(result).toHaveLength(2);
        expect(result[0].id).toBe('existing-agent');
        expect(result[1].id).toBe('new-agent');
      }
    });

    it('deduplicates nodes by ID when appending', () => {
      const existingNodes: Node[] = [
        { id: 'agent-1', type: 'agentNode', position: { x: 0, y: 0 }, data: { label: 'existing' } },
      ];

      const newNodes: Node[] = [
        { id: 'agent-1', type: 'agentNode', position: { x: 100, y: 0 }, data: { label: 'new' } },
      ];
      const newEdges: Edge[] = [];

      const setNodes = vi.fn((updater) => {
        if (typeof updater === 'function') {
          return updater(existingNodes);
        }
        return updater;
      });
      const setEdges = vi.fn();

      handleNodesGenerated(newNodes, newEdges, setNodes, setEdges);

      const nodeUpdater = setNodes.mock.calls[0][0];
      if (typeof nodeUpdater === 'function') {
        const result = nodeUpdater(existingNodes);
        // Should not add duplicate
        expect(result).toHaveLength(1);
        expect(result[0].data.label).toBe('existing');
      }
    });

    it('deduplicates edges by source-target when appending', () => {
      const existingEdges: Edge[] = [
        { id: 'edge-1', source: 'a', target: 'b' },
      ];

      const newNodes: Node[] = [
        { id: 'single-agent', type: 'agentNode', position: { x: 0, y: 0 }, data: {} },
      ];
      const newEdges: Edge[] = [
        { id: 'edge-new', source: 'a', target: 'b' }, // Same source-target as existing
        { id: 'edge-2', source: 'c', target: 'd' }, // New unique edge
      ];

      const setNodes = vi.fn();
      const setEdges = vi.fn((updater) => {
        if (typeof updater === 'function') {
          return updater(existingEdges);
        }
        return updater;
      });

      handleNodesGenerated(newNodes, newEdges, setNodes, setEdges);

      const edgeUpdater = setEdges.mock.calls[0][0];
      if (typeof edgeUpdater === 'function') {
        const result = edgeUpdater(existingEdges);
        // Should only add the unique edge
        expect(result).toHaveLength(2);
        expect(result[0].id).toBe('edge-1');
        expect(result[1].id).toBe('edge-2');
      }
    });
  });

  describe('isExecuteCommand', () => {
    it('returns true for "execute crew"', () => {
      expect(isExecuteCommand('execute crew')).toBe(true);
    });

    it('returns true for "execute crew" with leading/trailing spaces', () => {
      expect(isExecuteCommand('  execute crew  ')).toBe(true);
    });

    it('returns true for "Execute Crew" (case insensitive)', () => {
      expect(isExecuteCommand('Execute Crew')).toBe(true);
    });

    it('returns true for "EXECUTE CREW" (uppercase)', () => {
      expect(isExecuteCommand('EXECUTE CREW')).toBe(true);
    });

    it('returns true for "ec"', () => {
      expect(isExecuteCommand('ec')).toBe(true);
    });

    it('returns true for "EC" (case insensitive)', () => {
      expect(isExecuteCommand('EC')).toBe(true);
    });

    it('returns true for "run"', () => {
      expect(isExecuteCommand('run')).toBe(true);
    });

    it('returns true for "execute"', () => {
      expect(isExecuteCommand('execute')).toBe(true);
    });

    it('returns true for "ec " with job ID', () => {
      expect(isExecuteCommand('ec job-123')).toBe(true);
    });

    it('returns true for "execute crew " with job ID', () => {
      expect(isExecuteCommand('execute crew job-456')).toBe(true);
    });

    it('returns false for random text', () => {
      expect(isExecuteCommand('hello world')).toBe(false);
    });

    it('returns false for partial matches', () => {
      expect(isExecuteCommand('executing')).toBe(false);
    });

    it('returns false for "ecx" (not a valid command)', () => {
      expect(isExecuteCommand('ecx')).toBe(false);
    });

    it('returns false for "execute crews"', () => {
      expect(isExecuteCommand('execute crews')).toBe(false);
    });

    it('returns false for empty string', () => {
      expect(isExecuteCommand('')).toBe(false);
    });

    it('returns false for whitespace only', () => {
      expect(isExecuteCommand('   ')).toBe(false);
    });
  });

  describe('extractJobIdFromCommand', () => {
    it('extracts job ID from "ec job-123"', () => {
      expect(extractJobIdFromCommand('ec job-123')).toBe('job-123');
    });

    it('extracts job ID from "ec " with UUID', () => {
      expect(extractJobIdFromCommand('ec 550e8400-e29b-41d4-a716-446655440000')).toBe('550e8400-e29b-41d4-a716-446655440000');
    });

    it('extracts job ID from "execute crew job-456"', () => {
      expect(extractJobIdFromCommand('execute crew job-456')).toBe('job-456');
    });

    it('extracts job ID preserving original case', () => {
      expect(extractJobIdFromCommand('EC Job-ABC-123')).toBe('Job-ABC-123');
    });

    it('trims whitespace from extracted job ID', () => {
      expect(extractJobIdFromCommand('ec   job-123  ')).toBe('job-123');
    });

    it('returns null for "ec" without job ID', () => {
      expect(extractJobIdFromCommand('ec')).toBe(null);
    });

    it('returns null for "execute crew" without job ID', () => {
      expect(extractJobIdFromCommand('execute crew')).toBe(null);
    });

    it('returns null for "run" command', () => {
      expect(extractJobIdFromCommand('run')).toBe(null);
    });

    it('returns null for "execute" command', () => {
      expect(extractJobIdFromCommand('execute')).toBe(null);
    });

    it('returns null for unrelated commands', () => {
      expect(extractJobIdFromCommand('hello world')).toBe(null);
    });

    it('returns null for empty string', () => {
      expect(extractJobIdFromCommand('')).toBe(null);
    });

    it('handles "Execute Crew " with case variations', () => {
      expect(extractJobIdFromCommand('Execute Crew my-job-id')).toBe('my-job-id');
    });
  });

  describe('SLASH_COMMANDS', () => {
    it('contains 12 commands', () => {
      expect(SLASH_COMMANDS).toHaveLength(12);
    });

    it('every command starts with /', () => {
      SLASH_COMMANDS.forEach(cmd => {
        expect(cmd.command).toMatch(/^\//);
      });
    });

    it('every command has a non-empty description', () => {
      SLASH_COMMANDS.forEach(cmd => {
        expect(cmd.description.length).toBeGreaterThan(0);
      });
    });

    it('every command has a valid category', () => {
      const validCategories = ['crew', 'flow', 'general'];
      SLASH_COMMANDS.forEach(cmd => {
        expect(validCategories).toContain(cmd.category);
      });
    });

    it('includes /help command', () => {
      expect(SLASH_COMMANDS.find(c => c.command === '/help')).toBeDefined();
    });

    it('includes crew commands (list, load, save, run, delete, schedule)', () => {
      const crewCommands = SLASH_COMMANDS.filter(c => c.category === 'crew');
      expect(crewCommands.length).toBe(6);
    });

    it('includes flow commands (list, load, save, run, delete)', () => {
      const flowCommands = SLASH_COMMANDS.filter(c => c.category === 'flow');
      expect(flowCommands.length).toBe(5);
    });
  });

  describe('filterSlashCommands', () => {
    it('returns all commands for "/"', () => {
      const results = filterSlashCommands('/');
      expect(results).toHaveLength(SLASH_COMMANDS.length);
    });

    it('filters to list commands for "/list"', () => {
      const results = filterSlashCommands('/list');
      expect(results).toHaveLength(2);
      expect(results.every(c => c.command.startsWith('/list'))).toBe(true);
    });

    it('filters to "/list crews" for "/list c"', () => {
      const results = filterSlashCommands('/list c');
      expect(results).toHaveLength(1);
      expect(results[0].command).toBe('/list crews');
    });

    it('filters to run commands for "/run"', () => {
      const results = filterSlashCommands('/run');
      expect(results).toHaveLength(2);
      expect(results.every(c => c.command.startsWith('/run'))).toBe(true);
    });

    it('returns only /help for "/help"', () => {
      const results = filterSlashCommands('/help');
      expect(results).toHaveLength(1);
      expect(results[0].command).toBe('/help');
    });

    it('returns empty array for non-matching input "/xyz"', () => {
      const results = filterSlashCommands('/xyz');
      expect(results).toHaveLength(0);
    });

    it('is case-insensitive', () => {
      const results = filterSlashCommands('/LIST');
      expect(results).toHaveLength(2);
    });

    it('filters to crew-specific commands for "/load crew"', () => {
      const results = filterSlashCommands('/load crew');
      expect(results).toHaveLength(1);
      expect(results[0].command).toBe('/load crew');
    });

    it('filters to delete commands for "/delete"', () => {
      const results = filterSlashCommands('/delete');
      expect(results).toHaveLength(2);
      expect(results.every(c => c.command.startsWith('/delete'))).toBe(true);
    });
  });
});
