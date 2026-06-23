import { describe, it, expect } from 'vitest';
import {
  assessTrifecta,
  assessChatModeTrifecta,
  classifyMcpServer,
} from './toolCapabilityManifest';

// Capability bit-flags (mirror of the private constants in the module under test)
const CAP_SENSITIVE = 1 << 0;
const CAP_UNTRUSTED = 1 << 1;
const CAP_EXTERNAL = 1 << 2;
const CAP_DESTRUCTIVE = 1 << 3;

describe('classifyMcpServer', () => {
  it('flags Databricks SQL as sensitive + external + destructive (not untrusted)', () => {
    const caps = classifyMcpServer('Databricks SQL');
    expect(caps & CAP_SENSITIVE).toBeTruthy();
    expect(caps & CAP_EXTERNAL).toBeTruthy();
    expect(caps & CAP_DESTRUCTIVE).toBeTruthy();
    expect(caps & CAP_UNTRUSTED).toBeFalsy();
  });

  it('matches Unity Catalog Functions despite the dynamic (catalog.schema) suffix', () => {
    const caps = classifyMcpServer('Unity Catalog Functions (main.default)');
    expect(caps & CAP_SENSITIVE).toBeTruthy();
    expect(caps & CAP_EXTERNAL).toBeTruthy();
    expect(caps & CAP_DESTRUCTIVE).toBeFalsy();
  });

  it('treats UC system.ai functions as destructive (python_exec)', () => {
    const caps = classifyMcpServer('Unity Catalog Functions (system.ai)');
    expect(caps & CAP_DESTRUCTIVE).toBeTruthy();
  });

  it('flags Genie, AI Search and Vector Search as sensitive + external, not untrusted', () => {
    for (const name of ['Genie', 'AI Search Indexes', 'Databricks Vector Search']) {
      const caps = classifyMcpServer(name);
      expect(caps & CAP_SENSITIVE).toBeTruthy();
      expect(caps & CAP_EXTERNAL).toBeTruthy();
      expect(caps & CAP_UNTRUSTED).toBeFalsy();
    }
  });

  it('is case-insensitive (picker passes display names verbatim)', () => {
    expect(classifyMcpServer('databricks sql') & CAP_DESTRUCTIVE).toBeTruthy();
    expect(classifyMcpServer('DATABRICKS SQL') & CAP_DESTRUCTIVE).toBeTruthy();
  });

  it('defaults unknown/custom servers to untrusted + external (assume-the-worst)', () => {
    const caps = classifyMcpServer('Some Custom Slack MCP');
    expect(caps & CAP_UNTRUSTED).toBeTruthy();
    expect(caps & CAP_EXTERNAL).toBeTruthy();
    expect(caps & CAP_SENSITIVE).toBeFalsy();
  });

  it('returns 0 for an empty name', () => {
    expect(classifyMcpServer('')).toBe(0);
  });
});

describe('assessChatModeTrifecta', () => {
  it('does not fire for a single internal source alone', () => {
    expect(assessChatModeTrifecta({ mcpServers: ['Genie'] }).hasTrifecta).toBe(false);
  });

  it('does not fire for two internal sources with no egress channel', () => {
    const a = assessChatModeTrifecta({ mcpServers: ['Genie', 'Databricks SQL'] });
    expect(a.hasTrifecta).toBe(false);
    expect(a.readsSensitive).toBe(true);
    expect(a.ingestsUntrusted).toBe(false);
  });

  it('fires when an internal source is combined with an unknown MCP server', () => {
    const a = assessChatModeTrifecta({ mcpServers: ['Genie', 'Some Custom MCP'] });
    expect(a.hasTrifecta).toBe(true);
    expect(a.sensitiveTools).toContain('Genie');
    expect(a.untrustedTools).toContain('Some Custom MCP');
  });

  it('fires for Databricks SQL plus a web/catalog tool and reports it destructive', () => {
    const a = assessChatModeTrifecta({
      mcpServers: ['Databricks SQL'],
      toolTitles: ['ScrapeWebsiteTool'],
    });
    expect(a.hasTrifecta).toBe(true);
    expect(a.destructiveTools).toContain('Databricks SQL');
  });

  it('fires for a selected Agent Bricks endpoint on its own', () => {
    const a = assessChatModeTrifecta({ agentBricksEndpoints: ['my-agent'] });
    expect(a.hasTrifecta).toBe(true);
    expect(a.sensitiveTools).toContain('my-agent');
    expect(a.untrustedTools).toContain('my-agent');
    expect(a.externalTools).toContain('my-agent');
  });

  it('does not fire for web tools only (no internal data at risk)', () => {
    const a = assessChatModeTrifecta({ toolTitles: ['SerperDevTool', 'ScrapeWebsiteTool'] });
    expect(a.hasTrifecta).toBe(false);
    expect(a.readsSensitive).toBe(false);
  });

  it('returns empty assessment for no inputs', () => {
    const a = assessChatModeTrifecta({});
    expect(a.hasTrifecta).toBe(false);
    expect(a.sensitiveTools).toEqual([]);
  });
});

describe('assessTrifecta (catalog tools) — Agent Bricks now trips alone', () => {
  it('classifies AgentBricksTool as all three dimensions', () => {
    const a = assessTrifecta(['AgentBricksTool']);
    expect(a.hasTrifecta).toBe(true);
  });
});
