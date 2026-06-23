import { describe, it, expect } from 'vitest';
import { friendlyStep } from './RunTimeline';

describe('friendlyStep', () => {
  it('maps raw tool labels to readable phases', () => {
    expect(friendlyStep('Memory')).toBe('Recalling context');
    expect(friendlyStep('GenieTool')).toBe('Querying your data');
    expect(friendlyStep('PerplexityTool')).toBe('Searching the web');
    expect(friendlyStep('ScrapeWebsiteTool')).toBe('Reading sources');
    expect(friendlyStep('AgentBricksTool')).toBe('Consulting an agent');
    // SQL is detected BEFORE the file/read branch ("..._read_only" contains "read").
    expect(friendlyStep('databricks_sql_execute_sql_read_only')).toBe('Running a query');
  });

  it('falls back to the raw label when unknown', () => {
    expect(friendlyStep('SomeCustomTool')).toBe('SomeCustomTool');
    expect(friendlyStep('')).toBe('Working');
  });
});
