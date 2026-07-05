import { describe, it, expect } from 'vitest';
import { friendlyStep, isWebSearch } from './RunTimeline';

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

  it('does NOT call a non-web catalog/data search a web search', () => {
    // Regression: "search" alone used to route every *_search_* tool to
    // "Searching the web" — an ontos data-product search never touches the web.
    expect(friendlyStep('ontos_search_data_products')).toBe('Searching');
    expect(friendlyStep('knowledge_base_search')).toBe('Searching');
    expect(friendlyStep('vector_search_lookup')).toBe('Searching');
  });

  it('still labels genuine web-search tools as web searches', () => {
    expect(friendlyStep('serper_dev_tool')).toBe('Searching the web');
    expect(friendlyStep('tavily_search')).toBe('Searching the web');
    expect(friendlyStep('web_search')).toBe('Searching the web');
    expect(friendlyStep('brave_search')).toBe('Searching the web');
  });

  it('isWebSearch is precise about web-only tools', () => {
    expect(isWebSearch('PerplexityTool')).toBe(true);
    expect(isWebSearch('search_the_web')).toBe(true);
    expect(isWebSearch('ontos_search_data_products')).toBe(false);
    expect(isWebSearch('genie_query')).toBe(false);
  });

  it('falls back to the raw label when unknown', () => {
    expect(friendlyStep('SomeCustomTool')).toBe('SomeCustomTool');
    expect(friendlyStep('')).toBe('Working');
  });
});
