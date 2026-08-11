import { describe, test, expect } from 'vitest';
import React from 'react';
import { render, screen } from '@testing-library/react';
import { ThemeProvider, createTheme } from '@mui/material/styles';
import TracePlanView, { extractPlanItems } from './TracePlanView';

const theme = createTheme();

const show = (items: Parameters<typeof TracePlanView>[0]['items']) =>
  render(
    <ThemeProvider theme={theme}>
      <TracePlanView items={items} />
    </ThemeProvider>,
  );

// The exact payload the trace stores for a `todo` tool call: tool_args is a
// Python repr, not JSON.
const TOOL_USAGE_EVENT = {
  duration_ms: 0.15,
  extra_data: {
    task_name: 'Design the complete architecture',
    tool_name: 'todo',
    tool_args:
      "{'todos': [{'content': 'Compile complete architecture specification document', 'id': '1', 'status': 'in_progress'}, " +
      "{'content': 'Include frontend design with Vite/TypeScript', 'id': '2', 'status': 'pending'}, " +
      "{'content': 'Include backend design with Python', 'id': '3', 'status': 'pending'}]}",
    tool_class: 'PlanTool',
  },
};

// What the engine's own plan_updated event carries.
const PLAN_UPDATED_EVENT = {
  extra_data: {
    plan_items: JSON.stringify([
      { id: '1', content: 'Read the schema', status: 'completed' },
      { id: '2', content: 'Build the metric view', status: 'in_progress' },
      { id: '3', content: 'Regional breakdown', status: 'cancelled' },
    ]),
    plan_total: 3,
    plan_completed: 1,
  },
};

describe('extractPlanItems', () => {
  test('reads the engine plan_updated payload', () => {
    const items = extractPlanItems(PLAN_UPDATED_EVENT);
    expect(items?.map((i) => i.id)).toEqual(['1', '2', '3']);
    expect(items?.[1].status).toBe('in_progress');
  });

  test('parses the Python-repr tool_args from a todo call', () => {
    // This is the shape that rendered as an unreadable wall of JSON.
    const items = extractPlanItems(TOOL_USAGE_EVENT);
    expect(items).toHaveLength(3);
    expect(items?.[0].content).toContain('Compile complete architecture');
    expect(items?.[0].status).toBe('in_progress');
  });

  test('handles None/True/False in a repr payload', () => {
    const items = extractPlanItems({
      extra_data: {
        tool_args: "{'todos': [{'content': 'x', 'id': '1', 'status': 'pending', 'extra': None}]}",
      },
    });
    expect(items).toHaveLength(1);
  });

  test('survives apostrophes inside item text', () => {
    const items = extractPlanItems({
      extra_data: {
        plan_items: JSON.stringify([{ id: '1', content: "don't break", status: 'pending' }]),
      },
    });
    expect(items?.[0].content).toBe("don't break");
  });

  test('returns null for events with no plan, so the raw view still shows', () => {
    expect(extractPlanItems({ extra_data: { tool_name: 'read_file' } })).toBeNull();
    expect(extractPlanItems({})).toBeNull();
    expect(extractPlanItems(null)).toBeNull();
    expect(extractPlanItems('a string')).toBeNull();
  });

  test('unparseable arguments degrade to null rather than throwing', () => {
    expect(
      extractPlanItems({ extra_data: { tool_args: "{'todos': [ this is not parseable" } }),
    ).toBeNull();
  });

  // Python quotes each string independently: single quotes normally, double
  // quotes the moment the content holds an apostrophe. A regex matching '...'
  // read that apostrophe as a delimiter and produced unparseable JSON, so a
  // plan rendered as a checklist or as raw JSON depending on nothing more than
  // whether the model wrote a possessive.
  test('an apostrophe in the content still parses', () => {
    const items = extractPlanItems({
      extra_data: {
        tool_args:
          "{'todos': [{'id': '1', 'content': 'Research the market', 'status': 'completed'}, " +
          '{\'id\': \'2\', \'content\': "Research Databricks\' role in banking", \'status\': \'pending\'}]}',
      },
    });
    expect(items).toHaveLength(2);
    expect(items?.[1].content).toBe("Research Databricks' role in banking");
  });

  test('a double quote inside a single-quoted string survives', () => {
    const items = extractPlanItems({
      extra_data: {
        tool_args: "{'todos': [{'id': '1', 'content': 'He said \"go\" then left', 'status': 'pending'}]}",
      },
    });
    expect(items?.[0].content).toBe('He said "go" then left');
  });

  test('a short label is kept when the model writes one', () => {
    const items = extractPlanItems({
      extra_data: {
        tool_args:
          "{'todos': [{'id': '1', 'content': 'Research business drivers and market need', " +
          "'status': 'in_progress', 'label': 'Research why agentic banking'}]}",
      },
    });
    expect(items?.[0].label).toBe('Research why agentic banking');
    expect(items?.[0].content).toBe('Research business drivers and market need');
  });
});

describe('TracePlanView', () => {
  const ITEMS = [
    { id: '1', content: 'Read the schema', status: 'completed' },
    { id: '2', content: 'Build the metric view', status: 'in_progress' },
    { id: '3', content: 'Regional breakdown', status: 'cancelled' },
    { id: '4', content: 'Publish the dashboard', status: 'pending' },
  ];

  test('shows progress at a glance', () => {
    show(ITEMS);
    expect(screen.getByText('1/4 done')).toBeInTheDocument();
    expect(screen.getByText('1 cancelled')).toBeInTheDocument();
  });

  test('names what the agent is doing right now', () => {
    show(ITEMS);
    expect(screen.getByText(/Currently: Build the metric view/)).toBeInTheDocument();
  });

  test('renders every item', () => {
    show(ITEMS);
    for (const item of ITEMS) {
      expect(screen.getByText(item.content)).toBeInTheDocument();
    }
  });

  test('a fully completed plan reads as done', () => {
    show([{ id: '1', content: 'only step', status: 'completed' }]);
    expect(screen.getByText('1/1 done')).toBeInTheDocument();
  });

  test('a short label becomes the headline, the sentence the detail', () => {
    show([
      {
        id: '1',
        content: 'Research business drivers and market need for agentic banking',
        status: 'in_progress',
        label: 'Research why agentic banking',
      },
    ]);
    expect(screen.getByText('Research why agentic banking')).toBeInTheDocument();
    expect(
      screen.getByText('Research business drivers and market need for agentic banking'),
    ).toBeInTheDocument();
    // The label is what "Currently:" names — the sentence is too long for it.
    expect(screen.getByText(/Currently: Research why agentic banking/)).toBeInTheDocument();
  });

  test('an item without a label still renders its content once', () => {
    show([{ id: '1', content: 'Plain step', status: 'pending' }]);
    expect(screen.getAllByText('Plain step')).toHaveLength(1);
  });
});
