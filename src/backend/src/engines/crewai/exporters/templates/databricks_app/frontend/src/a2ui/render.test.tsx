import { describe, it, expect } from 'vitest'
import { renderToStaticMarkup } from 'react-dom/server'
import { A2UIRenderer } from './A2UIRenderer'
import type { Surface } from './types'

const html = (s: Surface) => renderToStaticMarkup(<A2UIRenderer payload={s} />)

describe('A2UIRenderer', () => {
  it('renders a presentation with slide nav and resolves bindings', () => {
    const out = html({
      surfaceKind: 'presentation',
      root: 'deck',
      components: [
        { id: 'deck', component: 'SlideDeck', children: ['s1', 's2'] },
        { id: 's1', component: 'Slide', title: 'Overview', children: ['t1'] },
        { id: 't1', component: 'Markdown', content: { path: '/body0' } },
        { id: 's2', component: 'Slide', title: 'Results', children: [] },
      ],
      dataModel: { body0: '## Hello' },
    })
    expect(out).toContain('Overview')
    expect(out).toContain('1 / 2') // deck nav
    expect(out).toContain('Hello') // binding resolved + markdown rendered
  })

  it('renders a dashboard with KeyValue + Table from bindings', () => {
    const out = html({
      surfaceKind: 'dashboard',
      root: 'g',
      components: [
        { id: 'g', component: 'Grid', columns: 2, children: ['kv', 'tbl'] },
        { id: 'kv', component: 'KeyValue', label: 'Revenue', value: { path: '/rev' } },
        { id: 'tbl', component: 'Table', columns: ['City', 'Sales'], rows: { path: '/rows' } },
      ],
      dataModel: { rev: '$1.2M', rows: [['NYC', 10], ['LA', 8]] },
    })
    expect(out).toContain('Revenue')
    expect(out).toContain('$1.2M')
    expect(out).toContain('NYC')
  })

  it('renders a mindmap tree', () => {
    const out = html({
      surfaceKind: 'mindmap',
      root: 'm',
      components: [{ id: 'm', component: 'Mindmap', root: { path: '/tree' } }],
      dataModel: { tree: { id: 'r', label: 'ML', children: [{ id: 'a', label: 'Supervised' }] } },
    })
    expect(out).toContain('ML')
    expect(out).toContain('Supervised')
  })

  it('renders a quiz with its first question and options from bindings', () => {
    const out = html({
      surfaceKind: 'quiz',
      root: 'q',
      components: [{ id: 'q', component: 'Quiz', title: 'ML Quiz', questions: { path: '/qs' } }],
      dataModel: {
        qs: [
          {
            question: 'What is supervised learning?',
            options: ['Labeled data', 'No data', 'Only images', 'Random'],
            answer: 0,
          },
        ],
      },
    })
    expect(out).toContain('ML Quiz')
    expect(out).toContain('What is supervised learning?')
    expect(out).toContain('Labeled data')
    expect(out).toContain('Question 1 of 1')
  })

  it('falls back to Unsupported for unknown components', () => {
    const out = html({
      surfaceKind: 'document',
      root: 'x',
      components: [{ id: 'x', component: 'Hologram' }],
    })
    expect(out).toContain('Unsupported component: Hologram')
  })
})
