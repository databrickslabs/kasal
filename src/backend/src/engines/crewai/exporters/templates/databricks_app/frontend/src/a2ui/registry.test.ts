import { describe, it, expect } from 'vitest'
import catalog from '@catalog'
import { registry } from './registry'

// Guard: the frontend renderer registry and the backend's component catalog must
// stay in lockstep. Every catalog component needs a renderer, and vice versa.
describe('A2UI catalog ⟺ registry parity', () => {
  const catalogComponents = Object.keys((catalog as any).components || {})
  const registryComponents = Object.keys(registry)

  it('every catalog component has a renderer', () => {
    const missing = catalogComponents.filter((c) => !registryComponents.includes(c))
    expect(missing, `catalog components without a renderer: ${missing.join(', ')}`).toEqual([])
  })

  it('every renderer is declared in the catalog', () => {
    const orphan = registryComponents.filter((c) => !catalogComponents.includes(c))
    expect(orphan, `renderers not in catalog: ${orphan.join(', ')}`).toEqual([])
  })
})
