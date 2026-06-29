/**
 * "Load from Catalog" (the "+" tab menu) must match the active canvas:
 * flow canvas -> Flows tab, crew canvas -> Crews tab. Previously it always
 * opened the Crews tab regardless of which canvas you were on.
 */
import { describe, it, expect, vi } from 'vitest';
import { readFileSync } from 'fs';
import { resolve } from 'path';
import {
  catalogTabForCanvas,
  openCatalogForCanvas,
  CATALOG_CREWS_TAB,
  CATALOG_FLOWS_TAB,
} from './WorkflowEventHandlers';

describe('catalogTabForCanvas', () => {
  it('opens the Flows tab on the flow canvas', () => {
    expect(catalogTabForCanvas(true)).toBe(CATALOG_FLOWS_TAB);
  });

  it('opens the Crews tab on the crew canvas', () => {
    expect(catalogTabForCanvas(false)).toBe(CATALOG_CREWS_TAB);
  });

  it('maps Crews to tab 0 and Flows to tab 3 (CrewFlowSelectionDialog indices)', () => {
    expect(CATALOG_CREWS_TAB).toBe(0);
    expect(CATALOG_FLOWS_TAB).toBe(3);
  });
});

describe('openCatalogForCanvas', () => {
  const makeSetters = () => ({
    setInitialTab: vi.fn(),
    setShowOnlyTab: vi.fn(),
    setOpen: vi.fn(),
  });

  it('pins the dialog to the Flows tab and opens it on the flow canvas', () => {
    const setters = makeSetters();
    openCatalogForCanvas(true, setters);
    expect(setters.setInitialTab).toHaveBeenCalledWith(CATALOG_FLOWS_TAB);
    expect(setters.setShowOnlyTab).toHaveBeenCalledWith(CATALOG_FLOWS_TAB);
    expect(setters.setOpen).toHaveBeenCalledWith(true);
  });

  it('pins the dialog to the Crews tab on the crew canvas', () => {
    const setters = makeSetters();
    openCatalogForCanvas(false, setters);
    expect(setters.setInitialTab).toHaveBeenCalledWith(CATALOG_CREWS_TAB);
    expect(setters.setShowOnlyTab).toHaveBeenCalledWith(CATALOG_CREWS_TAB);
    expect(setters.setOpen).toHaveBeenCalledWith(true);
  });
});

describe('WorkflowDesigner wires Load from Catalog to the canvas (wiring)', () => {
  it('routes onLoadCrew through openCatalogForCanvas(areFlowsVisible, ...)', () => {
    const src = readFileSync(resolve(__dirname, 'WorkflowDesigner.tsx'), 'utf-8');
    expect(src).toContain('openCatalogForCanvas(areFlowsVisible');
  });
});
