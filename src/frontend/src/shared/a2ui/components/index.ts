/**
 * A2UI component renderers.
 *
 * This barrel is the module's public surface — `registry.tsx` and
 * `A2UIRenderer.tsx` import from `./components` and should not reach into the
 * individual files. Re-export anything new from here too.
 *
 * The renderers were one 2,742-line `components.tsx`; they are grouped by the
 * kind of surface they draw. The dependency order is strictly one-way:
 *
 *   values / icons / slideContext   (leaves, no component imports)
 *     -> primitives -> data -> diagrams / geo -> media -> slides -> interactive / mindmap
 *
 * Slides render their children through the `render` prop rather than importing
 * the other component modules, which is what keeps the graph acyclic.
 */

export { iconByName } from './icons'

export {
  Markdown,
  Text,
  Heading,
  Image,
  Card_,
  KeyValue,
  List,
  Divider,
  Row,
  Column,
  Grid,
  Unsupported,
} from './primitives'

export { Table, Chart, Forecast } from './data'

export { Graph, Sequence, Diagram, normDiagramItems } from './diagrams'
export type { DiagramItem } from './diagrams'

export { Album, GeoMap } from './media'

// Region shading + flow ribbons. Dependency-free SVG, like ./diagrams.
// RegionHeatmap shades a GRID of regions, not real geography — see ./geo.tsx.
export { RegionHeatmap, Sankey } from './geo'

export { Slide, SlideDeck } from './slides'
// Re-exported here so the public import path stays `components` even though the
// menu now lives in its own module.
export { SurfaceDownloadMenu } from './surfaceDownload'

export { Quiz, Flashcards } from './interactive'

export { Mindmap } from './mindmap'

// The renderer is `Card_` (the shadcn `Card` primitive owns the bare name);
// the A2UI component name on the wire is `Card`.
export { Card_ as Card } from './primitives'
