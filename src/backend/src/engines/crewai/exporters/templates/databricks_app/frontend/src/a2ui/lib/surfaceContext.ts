import { createContext } from 'react'
import type { Surface } from '../types'

// The full Surface being rendered, exposed so deep components (e.g. SlideDeck's
// PowerPoint export) can act on the whole tree — not just their own node. Lives
// in its own leaf module so both A2UIRenderer (provider) and components.tsx
// (consumer) can import it without a circular dependency.
export const SurfaceContext = createContext<Surface | null>(null)

// A surface's OWN download chrome, controlled by the host.
//  - `downloads`: whether to render the deck "Download" menu / table "CSV"
//    button. Default `true` so the inline chat thread and the exported app keep
//    them. The preview pane sets it `false` (its top toolbar owns one PDF/PPT
//    download instead), and the PDF rasterizer sets it `false` so buttons are
//    never baked into the exported page.
//  - `onDownloadPdf`: PDF export is HOST-specific (Kasal restores Tailwind
//    preflight via `.kasal-a2ui`; the export ships Tailwind v4) so the host
//    supplies it; when present the deck's download menu offers a PDF option
//    alongside the shared (DOM-free, pptxgenjs) PowerPoint export.
//  - `fit`: fit the deck to the available HEIGHT (letterbox the 16:9 stage) so the
//    whole slide is visible with no vertical scroll. The preview pane sets it; the
//    inline chat leaves it off (the deck keeps its natural width-driven height in
//    the scrolling thread). Requires every ancestor up to the bounded-height host
//    to be a flex column — see PreviewPanel.
export const SurfaceChromeContext = createContext<{
  downloads: boolean
  onDownloadPdf?: () => void
  fit?: boolean
}>({ downloads: true })
