import type { FC } from 'react'
import type { NodeProps } from './types'
import {
  Markdown, Text, Heading, Image, Card, KeyValue, List, Table, Divider,
  Row, Column, Grid, Chart, SlideDeck, Slide, Mindmap, Quiz, Unsupported,
} from './components'

// The extensibility seam: component name -> React renderer. To add a new A2UI
// component, register its renderer here AND add it to the catalog the composer
// reads (the shared catalog.json default + the UIConfigurator's catalog) so the
// model is allowed to emit it. Names not in the registry render as Unsupported.
export const registry: Record<string, FC<NodeProps>> = {
  Markdown, Text, Heading, Image, Card, KeyValue, List, Table, Divider,
  Row, Column, Grid, Chart, SlideDeck, Slide, Mindmap, Quiz,
}

export { Unsupported }
