import type { FC } from 'react'
import type { NodeProps } from './types'
import {
  Markdown, Text, Heading, Image, Card, KeyValue, List, Table, Divider,
  Row, Column, Grid, Chart, SlideDeck, Slide, Mindmap, Quiz, Unsupported,
} from './components'

// The extensibility seam: component name -> React renderer. To add a new A2UI
// component, add it to agent_server/a2ui_catalog.json AND register it here.
export const registry: Record<string, FC<NodeProps>> = {
  Markdown, Text, Heading, Image, Card, KeyValue, List, Table, Divider,
  Row, Column, Grid, Chart, SlideDeck, Slide, Mindmap, Quiz,
}

export { Unsupported }
