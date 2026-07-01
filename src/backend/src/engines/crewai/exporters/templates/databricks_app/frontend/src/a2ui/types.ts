// A2UI wire types — must match agent_server/a2ui_catalog.json (the shared contract).

export type Binding = { path: string }

export interface ComponentNode {
  id: string
  component: string
  children?: string[]
  // arbitrary literal-or-binding props
  [key: string]: unknown
}

export interface Surface {
  surfaceKind: string
  root: string
  components: ComponentNode[]
  dataModel?: Record<string, unknown>
}

// Props every registered renderer receives.
export interface NodeProps {
  node: ComponentNode
  // render a child component by id
  render: (id: string) => JSX.Element
  // resolve a literal-or-binding value against the surface dataModel
  resolve: (value: unknown) => unknown
}
