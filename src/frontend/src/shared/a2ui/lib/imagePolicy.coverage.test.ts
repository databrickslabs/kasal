import { readdirSync, readFileSync, statSync } from 'fs'
import { join, relative } from 'path'
import { describe, it, expect } from 'vitest'

// Every markdown renderer and every A2UI media component goes through the one
// image policy (imagePolicy.ts + safeImage.tsx), so a new renderer cannot load
// remote images on render again by forgetting it.
const FRONTEND_SRC = join(__dirname, '..', '..', '..')
const EXPORT_APP_SRC = join(FRONTEND_SRC, '..', '..', 'backend', 'src', 'services', 'export', 'templates', 'databricks_app', 'frontend', 'src')

const sourceFiles = (root: string): string[] =>
  readdirSync(root).flatMap((name) => {
    const path = join(root, name)
    if (statSync(path).isDirectory()) return name === 'node_modules' ? [] : sourceFiles(path)
    return /\.tsx?$/.test(name) && !/\.test\./.test(name) ? [path] : []
  })

const ON_POLICY = /\b(safeImageComponents|mdComponents|MarkdownImage|SafeImage)\b|disallowedElements=\{\[\s*'img'/

// Files that render markdown without the policy, each with the reason.
const NOT_YET_ON_POLICY: Record<string, string> = {
  // Metric-view migration results; that viewer is owned by the metric-view
  // work. The page CSP (img-src) still blocks any remote image it renders.
  'features/executions/components/UCMVResultViewer.tsx': 'metric-view viewer',
}

describe('image policy coverage', () => {
  it('every ReactMarkdown renderer applies the image policy', () => {
    const offenders = [...sourceFiles(FRONTEND_SRC), ...sourceFiles(EXPORT_APP_SRC)]
      .filter((path) => readFileSync(path, 'utf8').includes('<ReactMarkdown'))
      .filter((path) => !ON_POLICY.test(readFileSync(path, 'utf8')))
      .map((path) => relative(FRONTEND_SRC, path))
      .filter((rel) => !(rel in NOT_YET_ON_POLICY))
    expect(offenders).toEqual([])
  })

  it('A2UI components render images only through SafeImage', () => {
    const a2ui = join(FRONTEND_SRC, 'shared', 'a2ui')
    const code = (path: string) => readFileSync(path, 'utf8').replace(/\/\*[\s\S]*?\*\/|\/\/[^\n]*/g, '')
    const offenders = sourceFiles(a2ui)
      .filter((path) => !path.endsWith(join('lib', 'safeImage.tsx')))
      .filter((path) => /<img\b|<video\b|<audio\b|<source\b|poster=|srcSet=/.test(code(path)))
      .map((path) => relative(a2ui, path))
    expect(offenders).toEqual([])
  })

  it('the exempt list only names files that still exist and still need it', () => {
    for (const rel of Object.keys(NOT_YET_ON_POLICY)) {
      const text = readFileSync(join(FRONTEND_SRC, rel), 'utf8')
      expect(text.includes('<ReactMarkdown') && !ON_POLICY.test(text), rel).toBe(true)
    }
  })
})
