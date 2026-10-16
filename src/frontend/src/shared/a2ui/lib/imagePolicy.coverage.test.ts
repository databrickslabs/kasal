import { readdirSync, readFileSync, statSync } from 'fs'
import { join, relative } from 'path'
import ts from 'typescript'
import { describe, it, expect } from 'vitest'

// Every markdown renderer and every A2UI media component goes through the one
// image policy (imagePolicy.ts + safeImage.tsx), so a new renderer cannot load
// remote images on render again by forgetting it. ESLint keeps react-markdown
// imports to an allowlist of files (eslint.config.js); this checks what they do.
const FRONTEND_SRC = join(__dirname, '..', '..', '..')
const EXPORT_APP_SRC = join(FRONTEND_SRC, '..', '..', 'backend', 'src', 'services', 'export', 'templates', 'databricks_app', 'frontend', 'src')

const sourceFiles = (root: string): string[] =>
  readdirSync(root).flatMap((name) => {
    const path = join(root, name)
    if (statSync(path).isDirectory()) return name === 'node_modules' ? [] : sourceFiles(path)
    return /\.tsx?$/.test(name) && !/\.test\./.test(name) ? [path] : []
  })

const ALL_SOURCES = [...sourceFiles(FRONTEND_SRC), ...sourceFiles(EXPORT_APP_SRC)]
const ON_POLICY = /\b(safeImageComponents|mdComponents|MarkdownImage|SafeImage)\b|disallowedElements=\{\[\s*'img'/
const DELEGATES = /\b(MarkdownImage|SafeImage)\b/

/** `img: …` overrides that neither are MarkdownImage nor delegate to it. */
function offPolicyImgOverrides(path: string, text = readFileSync(path, 'utf8')): string[] {
  const file = ts.createSourceFile(path, text, ts.ScriptTarget.Latest, true, ts.ScriptKind.TSX)
  const found: string[] = []
  const visit = (node: ts.Node) => {
    const offPolicy =
      (ts.isPropertyAssignment(node) && node.name.getText(file) === 'img' && !DELEGATES.test(node.initializer.getText(file))) ||
      (ts.isShorthandPropertyAssignment(node) && node.name.getText(file) === 'img')
    if (offPolicy) found.push(`${relative(FRONTEND_SRC, path)}:${file.getLineAndCharacterOfPosition(node.getStart(file)).line + 1}`)
    ts.forEachChild(node, visit)
  }
  visit(file)
  return found
}

describe('image policy coverage', () => {
  it('every ReactMarkdown renderer applies the image policy', () => {
    const offenders = ALL_SOURCES.filter((path) => readFileSync(path, 'utf8').includes('<ReactMarkdown'))
      .filter((path) => !ON_POLICY.test(readFileSync(path, 'utf8')))
      .map((path) => relative(FRONTEND_SRC, path))
    expect(offenders).toEqual([])
  })

  it('no img override replaces the policy, e.g. {...mdComponents, img: custom}', () => {
    const markdownFiles = ALL_SOURCES.filter((path) => /react-markdown|mdComponents|safeImageComponents/.test(readFileSync(path, 'utf8')))
    expect(markdownFiles.flatMap((path) => offPolicyImgOverrides(path))).toEqual([])
  })

  it('the override check catches a spread override, and accepts a delegating one', () => {
    const probe = join(FRONTEND_SRC, 'probe.tsx')
    expect(offPolicyImgOverrides(probe, 'const c = { ...mdComponents, img: (p) => <img {...p} /> }')).toHaveLength(1)
    expect(offPolicyImgOverrides(probe, 'const c = { ...mdComponents, img }')).toHaveLength(1)
    expect(offPolicyImgOverrides(probe, 'const c = { ...mdComponents, img: MarkdownImage }')).toEqual([])
    expect(offPolicyImgOverrides(probe, 'const c = { img: ({ src }) => <MarkdownImage src={fix(src)} /> }')).toEqual([])
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
})
