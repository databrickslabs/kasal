import { describe, it, expect } from 'vitest'
import { render, screen } from '@testing-library/react'
import ReactMarkdown from 'react-markdown'
import remarkGfm from 'remark-gfm'
import { MarkdownImage, SafeImage } from './safeImage'
import { mdComponents } from './markdown'

const REMOTE = 'https://images.example.org/c.png?d=1'

// jsdom never fetches images, so "no request" means: no <img> carrying the
// remote URL is in the DOM. That element is what makes a browser fetch it.
const imgSources = (root: HTMLElement) => Array.from(root.querySelectorAll('img')).map((i) => i.getAttribute('src'))

describe('SafeImage', () => {
  it('shows a remote image as a placeholder whose link opens it in a new tab', () => {
    const { container } = render(<SafeImage src={REMOTE} alt="chart" />)
    expect(container.querySelector('img')).toBeNull()
    expect(screen.getByText(/images\.example\.org/)).toBeInTheDocument()
    const load = screen.getByRole('link', { name: 'Load image' })
    expect(load).toHaveAttribute('href', REMOTE)
    expect(load).toHaveAttribute('target', '_blank')
    expect(load.getAttribute('rel')).toContain('noopener')
    expect(load.getAttribute('rel')).toContain('noreferrer')
  })

  it('renders data:, blob: and same-origin images directly', () => {
    const { container } = render(
      <>
        <SafeImage src="data:image/png;base64,iVBORw0KGgo=" alt="a" />
        <SafeImage src="blob:http://localhost:3000/abc" alt="b" />
        <SafeImage src="/logo192.png" alt="c" className="rounded" />
      </>,
    )
    expect(imgSources(container)).toEqual(['data:image/png;base64,iVBORw0KGgo=', 'blob:http://localhost:3000/abc', '/logo192.png'])
    expect(container.querySelector('img.rounded')).not.toBeNull()
    expect(screen.queryByRole('link')).toBeNull()
  })

  it('renders only the alt text for a source it will not load or link', () => {
    const { container } = render(<SafeImage src="javascript:alert(1)" alt="pic" />)
    expect(container.querySelector('img')).toBeNull()
    expect(screen.queryByRole('link')).toBeNull()
    expect(screen.getByText('pic')).toBeInTheDocument()
  })
})

describe('MarkdownImage', () => {
  it('never forwards srcset or style, which could name another image', () => {
    const { container } = render(
      <MarkdownImage
        src="/a.png"
        alt="a"
        {...({ srcSet: `${REMOTE} 2x`, style: { backgroundImage: `url(${REMOTE})` } } as object)}
      />,
    )
    const img = container.querySelector('img')!
    expect(img.getAttribute('src')).toBe('/a.png')
    expect(img.hasAttribute('srcset')).toBe(false)
    expect(img.hasAttribute('style')).toBe(false)
  })
})

describe('mdComponents (A2UI Markdown, memory records, the exported chat bubble)', () => {
  const md = `See ![chart](${REMOTE}) and ![logo](/logo192.png).\n\n[the source](https://example.com/page)`

  it('holds remote images back, loads same-origin ones and keeps links as they were', () => {
    const { container } = render(
      <ReactMarkdown remarkPlugins={[remarkGfm]} components={mdComponents}>
        {md}
      </ReactMarkdown>,
    )
    expect(imgSources(container)).toEqual(['/logo192.png'])
    expect(screen.getByRole('link', { name: 'Load image' })).toHaveAttribute('href', REMOTE)
    const link = screen.getByRole('link', { name: 'the source' })
    expect(link).toHaveAttribute('href', 'https://example.com/page')
    expect(link).toHaveAttribute('target', '_blank')
  })
})
