import { describe, it, expect } from 'vitest'
import { classifyImageSource, safeLinkHref } from './imagePolicy'

const PAGE = 'https://kasal.example.com/chat/run-1'

describe('classifyImageSource', () => {
  it('loads inline data: and blob: images directly', () => {
    expect(classifyImageSource('data:image/png;base64,iVBORw0KGgo=', PAGE)).toEqual({
      kind: 'inline',
      src: 'data:image/png;base64,iVBORw0KGgo=',
    })
    expect(classifyImageSource('blob:https://kasal.example.com/1234', PAGE)).toEqual({
      kind: 'inline',
      src: 'blob:https://kasal.example.com/1234',
    })
    expect(classifyImageSource('  DATA:image/gif;base64,R0lGOD=', PAGE).kind).toBe('inline')
  })

  it('loads same-origin images directly, relative or absolute', () => {
    expect(classifyImageSource('/docs/images/flow.png', PAGE)).toEqual({ kind: 'inline', src: '/docs/images/flow.png' })
    expect(classifyImageSource('images/flow.png', PAGE).kind).toBe('inline')
    expect(classifyImageSource('https://kasal.example.com/logo192.png', PAGE).kind).toBe('inline')
  })

  it('holds any other http(s) image back as external, with its host', () => {
    expect(classifyImageSource('https://images.example.org/c.png?d=1', PAGE)).toEqual({
      kind: 'external',
      url: 'https://images.example.org/c.png?d=1',
      host: 'images.example.org',
    })
    expect(classifyImageSource('http://example.net:8080/x.jpg', PAGE)).toMatchObject({
      kind: 'external',
      host: 'example.net:8080',
    })
    // Protocol-relative and other-port URLs are another origin too.
    expect(classifyImageSource('//cdn.example.org/x.png', PAGE)).toMatchObject({ kind: 'external', host: 'cdn.example.org' })
    expect(classifyImageSource('https://kasal.example.com:8443/x.png', PAGE).kind).toBe('external')
  })

  it('refuses sources that are neither inline nor http(s)', () => {
    for (const src of ['javascript:alert(1)', 'file:///etc/passwd', 'ftp://example.org/x.png', '', '   ', undefined, null, 42]) {
      expect(classifyImageSource(src, PAGE)).toEqual({ kind: 'unsupported' })
    }
  })

  it('safeLinkHref keeps http(s) and mailto links, relative ones included, and nothing else', () => {
    expect(safeLinkHref('https://example.com/a', PAGE)).toBe('https://example.com/a')
    expect(safeLinkHref('mailto:team@example.com', PAGE)).toBe('mailto:team@example.com')
    expect(safeLinkHref('/runs/1', PAGE)).toBe('/runs/1')
    for (const href of ['javascript:alert(1)', ' JavaScript:alert(1)', 'data:text/html,x', 'vbscript:x', 'file:///etc', '', undefined, 3]) {
      expect(safeLinkHref(href, PAGE)).toBeUndefined()
    }
  })

  it('uses the current page as the origin when none is given', () => {
    expect(classifyImageSource('/assets/a.png').kind).toBe('inline')
    expect(classifyImageSource(`${window.location.origin}/assets/a.png`).kind).toBe('inline')
    expect(classifyImageSource('https://images.example.org/a.png').kind).toBe('external')
  })
})
