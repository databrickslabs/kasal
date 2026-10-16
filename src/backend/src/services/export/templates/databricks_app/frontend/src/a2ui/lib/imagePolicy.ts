// Which images may load the moment they render.
//
// A browser requests an <img> as soon as it is in the page, so an image URL in
// content a model wrote (a chat answer, markdown, an A2UI Image or Album) would
// reach its host before anyone looked at it. Only images that need no request to
// another host load directly: data:, blob: and same-origin URLs. Any other
// http(s) image is shown as a placeholder (safeImage.tsx) whose "Load image" link
// opens it in a new tab. In Kasal the page's Content-Security-Policy img-src
// allows the same set as a backstop; dev (Vite) and the exported app send no page
// CSP, so there this policy is the only guard.

export type ImageSource =
  | { kind: 'inline'; src: string }
  | { kind: 'external'; url: string; host: string }
  | { kind: 'unsupported' }

const INLINE_SCHEME = /^(data|blob):/i

function currentPage(): string | undefined {
  return typeof window === 'undefined' ? undefined : window.location.href
}

export function classifyImageSource(src: unknown, pageUrl: string | undefined = currentPage()): ImageSource {
  if (typeof src !== 'string' || !src.trim()) return { kind: 'unsupported' }
  const value = src.trim()
  if (INLINE_SCHEME.test(value)) return { kind: 'inline', src: value }
  let url: URL
  try {
    url = new URL(value, pageUrl)
  } catch {
    return { kind: 'unsupported' }
  }
  if (pageUrl && url.origin === new URL(pageUrl).origin) return { kind: 'inline', src: value }
  if (url.protocol === 'http:' || url.protocol === 'https:') return { kind: 'external', url: url.href, host: url.host }
  return { kind: 'unsupported' }
}

const LINK_SCHEMES = new Set(['http:', 'https:', 'mailto:'])

/** A link target model content may carry (http, https or mailto), else undefined. */
export function safeLinkHref(href: unknown, pageUrl: string | undefined = currentPage()): string | undefined {
  if (typeof href !== 'string' || !href.trim()) return undefined
  try {
    return LINK_SCHEMES.has(new URL(href.trim(), pageUrl).protocol) ? href.trim() : undefined
  } catch {
    return undefined
  }
}
