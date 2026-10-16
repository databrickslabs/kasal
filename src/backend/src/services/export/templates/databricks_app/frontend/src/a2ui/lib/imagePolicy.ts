// Which images may load the moment they render.
//
// A browser requests an <img> as soon as it is in the page, so an image URL in
// content a model wrote (a chat answer, markdown, an A2UI Image or Album) would
// reach its host before anyone looked at it. Only images that need no request to
// another host load directly: data:, blob: and same-origin URLs. Any other
// http(s) image is shown as a placeholder (safeImage.tsx) whose "Load image" link
// opens it in a new tab. The page's Content-Security-Policy img-src allows the
// same set, so the click opens a tab rather than loading the image in the page.

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
