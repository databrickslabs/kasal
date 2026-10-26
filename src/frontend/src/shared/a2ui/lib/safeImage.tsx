import type { CSSProperties } from 'react'
import { ImageIcon } from 'lucide-react'
import { classifyImageSource } from './imagePolicy'

// The one way rendered content shows an image (see imagePolicy.ts). Styled
// inline rather than with Tailwind so the placeholder reads the same on the MUI
// pages, where Tailwind does not apply.
const placeholderStyle: CSSProperties = {
  display: 'inline-flex',
  alignItems: 'center',
  gap: 6,
  maxWidth: '100%',
  padding: '2px 8px',
  border: '1px dashed currentColor',
  borderRadius: 6,
  fontSize: '0.85em',
  lineHeight: 1.6,
  verticalAlign: 'middle',
}
const hostStyle: CSSProperties = { opacity: 0.75, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }
const loadStyle: CSSProperties = { whiteSpace: 'nowrap', textDecoration: 'underline' }

export interface SafeImageProps {
  src?: unknown
  alt?: string
  title?: string
  className?: string
}

export function SafeImage({ src, alt, title, className }: SafeImageProps) {
  const source = classifyImageSource(src)
  if (source.kind === 'inline') return <img src={source.src} alt={alt ?? ''} title={title} className={className} />
  if (source.kind === 'unsupported') return alt ? <span>{alt}</span> : null
  return (
    <span style={placeholderStyle} title={source.url} data-image-placeholder="">
      <ImageIcon aria-hidden="true" size={14} style={{ flexShrink: 0 }} />
      <span style={hostStyle}>{alt ? `${alt} · ${source.host}` : source.host}</span>
      <a href={source.url} target="_blank" rel="noopener noreferrer" style={loadStyle}>
        Load image
      </a>
    </span>
  )
}

// ReactMarkdown `img` override. Forwards only src/alt/title: a srcset or style
// attribute (raw HTML) could name another image and load it anyway.
export function MarkdownImage({ src, alt, title }: { src?: string; alt?: string; title?: string }) {
  return <SafeImage src={src} alt={alt} title={title} />
}
