/**
 * `FitBox` — shrink-to-fit for slide body regions.
 *
 * A slide is a FIXED 1280×720 canvas (see `SlideStage`), so a body region has a
 * hard height budget and the content either fits or it does not. The two failure
 * modes this replaces were both wrong for a deck:
 *
 *   - `overflow-auto` → a scrollbar. There is no scrollbar in a PDF or in
 *     PowerPoint, so whatever is below the fold is simply GONE from the artefact
 *     the reader gets, while looking fine on screen.
 *   - overflow visible → the body runs past its region and over the
 *     absolutely-positioned sources footer, which is the text-on-text overlap.
 *
 * So instead: measure, and if the content is too tall, scale it down until it
 * fits. Scaling (rather than clipping or scrolling) keeps every word on the slide,
 * which is what a deck promises. `width: 100/k %` compensates the transform so the
 * content reflows at the wider measure FIRST and then shrinks — text therefore
 * uses the full column instead of being squeezed into a narrow strip.
 *
 * Below `MIN_SCALE` the slide is not overfull, it is overloaded: shrinking further
 * produces text nobody can read at slide distance, so it clips and the fix belongs
 * in the content (fewer bullets), not here.
 */
import { useLayoutEffect, useRef, useState } from 'react'
import type { CSSProperties, ReactNode } from 'react'

/** Floor on the shrink factor — past this, text is unreadable at slide distance. */
const MIN_SCALE = 0.6
/** Sub-pixel slack, so a 0.3px rounding difference does not trigger a shrink. */
const SLACK = 1

export function FitBox({
  children,
  className,
  outerClassName,
  style,
}: {
  children: ReactNode
  /** Classes for the CONTENT element (the one that gets scaled). */
  className?: string
  /** Classes for the clipping box that owns the height budget. */
  outerClassName?: string
  style?: CSSProperties
}) {
  const outer = useRef<HTMLDivElement>(null)
  const inner = useRef<HTMLDivElement>(null)
  const [scale, setScale] = useState(1)

  // No dependency array: the measurement must re-run whenever the rendered
  // children change, and a slide's children are ids resolved by a parent, so
  // there is nothing cheap to compare. It converges rather than looping —
  // measuring RESETS the transform first, so a second pass on the same content
  // computes the same factor and `setScale` bails on the equality check.
  useLayoutEffect(() => {
    const box = outer.current
    const el = inner.current
    if (!box || !el) return

    const fit = () => {
      const availH = box.clientHeight
      const availW = box.clientWidth
      if (!availH || !availW) return
      // Measure unscaled and at natural height: `height: 100/k %` on the committed
      // style would make scrollHeight equal clientHeight and hide the overflow.
      el.style.transform = 'none'
      el.style.width = `${availW}px`
      el.style.height = 'auto'

      let k = 1
      for (let pass = 0; pass < 8; pass++) {
        el.style.width = `${availW / k}px`
        const contentH = el.scrollHeight * k
        if (contentH <= availH + SLACK) break
        // Damped step: the reflow at a wider measure usually recovers some height,
        // so taking the full ratio at once overshoots and the text ends up smaller
        // than it needs to be. Converges in 2-3 passes in practice.
        k *= Math.max(0.85, Math.sqrt(availH / contentH))
        if (k <= MIN_SCALE) {
          k = MIN_SCALE
          break
        }
      }
      // Hand the measured value back to React and let the render below own the
      // style, so the DOM we wrote to during measurement is not left behind.
      el.style.transform = ''
      el.style.width = ''
      el.style.height = ''
      setScale((prev) => (Math.abs(prev - k) < 0.005 ? prev : k))
    }

    fit()
    if (typeof ResizeObserver === 'undefined') return
    // The canvas is fixed, but a late webfont or image load changes the height the
    // content needs after the first paint.
    const ro = new ResizeObserver(fit)
    ro.observe(box)
    return () => ro.disconnect()
  })

  return (
    <div ref={outer} className={outerClassName ?? 'relative min-h-0 min-w-0 flex-1 overflow-hidden'}>
      <div
        ref={inner}
        className={className}
        style={{
          ...style,
          // Height restored to the full budget (pre-scale) so the content's own
          // `justify-center` still centres it vertically in the region.
          height: `${100 / scale}%`,
          width: `${100 / scale}%`,
          transform: scale === 1 ? undefined : `scale(${scale})`,
          transformOrigin: 'top left',
        }}
      >
        {children}
      </div>
    </div>
  )
}
