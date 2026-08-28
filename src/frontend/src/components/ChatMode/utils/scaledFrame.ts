/**
 * Shared plumbing for rendering agent-authored HTML inside a sandboxed iframe
 * that scales to fit the chat column and reports its own height.
 *
 * Used by both the diagram card (HtmlDiagramBlock) and the slide deck
 * (HtmlDeckBlock). The iframe is sandboxed WITHOUT `allow-same-origin` (the
 * content can't reach the app) and carries a CSP that blocks network egress
 * (`connect-src 'none'`).
 *
 * Content is laid out at a DEFINITE canvas width (#kft has an explicit width)
 * inside #ksz (a sizer). A definite width is essential: a shrink-to-fit
 * (inline-block) parent collapses content that uses width:100%/flex/margin:auto
 * down to its minimum, squeezing a wide diagram into a narrow column. We then
 * measure the laid-out size, scale #kft with a transform to fit the frame width
 * (never upscaling past 1×), and set #ksz to the EXACT scaled size so the
 * reported height always matches what's visible.
 */
import { useEffect, useId, useState } from 'react';

export const BODY_PAD = 12;
// Default canvas width diagrams are laid out at (the authored target ~1200px).
// Content narrower than the column renders at its own size (no upscaling).
export const DEFAULT_CANVAS_W = 1200;

// Matches the app's existing HTML-preview CSP (see Jobs/ShowResult): inline
// styles/scripts allowed for self-contained content; no network egress, no
// remote images beyond data:/blob:.
const CSP =
  "default-src 'self' 'unsafe-inline' 'unsafe-eval'; img-src data: blob:; " +
  "connect-src 'none'; base-uri 'none'; form-action 'none';";

// Height/height-message protocol shared with useScaledFrameHeight below.
const HEIGHT_MSG = 'kasal-frame-height';

export interface FrameOpts {
  /** Definite width the content is laid out at before scaling. */
  baseWidth?: number;
  /**
   * When true (default), the canvas GROWS to the available column width so a
   * responsive diagram uses all the space on a wide screen (base is the floor).
   * When false, the canvas stays fixed at baseWidth (decks: a fixed 16:9 stage
   * must not stretch), only ever scaled DOWN to fit.
   */
  fill?: boolean;
  /**
   * Allow scaling UP past 1× to fill the frame (default false). Used for the
   * fullscreen deck view so a fixed-width slide enlarges to the modal.
   */
  upscale?: boolean;
  /**
   * Fit BOTH width and height and center the content (default false), so a
   * fullscreen view shows everything with no scrolling. The frame must be given
   * a definite height by its container (the iframe fills it).
   */
  contain?: boolean;
}

// Cap on how much we enlarge content to fill the column (avoids blowing up a
// genuinely small diagram).
const MAX_UPSCALE = 2;

// The fit routine, injected into the iframe. It lays the content out, measures
// its REAL width (from the element children's bounding boxes — which respects a
// diagram's own max-width, captures overflow, and ignores empty canvas), shrinks
// #kft to exactly that width so there's no side gap, then scales it to the frame
// width and sizes #ksz to the scaled box so the reported height matches what's
// visible. `fill` diagrams enlarge (capped) to use the full column; decks stay
// fit-to-width (no upscale unless `upscale`).
function heightScript(
  id: string,
  baseWidth: number,
  fill: boolean,
  upscale: boolean,
  contain: boolean,
): string {
  return (
    '<' + 'script>(function(){var ID=' + JSON.stringify(id) + ';var PAD=' + BODY_PAD + ';' +
    'var BASE=' + baseWidth + ';var FILL=' + (fill ? 'true' : 'false') +
    ';var UP=' + (upscale ? 'true' : 'false') + ';var CONTAIN=' + (contain ? 'true' : 'false') +
    ';var CAP=' + MAX_UPSCALE + ';' +
    'function contentWidth(ft){var k=ft.children,L=Infinity,R=-Infinity,i,r;' +
    'for(i=0;i<k.length;i++){r=k[i].getBoundingClientRect();if(r.width||r.height){' +
    'L=Math.min(L,r.left);R=Math.max(R,r.right);}}' +
    'return R>L?R-L:ft.scrollWidth;}' +
    'function fit(){var ft=document.getElementById("kft"),sz=document.getElementById("ksz");' +
    'if(!ft||!sz)return;' +
    'var d=document.documentElement;var avail=d.clientWidth-PAD*2;' +
    'ft.style.transform="none";' +
    // Pass 1: lay out at the column width (fill/contain) or fixed base (deck),
    // then measure the content's real width.
    'var layoutW=(FILL||CONTAIN)?avail:BASE;ft.style.width=layoutW+"px";ft.style.maxWidth=layoutW+"px";' +
    'var cw=contentWidth(ft);' +
    // Pass 2: shrink #kft to the content width so there is no empty side gap.
    'if(cw>0&&isFinite(cw)){ft.style.width=cw+"px";ft.style.maxWidth=cw+"px";}' +
    'var w=ft.scrollWidth,h=ft.scrollHeight;var s;' +
    'if(CONTAIN){var availH=d.clientHeight-PAD*2;' +
    'var sw=(w>0)?avail/w:1,sh=(h>0)?availH/h:1;s=Math.min(sw,sh);if(s>CAP)s=CAP;}' +
    'else{s=(w>0&&avail>0)?avail/w:1;var allowUp=UP||FILL;if(!allowUp&&s>1)s=1;if(s>CAP)s=CAP;}' +
    'ft.style.transform="scale("+s+")";' +
    'sz.style.width=(w*s)+"px";sz.style.height=(h*s)+"px";' +
    // In contain mode the frame is a fixed size set by its container, so we do
    // not report a height (that would drive an auto-height frame).
    'if(!CONTAIN){parent.postMessage({t:' + JSON.stringify(HEIGHT_MSG) +
    ',id:ID,h:Math.ceil(h*s)+PAD*2},"*");}}' +
    'window.addEventListener("load",fit);' +
    'window.addEventListener("resize",fit);' +
    'setTimeout(fit,50);setTimeout(fit,300);setTimeout(fit,800);' +
    '})();<' + '/script>'
  );
}

/**
 * Build the full sandboxed iframe document for a piece of self-contained HTML.
 * Diagrams fill the column (canvas grows to available width, floored at
 * baseWidth); decks pass `{ fill: false }` to keep a fixed slide width.
 */
export function iframeDoc(html: string, id: string, opts: FrameOpts = {}): string {
  const baseWidth = opts.baseWidth ?? DEFAULT_CANVAS_W;
  const fill = opts.fill ?? true;
  const upscale = opts.upscale ?? false;
  const contain = opts.contain ?? false;
  // In contain mode the body fills the frame and centers the content (no scroll);
  // otherwise it flows from the top and the frame auto-heights to the content.
  const bodyStyle = contain
    ? 'html,body{height:100%;}body{padding:' + BODY_PAD +
      'px;display:flex;align-items:center;justify-content:center;overflow:hidden;}'
    : 'body{padding:' + BODY_PAD + 'px;}';
  return (
    '<!doctype html><html><head><meta charset="utf-8">' +
    '<meta http-equiv="Content-Security-Policy" content="' + CSP + '">' +
    '<style>html,body{margin:0;background:#fff;' +
    'font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,sans-serif;}' +
    bodyStyle + '#ksz{overflow:hidden;}#kft{transform-origin:top left;}</style>' +
    '</head><body><div id="ksz"><div id="kft">' + html + '</div></div>' +
    heightScript(id, baseWidth, fill, upscale, contain) + '</body></html>'
  );
}

/** Unique frame id + auto-updated height driven by the frame's height messages. */
export function useScaledFrameHeight(initial = 120): { frameId: string; height: number } {
  const frameId = useId();
  const [height, setHeight] = useState(initial);
  useEffect(() => {
    const onMessage = (e: MessageEvent) => {
      const d = e.data as { t?: string; id?: string; h?: number } | null;
      if (!d || d.t !== HEIGHT_MSG || d.id !== frameId) return;
      if (typeof d.h === 'number' && d.h > 0) setHeight(d.h + 8);
    };
    window.addEventListener('message', onMessage);
    return () => window.removeEventListener('message', onMessage);
  }, [frameId]);
  return { frameId, height };
}
