// The policy a crew's HTML document gets in the approval dialog's preview
// frame: it may load nothing from the network, only inline images, styles and
// fonts, the same rule the chat answer follows for images.
export const FRAME_POLICY =
  '<meta http-equiv="Content-Security-Policy" content="default-src \'none\'; ' +
  'img-src data: blob:; style-src \'unsafe-inline\'; font-src data:">';

const LEADING_DOCTYPE = /^\s*<!doctype[^>]*>/i;

/**
 * `html` with FRAME_POLICY ahead of everything in it that could fetch.
 *
 * A meta policy only counts inside <head> and only for what comes after it, so
 * it goes at the very start: right after a leading doctype (keeping standards
 * mode), else at position 0. The parser then opens <head> for it, wherever (or
 * whether) the document has its own head.
 */
export function withFramePolicy(html: string): string {
  const doctype = LEADING_DOCTYPE.exec(html);
  const at = doctype ? doctype[0].length : 0;
  return html.slice(0, at) + FRAME_POLICY + html.slice(at);
}
