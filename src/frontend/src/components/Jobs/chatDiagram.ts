import { hasDiagram, splitDiagramSegments } from '../ChatMode/utils/mdSandboxDiagram';
import { isDeck, mergeDeckSegments } from '../ChatMode/utils/htmlDeck';

/** Whether the string carries ```html / ```svg fences (what the chat's
 *  MessageContent renders live). */
export function hasFencedDiagram(text: string): boolean {
  return hasDiagram(mergeDeckSegments(splitDiagramSegments(text)));
}

/** Whether a result string carries what the chat renders live: a ```html /
 *  ```svg fence (a single diagram or a deck, possibly amid markdown prose) —
 *  or the deck HTML itself. A crew deliverable is often the raw
 *  `<section class="slide">` markup with no fence around it; a full standalone
 *  document (`<!DOCTYPE>`/`<html>`) is NOT treated as a deck — it keeps the
 *  sandboxed full-page renderer. */
export function hasChatDiagram(text: string): boolean {
  if (hasFencedDiagram(text)) return true;
  return isDeck(text) && !/<!DOCTYPE|<html[\s>]/i.test(text);
}
