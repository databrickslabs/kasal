import { hasDiagram, splitDiagramSegments } from '../ChatMode/utils/mdSandboxDiagram';
import { mergeDeckSegments } from '../ChatMode/utils/htmlDeck';

/** Whether a result string carries what the chat renders live: a ```html /
 *  ```svg fence (a single diagram) or a `<section class="slide">` deck —
 *  possibly in the middle of markdown prose. */
export function hasChatDiagram(text: string): boolean {
  return hasDiagram(mergeDeckSegments(splitDiagramSegments(text)));
}
