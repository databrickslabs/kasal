import React, { useMemo } from 'react';
import { iframeDoc, useScaledFrameHeight } from '../../utils/scaledFrame';

/**
 * A sandboxed, scale-to-fit iframe for agent-authored HTML. Manages its own
 * frame id + auto-height (see utils/scaledFrame). Used by the diagram card, the
 * slide deck, and their fullscreen views.
 */
interface ScaledFrameProps {
  html: string;
  /** Layout width before scaling (diagrams: column-fill; decks: slide width). */
  baseWidth?: number;
  /** Grow the canvas to the column width (diagrams) vs. fixed width (decks). */
  fill?: boolean;
  /** Allow scaling up past 1× (fullscreen deck). */
  upscale?: boolean;
  /** Fit both width and height, centered — fills the parent's height (fullscreen). */
  contain?: boolean;
  /** True while the html is still streaming in: the frame's auto-height then
   *  only GROWS (partial layouts measure erratically; shrinking per chunk is
   *  what shook the chat column). */
  streaming?: boolean;
  /** Canvas colour behind the content (default white). */
  background?: string;
  /** Padding around the content in px (default BODY_PAD). */
  pad?: number;
  title?: string;
}

const ScaledFrame: React.FC<ScaledFrameProps> = ({
  html,
  baseWidth,
  fill,
  upscale,
  contain,
  streaming = false,
  background = '#ffffff',
  pad,
  title = 'Preview',
}) => {
  const { frameId, height } = useScaledFrameHeight(120, streaming);
  const srcDoc = useMemo(
    () => iframeDoc(html, frameId, { baseWidth, fill, upscale, contain, background, pad }),
    [html, frameId, baseWidth, fill, upscale, contain, background, pad],
  );
  return (
    <iframe
      title={title}
      sandbox="allow-scripts"
      srcDoc={srcDoc}
      style={{
        display: 'block',
        width: '100%',
        border: 0,
        background,
        // Contain mode fills its container's height; otherwise auto-heights.
        height: contain ? '100%' : height,
      }}
    />
  );
};

export default ScaledFrame;
