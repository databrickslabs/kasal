import React from 'react';
import ReactMarkdown from 'react-markdown';
import remarkGfm from 'remark-gfm';
import { containsMarkdown } from '../../utils/markdown';
import { hasDiagram, splitDiagramSegments } from '../../utils/mdSandboxDiagram';
import { isDeck, mergeDeckSegments } from '../../utils/htmlDeck';
import HtmlDiagramBlock from './HtmlDiagramBlock';
import HtmlDeckBlock from './HtmlDeckBlock';
import { hasSkillBlock, splitSkillSegments } from '../../utils/skillBlock';
import SkillCard from './SkillCard';

interface MessageContentProps {
  content: string;
  /** True while the MESSAGE is still streaming. An unclosed fence only means
   *  "building" while this is true — after the message ends, an unclosed
   *  fence is a TRUNCATED deliverable and must finalize, not build forever. */
  streaming?: boolean;
  /** The message this content belongs to — a deck's studio writes edits back to it. */
  messageId?: string;
}

// Render a plain text/markdown run the way this component always has.
function renderText(content: string, key?: React.Key) {
  if (containsMarkdown(content)) {
    return (
      <div
        key={key}
        className="prose prose-sm dark:prose-invert max-w-none prose-p:my-1 prose-ul:my-1 prose-ol:my-1 prose-li:my-0.5 prose-headings:my-2 prose-pre:my-2"
      >
        <ReactMarkdown remarkPlugins={[remarkGfm]}>{content}</ReactMarkdown>
      </div>
    );
  }
  return (
    <p key={key} className="whitespace-pre-wrap">
      {content}
    </p>
  );
}

// Text/diagram rendering for one run of content (everything except skill
// cards, which are split out first).
function renderRich(content: string, streaming: boolean, messageId?: string) {
  // A ```html / ```svg block is rendered as a live diagram (sandboxed iframe)
  // instead of a code block, and can be copied as a Databricks %md-sandbox cell.
  // An unclosed fence (streaming) renders a live "building" preview.
  // Consecutive deck fences separated only by whitespace/`---` are ONE deck
  // — models intermittently emit a fence per slide (see mergeDeckSegments).
  const segments = mergeDeckSegments(splitDiagramSegments(content));
  if (hasDiagram(segments)) {
    return (
      <>
        {segments.map((seg, i) => {
          if (seg.type === 'diagram') {
            // A deck (```html with <section class="slide">) renders as a paged
            // slide deck; any other html/svg renders as a single diagram.
            const building = !seg.closed && streaming;
            return isDeck(seg.code) ? (
              <HtmlDeckBlock
                key={i}
                code={seg.code}
                streaming={building}
                truncated={!seg.closed && !streaming}
                messageId={messageId}
              />
            ) : (
              <HtmlDiagramBlock key={i} code={seg.code} streaming={building} />
            );
          }
          return seg.text.trim() ? renderText(seg.text, i) : null;
        })}
      </>
    );
  }
  return renderText(content);
}

// Memoized on the content string: the markdown detection (10 regexes) + full
// ReactMarkdown parse used to re-run for every message on every render tick.
const MessageContent: React.FC<MessageContentProps> = React.memo(
  ({ content, streaming = false, messageId }) => {
    // A ```skill block (a SKILL.md draft) renders as a card with a Save
    // button. Split on those FIRST; each text run between them still gets the
    // diagram / deck treatment.
    const skillSegments = splitSkillSegments(content);
    if (hasSkillBlock(skillSegments)) {
      return (
        <>
          {skillSegments.map((seg, i) =>
            seg.type === 'skill' ? (
              <SkillCard
                key={`skill-${i}`}
                code={seg.code}
                streaming={!seg.closed && streaming}
                truncated={!seg.closed && !streaming}
              />
            ) : seg.text.trim() ? (
              <React.Fragment key={`text-${i}`}>{renderRich(seg.text, streaming, messageId)}</React.Fragment>
            ) : null,
          )}
        </>
      );
    }
    return renderRich(content, streaming, messageId);
  },
);
MessageContent.displayName = 'MessageContent';

export default MessageContent;
