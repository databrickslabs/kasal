import ReactMarkdown, { type Components } from 'react-markdown';
import remarkGfm from 'remark-gfm';
import { MarkdownImage } from '../../../../shared/a2ui/lib/safeImage';

// An eval's deliverable in the Optimize dialog: links open in a new tab, and a
// remote image waits for a click (shared image policy).
const COMPONENTS: Components = {
  a: ({ node: _node, ...props }) => <a {...props} target="_blank" rel="noreferrer" />,
  img: MarkdownImage,
};

export default function DeliverableMarkdown({ content }: { content: string }) {
  return (
    <ReactMarkdown remarkPlugins={[remarkGfm]} components={COMPONENTS}>
      {content}
    </ReactMarkdown>
  );
}
