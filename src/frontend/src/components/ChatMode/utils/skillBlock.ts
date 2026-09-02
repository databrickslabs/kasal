/**
 * A ```skill fenced block is a SKILL.md draft the assistant proposes; the chat
 * renders it as a card with a "Save to workspace" button (SkillCard). This
 * module owns the two pure halves: splitting streamed content on those fences
 * (same conventions as the diagram splitter, including an UNCLOSED trailing
 * fence while the draft streams) and parsing the SKILL.md front-matter.
 */

export type SkillSegment =
  | { type: 'text'; text: string }
  | { type: 'skill'; code: string; closed: boolean };

// ```skill (optionally with trailing spaces) followed by a newline.
const OPEN_FENCE = /```skill[ \t]*\r?\n/i;
// A closing ``` fence sitting on its own line.
const CLOSE_FENCE = /\r?\n[ \t]*```[ \t]*(?:\r?\n|$)/;

/** Split assistant content into ordered text + skill segments. */
export function splitSkillSegments(content: string): SkillSegment[] {
  const segments: SkillSegment[] = [];
  let rest = content;
  for (let guard = 0; rest && guard < 1000; guard++) {
    const open = rest.match(OPEN_FENCE);
    if (!open || open.index === undefined) {
      segments.push({ type: 'text', text: rest });
      break;
    }
    const before = rest.slice(0, open.index);
    if (before) segments.push({ type: 'text', text: before });
    const afterOpen = rest.slice(open.index + open[0].length);
    const close = afterOpen.match(CLOSE_FENCE);
    if (!close || close.index === undefined) {
      segments.push({ type: 'skill', code: afterOpen, closed: false });
      break;
    }
    segments.push({ type: 'skill', code: afterOpen.slice(0, close.index), closed: true });
    rest = afterOpen.slice(close.index + close[0].length);
  }
  return segments;
}

export function hasSkillBlock(segments: SkillSegment[]): boolean {
  return segments.some((s) => s.type === 'skill');
}

export interface SkillDraft {
  name: string;
  description: string;
  body: string;
  license?: string | null;
  compatibility?: string | null;
}

const FRONT_MATTER = /^\s*---\r?\n([\s\S]*?)\r?\n---[ \t]*(?:\r?\n|$)/;

function unquote(value: string): string {
  const v = value.trim();
  if (v.length >= 2 && ((v.startsWith('"') && v.endsWith('"')) || (v.startsWith("'") && v.endsWith("'")))) {
    return v.slice(1, -1).replace(/\\"/g, '"');
  }
  return v;
}

/**
 * Parse a SKILL.md draft: `name`/`description` (and optional license /
 * compatibility) from the YAML front-matter, everything after it as the body.
 * Tolerant — a draft still streaming may have no closing `---` yet, in which
 * case everything is body and the name is empty (the card shows "Drafting…").
 */
export function parseSkillMarkdown(md: string): SkillDraft {
  const m = md.match(FRONT_MATTER);
  if (!m) return { name: '', description: '', body: md };
  const fields: Record<string, string> = {};
  for (const line of m[1].split(/\r?\n/)) {
    const idx = line.indexOf(':');
    if (idx <= 0) continue;
    fields[line.slice(0, idx).trim().toLowerCase()] = unquote(line.slice(idx + 1));
  }
  return {
    name: fields.name || '',
    description: fields.description || '',
    body: md.slice(m[0].length).replace(/^\r?\n/, ''),
    license: fields.license || null,
    compatibility: fields.compatibility || null,
  };
}
