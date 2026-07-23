/**
 * Builds renderable A2UI preview surfaces from RAW run-activity text — the
 * tool/memory/logs output a step produces. It normalizes that messy input
 * (Python object reprs, JSON tool envelopes, Genie/SQL statement responses,
 * search-result record arrays, "===" banners and "•"/"-" outlines) into the
 * SAME {@link UiSurface} shape final deliverables use, so the run-activity feed
 * renders through the one shared renderer instead of ad-hoc markup.
 *
 * Public entry points: {@link buildResultsSurface} (the full surface),
 * {@link contextSummary} (a clamped plain-text preview line),
 * {@link humanizeToolJson} and {@link cleanContextText} (text normalizers).
 */
import type { UiSurface, UiComponent } from '../../utils/surfaceAdapter';

/** Strip the markdown emphasis markers the A2UI Text renderer shows verbatim. */
function stripEmphasis(text: string): string {
  return text.replace(/\*\*/g, '').replace(/__/g, '').replace(/`/g, '').trim();
}

/**
 * Decode JSON/JS string escape sequences (\uXXXX, \n, \t, \r, \", \\, \/) that
 * leak when a tool's output is a JSON envelope the renderer couldn't parse —
 * most often because the backend CLAMPED it ("…[truncated]") mid-string, leaving
 * invalid JSON that JSON.parse rejects. Without this the trace shows raw
 * "نفس" / literal "\n" instead of readable text. One pass, so an
 * escaped backslash (\\) consumes both chars and never re-triggers the next
 * escape. A no-op when there are no escape sequences (plain prose passes through).
 */
function decodeEscapes(text: string): string {
  if (!text || !/\\(u[0-9a-fA-F]{4}|["\\/nrtbf])/.test(text)) return text;
  const simple: Record<string, string> = { n: '\n', r: '\r', t: '\t', b: '\b', f: '\f', '"': '"', '\\': '\\', '/': '/' };
  return text.replace(/\\(u[0-9a-fA-F]{4}|["\\/nrtbf])/g, (m, esc: string) =>
    esc[0] === 'u' ? String.fromCodePoint(parseInt(esc.slice(1), 16)) : (simple[esc] ?? m),
  );
}

/**
 * Best-effort recovery of the prose from a JSON envelope the strict parser
 * rejected — a {"<key>": "<text…"} the backend CLAMPED mid-string (its trace cap
 * appends "…[truncated]"). Strips the {"key": " wrapper, the truncation marker and
 * any dangling escape / closing brace, returning the (still-escaped) inner text
 * for {@link cleanContextText} to decode. Returns null when the body isn't a
 * single-string envelope (arrays / object-valued first keys keep the generic
 * path), so it only ever salvages what would otherwise leak as a raw blob.
 */
function salvageEnvelopeProse(raw: string): string | null {
  const t = raw.trim();
  const m = t.match(/^\{\s*"[^"]+"\s*:\s*"/);
  if (!m) return null;
  return t
    .slice(m[0].length)
    .replace(/…?\s*\[truncated\]\s*$/i, '')
    .replace(/"\s*\}?\s*$/, '')
    .replace(/\\+$/, '');
}

/** A short line that reads as a section header (an `=== … ===` banner, an
 *  ALL-CAPS banner, or a brief "Label:" lead-in) — returns the cleaned heading
 *  text, or null for ordinary prose. */
function headingText(line: string): string | null {
  const banner = line.match(/^=+\s*(.+?)\s*=+$/);
  if (banner) return banner[1].trim();
  const hasLetters = /[A-Za-z]/.test(line);
  if (hasLetters && line.length <= 70 && line === line.toUpperCase()) {
    return line.replace(/[:：]\s*$/, '').trim();
  }
  if (line.length <= 48 && /[:：]$/.test(line) && !/[.!?]/.test(line)) {
    return line.replace(/[:：]\s*$/, '').trim();
  }
  return null;
}

/**
 * Turn a free-text body — which may be a structured outline (headings, bullet
 * lists, key/value lines) — into a sequence of A2UI components: headings become
 * Text h4, runs of bullet/numbered lines become a List, everything else a body
 * paragraph. This is what makes verbose tool/memory output read as a clean
 * document instead of a wall of "===" banners and "•"/"-" markers. Returns the
 * new components plus the child ids to attach, all keyed under `prefix`.
 */
// Compact, elegant type scale for the preview's result cards — smaller than the
// renderer's deck-grade defaults. fontSize / lineHeight / fontWeight /
// letterSpacing all pass through extractNodeStyle's whitelist, so these refine
// the look without touching the shared renderer.
const TITLE_STYLE = { fontSize: '1.05rem', fontWeight: 700, letterSpacing: '0.01em' };
const HEAD_STYLE = { fontSize: '0.9rem', fontWeight: 600, letterSpacing: '0.03em' };
const BODY_STYLE = { fontSize: '0.85rem', lineHeight: 1.5 };

/** Drop a metadata line whose value is explicitly empty — "entities: []",
 *  "dates: {}", "result: none". A bare "Label:" is NOT noise (it's a section
 *  heading like "Suggested questions:" / "TITLE BLOCK:"), so the empty-value
 *  marker is required, not optional. */
function isNoiseLine(line: string): boolean {
  return /^[A-Za-z][\w &/()'-]*:\s*(\[\s*\]|\{\s*\}|none|n\/?a|null)\s*$/i.test(line);
}

// Provenance / framing the user does NOT want in the preview — only the
// retrieved CONTEXT itself. Drops "categories:/entities:/dates:/topics:/tags:/
// source(s):/score:" metadata lines and "Relevant/Found memories", "Search
// memory" framing headers.
const DROP_LABEL = /^(categories|category|entities|entity|dates|date|topics|topic|tags|tag|source|sources|score|url|uri|link|links|markdown|page age|thumbnail|thumbnail url|image|image url|metadata|scope|importance|id)\s*:/i;
const DROP_HEADER = /^(relevant memories|found memories|search memory|no memories|memories found)\b/i;

/** A "Label: • a • b • c" (or bare "• a • b • c") inline run → label + items. */
function splitInlineBullets(line: string): { label: string; items: string[] } | null {
  if (!line.includes('•')) return null;
  const idx = line.indexOf('•');
  const label = line.slice(0, idx).trim().replace(/[:：]\s*$/, '');
  const items = line.slice(idx).split('•').map((s) => s.trim()).filter(Boolean);
  if (items.length < 2) return null;
  return { label, items };
}

function bodyToComponents(
  body: string,
  prefix: string,
): { components: Record<string, UiComponent>; childIds: string[] } {
  const components: Record<string, UiComponent> = {};
  const childIds: string[] = [];
  let n = 0;
  let bullets: string[] = [];

  const pushText = (text: string, variant: string, style: Record<string, unknown>) => {
    const id = `${prefix}_${n++}`;
    components[id] = { id, component: 'Text', text, variant, style };
    childIds.push(id);
  };

  const flushBullets = () => {
    if (bullets.length === 0) return;
    const listId = `${prefix}_l${n++}`;
    const itemIds = bullets.map((t, k) => {
      const id = `${listId}_i${k}`;
      components[id] = { id, component: 'Text', text: t, variant: 'body', style: BODY_STYLE };
      return id;
    });
    components[listId] = { id: listId, component: 'List', children: itemIds };
    childIds.push(listId);
    bullets = [];
  };

  for (const raw of stripEmphasis(body).split('\n')) {
    let line = raw.trim();
    // Show ONLY the retrieved context — strip empty noise, provenance metadata
    // and the memory framing headers.
    if (!line || isNoiseLine(line) || DROP_LABEL.test(line) || DROP_HEADER.test(line)) {
      flushBullets();
      continue;
    }

    // A new memory/result starts with a "(score=…)" marker — drop the marker and
    // separate consecutive entries with a hairline divider for an elegant feed.
    const isEntryStart = /^\(score=[\d.]+\)/i.test(line);
    line = line.replace(/^\(score=[\d.]+\)\s*/i, '').trim();
    if (isEntryStart && childIds.length > 0) {
      flushBullets();
      const did = `${prefix}_d${n++}`;
      components[did] = { id: did, component: 'Divider' };
      childIds.push(did);
    }
    if (!line) continue;

    // "Label: • a • b • c" → a labelled bullet list. This is the big readability
    // win: dense outline lines become a heading over clean list items.
    const inline = splitInlineBullets(line);
    if (inline) {
      flushBullets();
      if (inline.label) pushText(inline.label, 'h4', HEAD_STYLE);
      bullets.push(...inline.items);
      flushBullets();
      continue;
    }

    // A leading bullet / number marker → list item.
    const bullet = line.match(/^[-•*]\s+(.*)$/) || line.match(/^\d+[.)]\s+(.*)$/);
    if (bullet) {
      bullets.push(bullet[1].trim());
      continue;
    }

    flushBullets();
    const heading = headingText(line);
    if (heading) pushText(heading, 'h4', HEAD_STYLE);
    else pushText(line, 'body', BODY_STYLE);
  }
  flushBullets();
  return { components, childIds };
}

/** Title-case a JSON key: "suggestedQuestions" / "text_attachments" → "Suggested questions". */
function humanizeKey(k: string): string {
  const s = k.replace(/[_-]+/g, ' ').replace(/([a-z])([A-Z])/g, '$1 $2').trim();
  return s ? s.charAt(0).toUpperCase() + s.slice(1) : k;
}

// JSON keys that are plumbing, not content — dropped from a humanized tool result.
const JSON_DROP_KEYS = new Set([
  'conversationid', 'messageid', 'conversation_id', 'message_id', 'id', 'ids',
  'status', 'queryattachments', 'query_attachments', 'metadata', 'usage', 'role',
]);

/**
 * Humanize a JSON tool result (e.g. a Genie response envelope) into readable
 * text: pull out the prose attachments and suggested questions, render
 * string-array fields as labelled bullet lists, and drop ids / status / empty
 * arrays. Returns null when `raw` isn't a JSON object/array, so prose bodies
 * pass straight through. The text it returns is then formatted by
 * {@link bodyToComponents} (headings / lists / paragraphs) like any other body.
 */
export function humanizeToolJson(raw: string): string | null {
  const t = raw.trim();
  if (!(t.startsWith('{') || t.startsWith('['))) return null;
  let parsed: unknown;
  try {
    parsed = JSON.parse(t);
  } catch {
    return null;
  }
  if (typeof parsed === 'string') {
    try { parsed = JSON.parse(parsed); } catch { /* keep as-is */ }
  }
  if (!parsed || typeof parsed !== 'object') return null;

  const out: string[] = [];
  // Prose fields render as bare paragraphs (the model's actual answer/content);
  // every other scalar keeps its key as a "Label: value" line so structured tool
  // results stay readable instead of leaking a raw blob or dropping their numbers.
  const PROSE_KEY = /attachment|answer|result|content|text|message|summary/i;
  const isScalar = (v: unknown) => typeof v === 'string' || typeof v === 'number' || typeof v === 'boolean';
  const collect = (o: Record<string, unknown>) => {
    for (const [k, v] of Object.entries(o)) {
      if (JSON_DROP_KEYS.has(k.toLowerCase())) continue;
      if (v === null || v === undefined) continue;
      if (typeof v === 'string') {
        const s = v.trim();
        if (!s) continue;
        if (PROSE_KEY.test(k)) out.push(s); // prose → paragraph (keeps own newlines/bullets)
        else out.push(`${humanizeKey(k)}: ${s}`); // structured string → labelled line
      } else if (typeof v === 'number' || typeof v === 'boolean') {
        out.push(`${humanizeKey(k)}: ${v}`); // scalar → labelled line (previously dropped)
      } else if (Array.isArray(v)) {
        const scalars = v.filter(isScalar).map((x) => String(x).trim()).filter(Boolean);
        const objs = v.filter((x) => x && typeof x === 'object' && !Array.isArray(x)) as Record<string, unknown>[];
        if (scalars.length) {
          if (PROSE_KEY.test(k)) {
            scalars.forEach((s) => out.push(s)); // prose array → paragraphs
          } else {
            out.push(`${/question/i.test(k) ? 'Suggested questions' : humanizeKey(k)}:`);
            scalars.forEach((s) => out.push(`- ${s}`)); // → labelled bullet list
          }
        }
        objs.forEach((el) => collect(el)); // array of records → recurse each (was dropped)
      } else if (typeof v === 'object') {
        collect(v as Record<string, unknown>);
      }
    }
  };

  if (Array.isArray(parsed)) {
    const scalars = parsed.filter(isScalar).map((x) => String(x).trim()).filter(Boolean);
    scalars.forEach((s) => out.push(`- ${s}`));
    parsed.forEach((el) => { if (el && typeof el === 'object' && !Array.isArray(el)) collect(el as Record<string, unknown>); });
  } else {
    collect(parsed as Record<string, unknown>);
  }
  return out.length ? out.join('\n') : null;
}

/**
 * Compose a list of `{ title, body }` results into an A2UI surface — a Column of
 * Cards, each a heading (Text h3) over the structured body (headings / lists /
 * paragraphs via {@link bodyToComponents}) — so transient / intermediate results
 * render through the SAME renderer pipeline as final deliverables, instead of
 * ad-hoc markup. Pure; built on demand, nothing here is persisted. Used by
 * ChatMode's live preview feed.
 */
/**
 * Normalize raw tool/memory output into PLAIN, human-readable text for the
 * run-activity context view: unwrap Python object reprs (MemoryMatch/MemoryRecord)
 * down to their `content`, turn markdown links into their text, and drop the
 * plumbing tokens that leak from save/recall confirmations (scope=, importance=,
 * id=, bare URLs) so a non-technical reader sees the substance, not the
 * scaffolding.
 *
 * Deliberately does NOT touch the "(score=…)" entry markers — bodyToComponents
 * uses them to divide consecutive memory entries before stripping them.
 */
export function cleanContextText(raw: string): string {
  if (!raw) return '';
  // 0) Decode leaked JSON escape sequences (\uXXXX, \n, …) FIRST, so escaped
  //    output renders as readable text and the URL strip below operates on real
  //    newlines (its greedy \S+ would otherwise span literal "\n" sequences).
  let t = decodeEscapes(raw);
  // 1) Python object reprs (e.g. [MemoryMatch(record=MemoryRecord(id='…',
  //    content='…', scope='…', …))]) → keep only the human-readable content='…'.
  if (/content=['"]/.test(t) && /\b[A-Z]\w*\(/.test(t)) {
    const strict = [...t.matchAll(/content=(['"])([\s\S]*?)\1\s*,\s*\w+=/g)].map((m) => m[2]);
    const contents = strict.length ? strict : [...t.matchAll(/content=(['"])([\s\S]*?)\1/g)].map((m) => m[2]);
    if (contents.length) t = contents.join('\n\n');
  }
  // 2) Markdown links → their visible text; then drop any remaining bare URLs.
  t = t.replace(/\[([^\]]+)\]\(\s*<?https?:\/\/[^)]*>?\s*\)/g, '$1');
  t = t.replace(/<?https?:\/\/\S+>?/g, '');
  // 3) Strip plumbing tokens that leak from save/recall confirmations.
  t = t
    .replace(/\bscope=(['"])[^'"]*\1/gi, '')
    .replace(/\bscope=\/[^\s,)]*/gi, '')
    .replace(/\bimportance=[\d.]+/gi, '')
    .replace(/\bid=(['"])[^'"]*\1/gi, '');
  // 4) Tidy empty parens/brackets left behind and collapse blank runs.
  t = t.replace(/\(\s*[,;]*\s*\)/g, '').replace(/[ \t]{2,}/g, ' ');
  return t.trim();
}

// Keys that are plumbing/links — never shown as a table column.
const TABLE_NOISE_KEYS = new Set([
  'url', 'uri', 'link', 'links', 'thumbnail_url', 'favicon_url', 'image_url', 'thumbnail', 'favicon', 'image', 'id', 'uuid',
]);
// Preferred column order so the most useful fields lead; the rest follow.
const TABLE_COL_ORDER = ['title', 'name', 'headline', 'description', 'summary', 'snippet', 'query', 'page_age', 'published', 'date'];

/**
 * Find every array-of-records (list of uniform objects) in a JSON tool result,
 * at the top level or one object deep (e.g. results.web / results.news). Returns
 * null when the body isn't JSON or has no such array — those keep the prose path.
 * Operates on the RAW body (not the url-stripped clean text) so the JSON parses.
 */
function recordArraysFromJson(raw: string): { label: string; records: Record<string, unknown>[] }[] | null {
  const t = raw.trim();
  if (!(t.startsWith('{') || t.startsWith('['))) return null;
  let parsed: unknown;
  try { parsed = JSON.parse(t); } catch { return null; }
  const isRecordArray = (v: unknown): v is Record<string, unknown>[] =>
    Array.isArray(v) && v.length > 0 && v.every((x) => x !== null && typeof x === 'object' && !Array.isArray(x));
  const groups: { label: string; records: Record<string, unknown>[] }[] = [];
  if (isRecordArray(parsed)) {
    groups.push({ label: '', records: parsed });
  } else if (parsed && typeof parsed === 'object') {
    for (const [k, v] of Object.entries(parsed as Record<string, unknown>)) {
      if (JSON_DROP_KEYS.has(k.toLowerCase())) continue;
      if (isRecordArray(v)) groups.push({ label: k, records: v });
      else if (v && typeof v === 'object' && !Array.isArray(v)) {
        for (const [k2, v2] of Object.entries(v as Record<string, unknown>)) {
          if (JSON_DROP_KEYS.has(k2.toLowerCase())) continue;
          if (isRecordArray(v2)) groups.push({ label: k2, records: v2 });
        }
      }
    }
  }
  return groups.length ? groups : null;
}

/** Choose readable, scalar table columns (drop links/ids and array fields). */
function tableColumns(records: Record<string, unknown>[]): string[] {
  const present: string[] = [];
  for (const r of records) {
    for (const [k, v] of Object.entries(r)) {
      if (TABLE_NOISE_KEYS.has(k.toLowerCase())) continue;
      if (v === null || v === undefined || typeof v === 'object') continue; // skip arrays/objects (e.g. snippets)
      if (!present.includes(k)) present.push(k);
    }
  }
  present.sort((a, b) => {
    const ia = TABLE_COL_ORDER.indexOf(a.toLowerCase());
    const ib = TABLE_COL_ORDER.indexOf(b.toLowerCase());
    return (ia === -1 ? 99 : ia) - (ib === -1 ? 99 : ib);
  });
  return present.slice(0, 3); // keep it scannable
}

/** A readable cell value: ISO timestamps → a date, long strings clipped. */
function tableCell(v: unknown): string {
  let s = v === null || v === undefined ? '' : String(v);
  s = s.replace(/^(\d{4}-\d{2}-\d{2})T[\d:.]+.*$/, '$1');
  return s.length > 180 ? `${s.slice(0, 180).trim()}…` : s;
}

interface GenieAttachment { description?: string; columns: string[]; rows: Record<string, string>[]; }

/**
 * Shape a Databricks `statement_response` (a SQL result: manifest.schema.columns
 * + result.data_array of {values:[{string_value}]}) into table columns + rows.
 * Shared by the Genie envelope and the raw SQL tool result.
 */
function statementResponseToTable(sr: Record<string, unknown> | undefined): { columns: string[]; rows: Record<string, string>[] } {
  const manifest = sr?.manifest as Record<string, unknown> | undefined;
  const schema = manifest?.schema as Record<string, unknown> | undefined;
  const colDefs = Array.isArray(schema?.columns) ? (schema!.columns as Record<string, unknown>[]) : [];
  const cols = colDefs.map((c) => String(c?.name ?? '')).filter(Boolean);
  const result = sr?.result as Record<string, unknown> | undefined;
  const dataArray = Array.isArray(result?.data_array) ? (result!.data_array as unknown[]) : [];
  const rows = dataArray.map((r) => {
    const vals = Array.isArray((r as Record<string, unknown>)?.values)
      ? ((r as Record<string, unknown>).values as unknown[])
      : (Array.isArray(r) ? (r as unknown[]) : []);
    const row: Record<string, string> = {};
    cols.forEach((c, i) => {
      const v = vals[i];
      const cell = v && typeof v === 'object' ? ((v as Record<string, unknown>).string_value ?? Object.values(v as Record<string, unknown>)[0]) : v;
      row[humanizeKey(c)] = tableCell(cell);
    });
    return row;
  });
  return { columns: cols.map(humanizeKey), rows };
}

/**
 * Parse a Databricks Genie tool envelope into its parts: the QUERY RESULT DATA
 * (the SQL `statement_response.result.data_array` shaped against the manifest
 * schema → a real table — the thing users actually want to see), the prose
 * `textAttachments` (the spoken answer) and the `suggestedQuestions`. Returns
 * null when the body isn't a Genie envelope, so other content keeps its path.
 */
function genieEnvelope(raw: string): { attachments: GenieAttachment[]; answers: string[]; questions: string[] } | null {
  const t = (raw || '').trim();
  if (!t.startsWith('{') || !/queryAttachments|query_attachments/.test(t)) return null;
  let parsed: unknown;
  try { parsed = JSON.parse(t); } catch { return null; }
  const content = (parsed as Record<string, unknown>)?.content as Record<string, unknown> | undefined;
  if (!content || typeof content !== 'object') return null;
  const qa = (content.queryAttachments ?? content.query_attachments);
  if (!Array.isArray(qa)) return null;
  const attachments: GenieAttachment[] = qa.map((a) => {
    const att = (a ?? {}) as Record<string, unknown>;
    const { columns, rows } = statementResponseToTable(att.statement_response as Record<string, unknown> | undefined);
    return { description: typeof att.description === 'string' ? att.description : undefined, columns, rows };
  });
  const strArr = (v: unknown): string[] => (Array.isArray(v) ? v.filter((x): x is string => typeof x === 'string' && x.trim().length > 0) : []);
  return { attachments, answers: strArr(content.textAttachments), questions: strArr(content.suggestedQuestions) };
}

/**
 * A raw Databricks SQL tool result — a `statement_response` (top-level, or under a
 * `statement_response` key) with a manifest + data_array — shaped into a table.
 * Returns null when the body isn't such a result.
 */
function sqlEnvelope(raw: string): { columns: string[]; rows: Record<string, string>[] } | null {
  const t = (raw || '').trim();
  if (!t.startsWith('{') || !/data_array|statement_id/.test(t)) return null;
  let parsed: unknown;
  try { parsed = JSON.parse(t); } catch { return null; }
  const obj = parsed as Record<string, unknown>;
  const sr = (obj?.manifest && obj?.result) ? obj : (obj?.statement_response as Record<string, unknown> | undefined);
  if (!sr) return null;
  const table = statementResponseToTable(sr);
  return table.columns.length && table.rows.length ? table : null;
}

export function buildResultsSurface(items: { title: string; body?: string }[]): UiSurface {
  const components: Record<string, UiComponent> = {};
  const cardIds: string[] = [];

  items.forEach((it, i) => {
    const childIds: string[] = [];
    // The caller can pass an empty title to render the body alone (e.g. the
    // run-activity screens label the step themselves — no redundant heading).
    if (it.title) {
      const titleId = `r_title_${i}`;
      components[titleId] = { id: titleId, component: 'Text', text: stripEmphasis(it.title), variant: 'h3', style: TITLE_STYLE };
      childIds.push(titleId);
    }

    let madeTable = false;

    // A Databricks Genie envelope: show the spoken answer, then the QUERY RESULT
    // DATA as a real table (ticker / metrics / …), then the suggested questions —
    // so the data the user asked for is actually visible, not dropped.
    const genie = genieEnvelope(it.body || '');
    if (genie && (genie.answers.length || genie.attachments.some((a) => a.rows.length))) {
      genie.answers.forEach((ans, ai) => {
        const blocks = bodyToComponents(ans, `r_ga${i}_${ai}`);
        Object.assign(components, blocks.components);
        childIds.push(...blocks.childIds);
      });
      genie.attachments.forEach((a, ai) => {
        if (!a.rows.length || !a.columns.length) return;
        if (a.description) {
          const dId = `r_gd${i}_${ai}`;
          components[dId] = { id: dId, component: 'Text', text: a.description, variant: 'body', style: { ...BODY_STYLE, opacity: 0.75 } };
          childIds.push(dId);
        }
        const tId = `r_gt${i}_${ai}`;
        components[tId] = { id: tId, component: 'Table', columns: a.columns, rows: a.rows };
        childIds.push(tId);
      });
      if (genie.questions.length) {
        const qhId = `r_gqh${i}`;
        components[qhId] = { id: qhId, component: 'Text', text: 'Suggested questions', variant: 'h4', style: HEAD_STYLE };
        childIds.push(qhId);
        const qlId = `r_gql${i}`;
        const itemIds = genie.questions.map((q, qi) => {
          const id = `${qlId}_${qi}`;
          components[id] = { id, component: 'Text', text: q, variant: 'body', style: BODY_STYLE };
          return id;
        });
        components[qlId] = { id: qlId, component: 'List', children: itemIds };
        childIds.push(qlId);
      }
      madeTable = true;
    }

    // A raw SQL tool result (statement_response) → its returned rows as a table.
    if (!madeTable) {
      const sql = sqlEnvelope(it.body || '');
      if (sql) {
        const tId = `r_sql${i}`;
        components[tId] = { id: tId, component: 'Table', columns: sql.columns, rows: sql.rows };
        childIds.push(tId);
        madeTable = true;
      }
    }

    // A JSON result that's a list of records (e.g. web/news search hits) renders
    // as a TABLE — far more scannable than flattened prose. Uses the RAW body so
    // the JSON parses (cleanContextText would strip the URLs we filter by column).
    const groups = madeTable ? null : recordArraysFromJson(it.body || '');
    if (groups) {
      groups.forEach((g, gi) => {
        const cols = tableColumns(g.records);
        if (!cols.length) return;
        if (g.label) {
          const hId = `r_th${i}_${gi}`;
          components[hId] = { id: hId, component: 'Text', text: humanizeKey(g.label), variant: 'h4', style: HEAD_STYLE };
          childIds.push(hId);
        }
        const rows = g.records.map((r) => {
          const row: Record<string, string> = {};
          cols.forEach((c) => { row[humanizeKey(c)] = tableCell(r[c]); });
          return row;
        });
        // Per-row source URL (parallel to rows) so the first column hyperlinks.
        const links = g.records.map((r) => {
          const u = r.url ?? r.uri ?? r.link;
          return typeof u === 'string' ? u : '';
        });
        const tId = `r_tbl${i}_${gi}`;
        components[tId] = { id: tId, component: 'Table', columns: cols.map(humanizeKey), rows, links };
        childIds.push(tId);
        madeTable = true;
      });
    }

    if (!madeTable) {
      // Humanize a JSON envelope (e.g. {"results":"…"} or a Genie response) from the
      // RAW body FIRST, so the JSON parses and its escapes (\n, \uXXXX) are DECODED,
      // then strip object reprs / links / plumbing from the extracted prose. Order
      // matters: cleanContextText's URL strip uses a greedy \S+ that can swallow a
      // trailing quote/brace and break the JSON — so cleaning before the parse would
      // make humanizeToolJson fail and leak the raw, escaped envelope. If the strict
      // parse fails because the backend CLAMPED the output mid-string, salvage the
      // prose from the wrapper; otherwise clean the raw body (cleanContextText still
      // decodes any leaked escapes). Non-JSON bodies (Python reprs) take the last path.
      const bodyText = cleanContextText(
        humanizeToolJson(it.body || '') ?? salvageEnvelopeProse(it.body || '') ?? (it.body || ''),
      );
      const blocks = bodyToComponents(bodyText, `r_b${i}`);
      Object.assign(components, blocks.components);
      childIds.push(...blocks.childIds);
    }

    const colId = `r_col_${i}`;
    components[colId] = { id: colId, component: 'Column', children: childIds };
    const cardId = `r_card_${i}`;
    components[cardId] = { id: cardId, component: 'Card', children: [colId] };
    cardIds.push(cardId);
  });

  components.root = { id: 'root', component: 'Column', children: cardIds };
  return { rootId: 'root', components, data: {} };
}

/**
 * A short, PLAIN-TEXT summary of a step's content for the activity list preview —
 * cleaned exactly like the full view (it runs the body through buildResultsSurface
 * and collects the rendered Text, so reprs, links, provenance and JSON noise are
 * already stripped) then clamped to `maxChars`. Multi-line (\n between blocks) so
 * the caller can show a few readable lines.
 */
export function contextSummary(raw: string, maxChars = 220): string {
  if (!raw || !raw.trim()) return '';
  const surface = buildResultsSurface([{ title: '', body: raw }]);
  const out: string[] = [];
  const seen = new Set<string>();
  const visit = (id: string) => {
    if (seen.has(id)) return;
    seen.add(id);
    const c = surface.components[id];
    if (!c) return;
    if (c.component === 'Text' && typeof c.text === 'string' && c.text.trim()) out.push(c.text.trim());
    // Pull the leading cell (e.g. each result's Title) from a Table so a
    // table-rendered result still yields a readable list preview.
    if (c.component === 'Table') {
      const cols = Array.isArray(c.columns) ? (c.columns as unknown[]).map(String) : [];
      const rows = Array.isArray(c.rows) ? (c.rows as unknown[]) : [];
      rows.slice(0, 5).forEach((r) => {
        if (r && typeof r === 'object') {
          const first = cols.length ? (r as Record<string, unknown>)[cols[0]] : Object.values(r as Record<string, unknown>)[0];
          if (first) out.push(String(first).trim());
        }
      });
    }
    if (Array.isArray(c.children)) c.children.forEach(visit);
  };
  visit(surface.rootId);
  const text = out.join('\n').replace(/[ \t]+/g, ' ').trim();
  return text.length > maxChars ? `${text.slice(0, maxChars).trim()}…` : text;
}
