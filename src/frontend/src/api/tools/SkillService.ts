import { apiClient } from '../../config/api/ApiConfig';

/**
 * Agent Skills — packaged procedural know-how an agent can load on demand.
 *
 * Distinct from tools and knowledge, and the line matters when explaining it:
 * a tool is code the agent CALLS, knowledge is documents it SEARCHES, a skill is
 * how it WORKS. Only a skill's name and description sit in the prompt; the
 * instructions load when the agent decides the skill applies.
 */

export interface SkillFile {
  path: string;
  size_bytes?: number | null;
  sha256?: string | null;
}

export interface Skill {
  id: number;
  name: string;
  description: string;
  body: string;
  license?: string | null;
  compatibility?: string | null;
  metadata: Record<string, unknown>;
  enabled: boolean;
  global_enabled: boolean;
  /** builtin | uploaded | authored. */
  source: string;
  /** True when this workspace row replaces a skill Kasal ships. */
  overrides_builtin?: boolean;
  /** NULL for one of Kasal's builtins; set for a skill this workspace owns. */
  group_id?: string | null;
  files: SkillFile[];
  created_by_email?: string | null;
  updated_at?: string | null;
}

/**
 * A skill as it exists in Unity Catalog (governance metadata only — the body +
 * files live in the UC Files API, not on the securable). `name` is the full
 * `skills/{catalog}.{schema}.{id}` resource name; `finalize_time` is set once the
 * SKILL.md upload has been finalized.
 */
export interface UcSkill {
  name: string;
  bundle_name?: string;
  description?: string;
  comment?: string;
  finalize_time?: string | null;
}

/** One row of a batch UC sync summary (publish-all / pull-all). */
export interface UcSyncResult {
  name: string;
  status: 'ok' | 'error';
  error?: string;
}

export interface TranscriptTurn {
  role: 'user' | 'assistant';
  content: string;
}

/** A proposed SKILL.md from POST /skills/draft, already run through the validator. */
export interface SkillDraft {
  name: string;
  description: string;
  body: string;
  valid: boolean;
  errors: string[];
  warnings: string[];
}

export interface SkillFileInput {
  path: string;
  content: string;
}

export interface SkillInput {
  name: string;
  description: string;
  body?: string;
  license?: string | null;
  compatibility?: string | null;
  metadata?: Record<string, unknown>;
  enabled?: boolean;
  global_enabled?: boolean;
  /**
   * Bundled reference files, REPLACED wholesale when present. Omit the field to
   * leave existing files alone; send [] to remove them all.
   */
  files?: SkillFileInput[];
}

export interface SkillValidationResult {
  valid: boolean;
  /** The reference validator's own messages, not a paraphrase. */
  errors: string[];
  warnings: string[];
}

const BASE = '/skills';

export const SkillService = {
  async list(): Promise<Skill[]> {
    const { data } = await apiClient.get<{ skills: Skill[]; count: number }>(BASE);
    return data.skills ?? [];
  },

  /**
   * Check a draft without saving it.
   *
   * Answers 200 with `valid: false` for an invalid draft — that is the answer
   * the editor asked for, not a failed request — so read the body rather than
   * relying on a rejected promise.
   */
  /**
   * Draft a skill from a request and/or the conversation (capture mode). One
   * generation call on the backend; the draft comes back validated. Nothing is
   * saved — the chat renders it as a card and a person clicks Save.
   */
  async draft(
    request: string,
    transcript?: TranscriptTurn[],
    model?: string,
  ): Promise<SkillDraft> {
    const { data } = await apiClient.post<SkillDraft>(`${BASE}/draft`, {
      request,
      transcript: transcript && transcript.length > 0 ? transcript : null,
      model: model || null,
    });
    return data;
  },

  async validate(input: SkillInput): Promise<SkillValidationResult> {
    const { data } = await apiClient.post<SkillValidationResult>(
      `${BASE}/validate`,
      input,
    );
    return data;
  },

  async create(input: SkillInput): Promise<Skill> {
    const { data } = await apiClient.post<Skill>(BASE, input);
    return data;
  },

  async update(id: number, input: Partial<SkillInput>): Promise<Skill> {
    const { data } = await apiClient.put<Skill>(`${BASE}/${id}`, input);
    return data;
  },

  async remove(id: number): Promise<void> {
    await apiClient.delete(`${BASE}/${id}`);
  },

  /**
   * Turn a skill on or off for this workspace.
   *
   * Disabling one of Kasal's builtins returns the workspace's own copy, which
   * has a DIFFERENT id — so callers reconcile the list by name, not by id.
   */
  async setEnabled(id: number, enabled: boolean): Promise<Skill> {
    const { data } = await apiClient.patch<Skill>(`${BASE}/${id}/enabled`, { enabled });
    return data;
  },

  /**
   * The content of one bundled file.
   *
   * Fetched on demand rather than included in the skill listing: a workspace
   * with twenty skills would otherwise ship every reference file on every page
   * load, and the whole point of bundling them is that they load only when
   * something needs them.
   */
  async readFile(id: number, path: string): Promise<{ path: string; content: string }> {
    const { data } = await apiClient.get<{ path: string; content: string }>(
      `${BASE}/${id}/files`,
      { params: { path } },
    );
    return data;
  },

  /**
   * Put a skill back to the version Kasal ships.
   *
   * Only meaningful for a row that overrides a builtin — the API 404s
   * otherwise, since resetting a skill the workspace wrote itself would just be
   * a delete under a friendlier name.
   */
  async reset(id: number): Promise<Skill> {
    const { data } = await apiClient.post<Skill>(`${BASE}/${id}/reset`);
    return data;
  },

  async upload(file: File, replace = false): Promise<Skill> {
    const form = new FormData();
    form.append('file', file);
    const { data } = await apiClient.post<Skill>(`${BASE}/upload?replace=${replace}`, form, {
      headers: { 'Content-Type': 'multipart/form-data' },
    });
    return data;
  },

  /**
   * Download a skill as a folder-shaped zip.
   *
   * Portability is the reason to use this format at all: what comes out here
   * runs unchanged in Claude Code, Cursor, Codex or Gemini CLI.
   */
  async export(id: number, name: string): Promise<void> {
    const { data } = await apiClient.get<Blob>(`${BASE}/${id}/export`, {
      responseType: 'blob',
    });
    const url = URL.createObjectURL(data);
    const link = document.createElement('a');
    link.href = url;
    link.download = `${name}.zip`;
    link.click();
    URL.revokeObjectURL(url);
  },

  /**
   * Publish a skill into Unity Catalog as a governed UC skill.
   *
   * Runs the create→upload SKILL.md→finalize lifecycle on the backend, on
   * behalf of the logged-in user (OBO) so it only writes where their UC grants
   * allow. Idempotent — re-publishing updates the UC skill's content in place.
   * `catalog`/`schema` are chosen per publish (the caller remembers the last).
   */
  async syncToUc(id: number, catalog: string, schema: string): Promise<UcSkill> {
    const { data } = await apiClient.post<UcSkill>(`${BASE}/${id}/sync-to-uc`, {
      catalog,
      schema,
    });
    return data;
  },

  /** The skills published in a Unity Catalog schema (what the user can see there). */
  async listUc(catalog: string, schema: string): Promise<UcSkill[]> {
    const { data } = await apiClient.get<{ skills: UcSkill[] }>(`${BASE}/uc`, {
      params: { catalog, schema },
    });
    return data.skills ?? [];
  },

  /** Publish every workspace skill to `catalog.schema`; per-skill result summary. */
  async syncAllToUc(catalog: string, schema: string): Promise<UcSyncResult[]> {
    const { data } = await apiClient.post<{ results: UcSyncResult[] }>(
      `${BASE}/sync-all-to-uc`,
      { catalog, schema },
    );
    return data.results ?? [];
  },

  /**
   * Pull every skill published in `catalog.schema` into this workspace.
   *
   * Imported skills upsert BY NAME — a re-pull updates the workspace's own copy
   * in place rather than duplicating it. Per-skill result summary.
   */
  async syncAllFromUc(catalog: string, schema: string): Promise<UcSyncResult[]> {
    const { data } = await apiClient.post<{ results: UcSyncResult[] }>(
      `${BASE}/sync-all-from-uc`,
      { catalog, schema },
    );
    return data.results ?? [];
  },
};

export default SkillService;
