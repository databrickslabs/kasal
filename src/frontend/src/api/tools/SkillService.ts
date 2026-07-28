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
  /** NULL for one of Kasal's builtins; set for a skill this workspace owns. */
  group_id?: string | null;
  files: SkillFile[];
  created_by_email?: string | null;
  updated_at?: string | null;
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
};

export default SkillService;
