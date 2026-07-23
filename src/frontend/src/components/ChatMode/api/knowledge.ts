import { getClient } from './client';

/*
 * Knowledge-file upload for the chat workspace. Reuses Kasal's knowledge upload
 * endpoint (the same one the workflow designer uses): the file is stored in the
 * configured Databricks Volume and embedded into the workspace's pgvector
 * knowledge store (Lakebase), scoped by group_id. The dispatched crew can then
 * search it via the DatabricksKnowledgeSearchTool.
 */

export interface KnowledgeUploadResult {
  /** Full stored path (e.g. /Volumes/.../file.txt) used as the knowledge source. */
  path: string;
  status: string;
  filename: string;
}

/**
 * Upload one knowledge file, scoped to `executionId` (a stable per-chat id).
 * Group isolation + auth come from the shared apiClient interceptors.
 */
export async function uploadKnowledgeFile(
  file: File,
  executionId: string,
  signal?: AbortSignal
): Promise<KnowledgeUploadResult> {
  const form = new FormData();
  form.append('file', file);
  // Empty volume_config -> backend falls back to the group's configured volume.
  form.append('volume_config', JSON.stringify({}));
  form.append('agent_ids', JSON.stringify([]));

  let response;
  try {
    response = await getClient().post(
      `/databricks/knowledge/upload/${executionId}`,
      form,
      { headers: { 'Content-Type': 'multipart/form-data' }, signal }
    );
  } catch (err) {
    // The backend raises Kasal exceptions (HTTP 4xx/5xx) with the real cause in
    // `detail`; surface that instead of axios's generic "status code 4xx".
    const detail = (err as { response?: { data?: { detail?: unknown } } })?.response?.data?.detail;
    if (typeof detail === 'string' && detail) throw new Error(detail);
    throw err instanceof Error ? err : new Error('Upload failed');
  }

  const data = (response.data || {}) as Record<string, unknown>;
  if (data.status === 'error') {
    throw new Error((data.message as string) || 'Upload failed');
  }
  return {
    path: (data.path as string) || '',
    status: (data.status as string) || 'success',
    filename: (data.filename as string) || file.name,
  };
}
