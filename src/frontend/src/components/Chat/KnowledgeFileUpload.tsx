import React, { useEffect, useRef, useState } from 'react';
import {
  Box,
  Chip,
  CircularProgress,
  IconButton,
  Snackbar,
  Tooltip,
} from '@mui/material';
import { AttachFile as AttachFileIcon } from '@mui/icons-material';
import { Agent } from '../../api/workflow/AgentService';
import { apiClient } from '../../config/api/ApiConfig';
import { useAgentStore } from '../../store/agent';

/**
 * Attach a knowledge file from the canvas chat.
 *
 * A paperclip and a file picker — the same interaction as Chat mode, which is
 * the point: two ways to attach a file to the same backend endpoint should not
 * be two different experiences. This replaced a dialog with an agent
 * multi-select, a Databricks-config warning and an in-dialog file list, all to
 * do what one native file browser does.
 *
 * Attached files SURVIVE A REFRESH, keyed by session, exactly as Chat mode's
 * composer does. They previously did not, which was misleading in a way an
 * empty state is not: the file was still uploaded, still embedded and still
 * searchable by the crew — only the UI had forgotten it, so the user's only
 * recourse was to upload it again and create a duplicate.
 *
 * NO Databricks configuration is required. The old version refused to upload
 * unless a knowledge volume was enabled, which was true when the file went to a
 * Databricks Volume and was indexed in Vector Search. It is not true now:
 * ``DatabricksKnowledgeService.upload_knowledge_file`` stages the file, embeds
 * it into ``knowledge_embeddings`` (SQLite locally, Lakebase pgvector when
 * deployed) and deletes the temp file — "no Databricks Volume is involved" —
 * and ``knowledge/search_service.py`` reads that same table back.
 */

export interface UploadedFile {
  id: string;
  filename: string;
  path: string;
  size: number;
  status: 'uploading' | 'success' | 'error';
  error?: string;
}

/** Imperative handle so a "+" menu item can open the file picker. */
export interface KnowledgeFileUploadHandle {
  open: () => void;
}

interface KnowledgeFileUploadProps {
  executionId: string;
  groupId: string;
  onFilesUploaded?: (files: UploadedFile[]) => void;
  /** Canvas nodes are refreshed from these when an agent gains a source. */
  onAgentsUpdated?: (updatedAgents: Agent[]) => void;
  /** The live wiring: the task gains the file path for its knowledge search. */
  onTasksUpdated?: (uploadedFilePath: string) => void;
  /** Detaching removes the file from the task's knowledge search too. */
  onTaskFileRemoved?: (removedFilePath: string) => void;
  disabled?: boolean;
  compact?: boolean;
  /**
   * Hide the paperclip, keeping the chips. Used when the composer's "+" menu
   * owns the "Add files" ACTION: the action moves into the menu, but the
   * attached-file chips stay visible in the composer — a chip is state that
   * will be sent with the next message, and hiding it is how people re-upload
   * a file they already attached, or send one they forgot about.
   */
  hideTrigger?: boolean;
  /**
   * The agents ON THE CANVAS. Used for the upload's access control and for the
   * "files attached" indicator on the agent.
   */
  availableAgents?: Agent[];
  hasAgents?: boolean;
  hasTasks?: boolean;
}

const ACCEPTED = '.pdf,.txt,.json,.csv,.doc,.docx,.md';

/** Same shape and lifetime rule as Chat mode's composer, keyed per session. */
const attachmentsKey = (executionId: string) =>
  `kasal-canvas-attachments-${executionId}`;

export const KnowledgeFileUpload = React.forwardRef<
  KnowledgeFileUploadHandle,
  KnowledgeFileUploadProps
>(function KnowledgeFileUpload(
  {
    executionId,
    groupId: _groupId,
    onFilesUploaded,
    onAgentsUpdated,
    onTasksUpdated,
    onTaskFileRemoved,
    disabled = false,
    compact = false,
    availableAgents = [],
    hasAgents = false,
    hasTasks = false,
    hideTrigger = false,
  },
  ref,
) {
  const [attached, setAttached] = useState<UploadedFile[]>([]);
  const [uploading, setUploading] = useState(0);
  const [toast, setToast] = useState<string | null>(null);
  const fileInputRef = useRef<HTMLInputElement>(null);
  const hydrated = useRef(false);
  const { updateAgent } = useAgentStore();

  React.useImperativeHandle(ref, () => ({ open: () => fileInputRef.current?.click() }), []);

  const agentIds = availableAgents
    .map((a) => a.id)
    .filter((id): id is string => Boolean(id));

  // Restore what this session already has attached.
  useEffect(() => {
    hydrated.current = false;
    try {
      const raw = localStorage.getItem(attachmentsKey(executionId));
      const stored = raw ? (JSON.parse(raw) as UploadedFile[]) : [];
      setAttached(Array.isArray(stored) ? stored : []);
    } catch {
      setAttached([]);
    }
    hydrated.current = true;
  }, [executionId]);

  // Persist only what actually landed; a failed upload is not worth restoring.
  useEffect(() => {
    if (!hydrated.current) return;
    const ready = attached.filter((f) => f.status === 'success');
    try {
      if (ready.length > 0) {
        localStorage.setItem(attachmentsKey(executionId), JSON.stringify(ready));
      } else {
        localStorage.removeItem(attachmentsKey(executionId));
      }
    } catch {
      /* storage full or blocked — the upload itself is unaffected */
    }
  }, [attached, executionId]);

  const handleFilesSelected = (event: React.ChangeEvent<HTMLInputElement>) => {
    const selected = event.target.files;
    if (selected && selected.length > 0) void uploadFiles(Array.from(selected));
    // Reset so picking the same file again re-triggers change.
    event.target.value = '';
  };

  const uploadFiles = async (files: File[]) => {
    setUploading((n) => n + files.length);
    const done: UploadedFile[] = [];

    for (const file of files) {
      const entry: UploadedFile = {
        id: `${file.name}-${file.size}-${Date.now()}`,
        filename: file.name,
        path: '',
        size: file.size,
        status: 'uploading',
      };
      try {
        const form = new FormData();
        form.append('file', file);
        // Empty: the backend only still reads `selected_agents` from this and
        // calls the field legacy. Agent scoping travels in `agent_ids`.
        form.append('volume_config', JSON.stringify({}));
        form.append('agent_ids', JSON.stringify(agentIds));

        const { data } = await apiClient.post(
          `/databricks/knowledge/upload/${executionId}`,
          form,
          { headers: { 'Content-Type': 'multipart/form-data' } },
        );

        entry.status = 'success';
        entry.path = data?.path || '';

        // The task is what actually searches the file at run time.
        if (entry.path && onTasksUpdated) onTasksUpdated(entry.path);
        if (entry.path && agentIds.length > 0) await recordOnAgents(data);

        setAttached((prev) => [
          ...prev.filter((f) => f.filename !== entry.filename),
          entry,
        ]);
        if (data?.unchanged) {
          setToast(`${file.name} was already attached`);
        }
      } catch (err) {
        // The backend puts the real cause in `detail`; axios's own message is
        // just the status code, which tells the user nothing actionable.
        const detail = (err as { response?: { data?: { detail?: string } } })
          ?.response?.data?.detail;
        entry.status = 'error';
        entry.error =
          detail ?? (err instanceof Error ? err.message : 'Upload failed');
        setToast(`${file.name}: ${entry.error}`);
      } finally {
        setUploading((n) => Math.max(0, n - 1));
        done.push(entry);
      }
    }
    onFilesUploaded?.(done);
  };

  /**
   * Detach a file: forget the chip AND the embedded chunks.
   *
   * Removing only the chip would leave the file searchable by the crew until
   * the TTL expired it, which is not what "detach" means to the person clicking
   * it. The delete is fire-and-forget — the chip goes either way, and a failed
   * cleanup is still caught by the retention sweep.
   */
  const detach = async (file: UploadedFile) => {
    setAttached((prev) => prev.filter((f) => f.id !== file.id));
    onTaskFileRemoved?.(file.path);
    try {
      await apiClient.delete(
        `/databricks/knowledge/delete/${encodeURIComponent(executionId)}/${encodeURIComponent(file.filename)}`,
      );
      setToast(`${file.filename} detached`);
    } catch (err) {
      console.warn(`Could not remove the uploaded copy of ${file.filename}:`, err);
      setToast(`${file.filename} detached (the stored copy expires with the retention window)`);
    }
  };

  /**
   * Record the file on the canvas agents.
   *
   * Display only: ``knowledge_sources`` is what TaskForm reads to show "Knowledge
   * Files Attached", and the backend deliberately ignores it at run time —
   * ``agent_adapter`` deletes it from the config with a warning, because the
   * search happens through the task's tool instead. Failures here are logged,
   * never surfaced: the upload succeeded and the file is searchable regardless.
   */
  const recordOnAgents = async (uploadResp: {
    path: string;
    filename: string;
    size?: number;
    uploaded_at?: string;
  }) => {
    try {
      const { AgentService } = await import('../../api/workflow/AgentService');
      const source = {
        type: 'databricks_volume',
        source: uploadResp.path,
        metadata: {
          filename: uploadResp.filename,
          uploaded_at: uploadResp.uploaded_at || new Date().toISOString(),
        },
        fileInfo: {
          filename: uploadResp.filename,
          path: uploadResp.path,
          file_size_bytes: uploadResp.size,
          is_uploaded: true,
          exists: true,
          success: true,
        },
      } as unknown as import('../../types/workflow/agent').KnowledgeSource;

      const updated: Agent[] = [];
      for (const agentId of agentIds) {
        const current = await AgentService.getAgent(agentId);
        if (!current) continue;
        const existing = current.knowledge_sources || [];
        if (existing.some((s) => s.source === uploadResp.path)) continue;

        const saved = await AgentService.updateAgentFull(agentId, {
          ...current,
          knowledge_sources: [...existing, source],
        } as Omit<Agent, 'id' | 'created_at'>);
        if (saved) {
          updated.push(saved);
          if (saved.id) updateAgent(saved.id, saved);
        }
      }
      if (updated.length > 0) onAgentsUpdated?.(updated);
    } catch (err) {
      console.error('Could not record the knowledge file on the agents:', err);
    }
  };

  const tooltip = !hasAgents
    ? 'Add at least one agent to the canvas before attaching files'
    : !hasTasks
      ? 'Add at least one task to the canvas before attaching files'
      : 'Attach a file for tasks to search';

  return (
    <Box sx={{ display: 'flex', alignItems: 'center', gap: 0.5, flexWrap: 'wrap' }}>
      <input
        ref={fileInputRef}
        type="file"
        multiple
        hidden
        accept={ACCEPTED}
        onChange={handleFilesSelected}
      />

      {!hideTrigger && (
      <Tooltip title={tooltip}>
        <span>
          <IconButton
            onClick={() => fileInputRef.current?.click()}
            disabled={disabled || uploading > 0}
            color="primary"
            size={compact ? 'small' : 'medium'}
            sx={
              compact
                ? {
                    padding: '4px',
                    color: 'text.secondary',
                    '&:hover': {
                      backgroundColor: 'action.hover',
                      color: 'primary.main',
                    },
                  }
                : {}
            }
          >
            {uploading > 0 ? (
              <CircularProgress size={compact ? 16 : 20} />
            ) : (
              <AttachFileIcon sx={compact ? { fontSize: 18 } : {}} />
            )}
          </IconButton>
        </span>
      </Tooltip>
      )}

      {/* One chip per attached file, each with its own detach.
          No file icon: the paperclip beside it already says "attachment", and
          the filename says which one — a document glyph in between repeats
          both and buys nothing. */}
      {attached.map((file) => (
        <Tooltip key={file.id} title={`${file.filename} — click × to detach`}>
          <Chip
            size="small"
            variant="outlined"
            label={file.filename}
            onDelete={() => void detach(file)}
            sx={{
              maxWidth: 180,
              // Follows the surface rather than MUI's grey fill, so it reads as
              // part of the composer and stays correct in a dark theme.
              bgcolor: 'background.paper',
            }}
          />
        </Tooltip>
      ))}

      <Snackbar
        open={Boolean(toast)}
        autoHideDuration={5000}
        onClose={() => setToast(null)}
        message={toast ?? ''}
      />
    </Box>
  );
});

export default KnowledgeFileUpload;
