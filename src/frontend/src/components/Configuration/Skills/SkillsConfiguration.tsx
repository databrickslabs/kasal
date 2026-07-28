import React, { useCallback, useEffect, useRef, useState } from 'react';
import {
  Alert,
  Box,
  Button,
  Chip,
  CircularProgress,
  FormControlLabel,
  IconButton,
  Paper,
  Snackbar,
  Stack,
  Switch,
  Tooltip,
  Typography,
} from '@mui/material';
import AddIcon from '@mui/icons-material/Add';
import DeleteIcon from '@mui/icons-material/Delete';
import DownloadIcon from '@mui/icons-material/Download';
import EditIcon from '@mui/icons-material/Edit';
import RestartAltIcon from '@mui/icons-material/RestartAlt';
import UploadFileIcon from '@mui/icons-material/UploadFile';
import { Skill, SkillInput, SkillService } from '../../../api/tools/SkillService';
import SkillEditor from './SkillEditor';
import SkillFileViewer from './SkillFileViewer';

/**
 * Agent Skills — authoring, upload, enablement.
 *
 * The line to hold in the copy here, because skills and knowledge will look
 * similar to users: **knowledge is what an agent searches, a skill is how it
 * works.** One is retrieval, the other is procedure.
 *
 * Shaped like the MCP page so an admin learns one interface, with one
 * difference that matters: skills are workspace CONTENT, so a workspace authors
 * its own rather than waiting for a Kasal admin. Kasal's builtins show as
 * read-only and can be turned off per workspace.
 */
const SkillsConfiguration: React.FC = () => {
  const [skills, setSkills] = useState<Skill[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [toast, setToast] = useState<string | null>(null);
  const [editing, setEditing] = useState<Skill | null>(null);
  const [editorOpen, setEditorOpen] = useState(false);
  const [viewing, setViewing] = useState<{ skill: Skill; path: string } | null>(
    null,
  );
  const fileInput = useRef<HTMLInputElement>(null);

  const load = useCallback(async () => {
    setLoading(true);
    try {
      setSkills(await SkillService.list());
      setError(null);
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Could not load skills.');
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    void load();
  }, [load]);

  const handleSave = async (input: SkillInput) => {
    const saved = editing
      ? await SkillService.update(editing.id, input)
      : await SkillService.create(input);
    setSkills((prev) =>
      editing ? prev.map((s) => (s.id === saved.id ? saved : s)) : [...prev, saved],
    );
    setToast(`${saved.name} saved.`);
  };

  const handleToggle = async (skill: Skill) => {
    try {
      const saved = await SkillService.setEnabled(skill.id, !skill.enabled);
      // Turning off a builtin returns this workspace's own copy, with a
      // DIFFERENT id — so the row is replaced by name, not by id.
      setSkills((prev) => prev.map((s) => (s.name === saved.name ? saved : s)));
    } catch (err) {
      setToast(err instanceof Error ? err.message : 'Could not change that.');
    }
  };

  const handleDelete = async (skill: Skill) => {
    try {
      await SkillService.remove(skill.id);
      setSkills((prev) => prev.filter((s) => s.id !== skill.id));
      setToast(`${skill.name} removed.`);
    } catch (err) {
      setToast(err instanceof Error ? err.message : 'Could not remove that skill.');
    }
  };

  const handleReset = async (skill: Skill) => {
    try {
      const shipped = await SkillService.reset(skill.id);
      // The reset row has the BUILTIN's id, not the override's, so reconcile by
      // name — the same rule the enable toggle follows.
      setSkills((prev) => prev.map((s) => (s.name === shipped.name ? shipped : s)));
      setToast(`${shipped.name} restored to the version Kasal ships.`);
    } catch (err) {
      setToast(err instanceof Error ? err.message : 'Could not reset that skill.');
    }
  };

  const handleUpload = async (file: File) => {
    try {
      const saved = await SkillService.upload(file);
      setSkills((prev) => [...prev.filter((s) => s.name !== saved.name), saved]);
      setToast(`${saved.name} imported.`);
    } catch (err) {
      // The backend's message is the reference validator's own wording, or the
      // packaging refusal — both are what an author needs to act on, so it is
      // surfaced verbatim rather than replaced with "upload failed".
      const detail =
        (err as { response?: { data?: { detail?: string } } })?.response?.data?.detail;
      setToast(detail ?? (err instanceof Error ? err.message : 'Import failed.'));
    }
  };

  return (
    <Box>
      <Stack
        direction="row"
        justifyContent="space-between"
        alignItems="flex-start"
        spacing={2}
        sx={{ mb: 2 }}
      >
        {/* The text yields, the actions do not: letting a two-line description
            compete for width is what wraps a button label onto two lines. */}
        <Box sx={{ minWidth: 0 }}>
          <Typography variant="h6">Skills</Typography>
          <Typography variant="body2" color="text.secondary">
            Procedures your agents can follow — &quot;how we run a QBR&quot;, &quot;how we
            price a deal&quot;. Knowledge is what an agent searches; a skill is how it
            works. Attach them to an agent in its Skills section.
          </Typography>
        </Box>
        <Stack direction="row" spacing={1} sx={{ flexShrink: 0 }}>
          <Button
            size="small"
            startIcon={<UploadFileIcon />}
            onClick={() => fileInput.current?.click()}
            sx={{ whiteSpace: 'nowrap' }}
          >
            Import
          </Button>
          <Button
            size="small"
            startIcon={<AddIcon />}
            variant="contained"
            onClick={() => {
              setEditing(null);
              setEditorOpen(true);
            }}
            sx={{ whiteSpace: 'nowrap' }}
          >
            New skill
          </Button>
        </Stack>
      </Stack>

      <input
        ref={fileInput}
        type="file"
        accept=".zip"
        hidden
        onChange={(e) => {
          const file = e.target.files?.[0];
          if (file) void handleUpload(file);
          e.target.value = '';
        }}
      />

      {error && (
        <Alert severity="error" sx={{ mb: 2 }}>
          {error}
        </Alert>
      )}

      {loading ? (
        <Box sx={{ display: 'flex', justifyContent: 'center', py: 4 }}>
          <CircularProgress size={28} />
        </Box>
      ) : skills.length === 0 ? (
        <Paper variant="outlined" sx={{ p: 3, textAlign: 'center' }}>
          <Typography variant="body2" color="text.secondary">
            No skills yet. Write one here, or import a skill folder from Claude
            Code, Cursor or anywhere else that uses the Agent Skills format.
          </Typography>
        </Paper>
      ) : (
        <Stack spacing={1.5}>
          {skills.map((skill) => {
            // A builtin is Kasal's own content: toggleable per workspace,
            // editable only by authoring your own skill of the same name.
            const isBuiltin = !skill.group_id;
            return (
              <Paper key={skill.id} variant="outlined" sx={{ p: 2 }}>
                <Stack
                  direction="row"
                  justifyContent="space-between"
                  alignItems="flex-start"
                  spacing={2}
                >
                  <Box sx={{ minWidth: 0, flex: 1 }}>
                    <Stack direction="row" spacing={1} alignItems="center">
                      <Typography variant="subtitle1">{skill.name}</Typography>
                      {isBuiltin && (
                        <Tooltip title="Shipped by Kasal. Edit it freely — your changes stay in this teamspace, and Reset brings the shipped version back.">
                          <Chip size="small" variant="outlined" label="Built in" />
                        </Tooltip>
                      )}
                      {skill.overrides_builtin && (
                        <Tooltip title="You have edited the built-in skill. Reset restores the shipped version.">
                          <Chip size="small" color="info" variant="outlined" label="Edited" />
                        </Tooltip>
                      )}
                      {skill.source === 'uploaded' && (
                        <Chip size="small" variant="outlined" label="Imported" />
                      )}
                      {skill.global_enabled && (
                        <Chip size="small" color="primary" label="All agents" />
                      )}
                    </Stack>

                    <Typography variant="body2" color="text.secondary" sx={{ mt: 0.5 }}>
                      {skill.description}
                    </Typography>

                    {skill.files.length > 0 && (
                      <Stack
                        direction="row"
                        spacing={0.5}
                        sx={{ mt: 1, flexWrap: 'wrap', gap: 0.5 }}
                      >
                        {/* Clickable: the chips used to name files with no way
                            to read them, which reads as a broken reference. */}
                        {skill.files.map((f) => (
                          <Tooltip key={f.path} title="Open this file">
                            <Chip
                              size="small"
                              variant="outlined"
                              label={f.path}
                              onClick={() => setViewing({ skill, path: f.path })}
                            />
                          </Tooltip>
                        ))}
                      </Stack>
                    )}
                  </Box>

                  <Stack direction="row" spacing={0.5} alignItems="center">
                    <FormControlLabel
                      control={
                        <Switch
                          size="small"
                          checked={skill.enabled}
                          onChange={() => void handleToggle(skill)}
                        />
                      }
                      label={
                        <Typography variant="caption">
                          {skill.enabled ? 'Enabled' : 'Disabled'}
                        </Typography>
                      }
                    />
                    <Tooltip title="Download as a skill folder">
                      <IconButton
                        size="small"
                        onClick={() => void SkillService.export(skill.id, skill.name)}
                      >
                        <DownloadIcon fontSize="small" />
                      </IconButton>
                    </Tooltip>
                    {/* Editable whether or not it is a builtin: editing one
                        saves this workspace's own copy underneath, which the
                        user never has to think about. */}
                    <IconButton
                      size="small"
                      onClick={() => {
                        setEditing(skill);
                        setEditorOpen(true);
                      }}
                    >
                      <EditIcon fontSize="small" />
                    </IconButton>
                    {skill.overrides_builtin && (
                      <Tooltip title="Discard your changes and go back to the version Kasal ships">
                        <IconButton
                          size="small"
                          onClick={() => void handleReset(skill)}
                        >
                          <RestartAltIcon fontSize="small" />
                        </IconButton>
                      </Tooltip>
                    )}
                    {!isBuiltin && (
                      <>
                        <IconButton
                          size="small"
                          color="error"
                          onClick={() => void handleDelete(skill)}
                        >
                          <DeleteIcon fontSize="small" />
                        </IconButton>
                      </>
                    )}
                  </Stack>
                </Stack>
              </Paper>
            );
          })}
        </Stack>
      )}

      <SkillEditor
        open={editorOpen}
        skill={editing}
        onClose={() => setEditorOpen(false)}
        onSave={handleSave}
      />

      <SkillFileViewer
        open={Boolean(viewing)}
        skillId={viewing?.skill.id ?? null}
        skillName={viewing?.skill.name ?? ''}
        path={viewing?.path ?? ''}
        onClose={() => setViewing(null)}
      />

      <Snackbar
        open={Boolean(toast)}
        autoHideDuration={5000}
        onClose={() => setToast(null)}
        message={toast ?? ''}
      />
    </Box>
  );
};

export default SkillsConfiguration;
