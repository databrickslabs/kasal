/**
 * Domain context, as a pointer to Skills.
 *
 * The generator's domain context is a skill (tagged as UCMV domain context)
 * attached to the agent that runs the task; the backend feeds the attached
 * skill into this tool's `domain_context` parameter. This panel shows whether
 * that is set up and creates / attaches the skill in place — the text itself
 * no longer lives on the task.
 *
 * An inline value saved on an older task is still honoured (after the skill
 * text), so it is shown with a way to move it into a skill.
 */

import React from 'react';
import { Alert, Box, Button, Chip, Typography } from '@mui/material';
import AddIcon from '@mui/icons-material/Add';
import { Skill, SkillService } from '../../../../api/tools/SkillService';
import { openSettingsSection } from '../../../configuration/lib/settingsIntent';
import { isDomainContextSkill } from '../../../configuration/components/Skills/skillTemplates';
import DomainContextSkillDialog from '../../../configuration/components/Skills/DomainContextSkillDialog';
import { attachSkillToAgent } from '../../../configuration/components/Skills/attachSkill';

interface UCMVDomainContextHintProps {
  /** The saved agent that runs this task; undefined = not known (no agent wired). */
  agentId?: string | number;
  agentName?: string;
  /** Skills attached to that agent. */
  agentSkills?: string[];
  /** A `domain_context` still saved on the task's tool config (pre-Skills). */
  inlineValue?: string;
  onClearInline: () => void;
  disabled?: boolean;
}

export const UCMVDomainContextHint: React.FC<UCMVDomainContextHintProps> = ({
  agentId,
  agentName,
  agentSkills,
  inlineValue,
  onClearInline,
  disabled = false,
}) => {
  const [contextSkills, setContextSkills] = React.useState<Skill[] | null>(null);
  // Attachments made from here — the agent prop is a snapshot from when the form opened.
  const [attachedHere, setAttachedHere] = React.useState<string[]>([]);
  const [dialog, setDialog] = React.useState<{ body?: string; fromInline?: boolean } | null>(null);
  const [error, setError] = React.useState<string | null>(null);

  const load = React.useCallback(() => {
    SkillService.list()
      .then((all) => setContextSkills(all.filter((s) => s.enabled && isDomainContextSkill(s))))
      .catch(() => setContextSkills([]));
  }, []);
  React.useEffect(load, [load]);

  const attachedNames = [...(agentSkills || []), ...attachedHere];
  const feeding = (contextSkills || []).filter(
    (s) => s.global_enabled || attachedNames.includes(s.name),
  );
  const available = (contextSkills || []).filter((s) => !feeding.includes(s));
  const inline = (inlineValue || '').trim();
  const agentKnown = agentId !== undefined && agentId !== null && agentId !== '';
  const agentLabel = agentName ? `“${agentName}”` : 'the agent that runs this task';

  const attach = async (name: string) => {
    if (!agentKnown) return;
    setError(null);
    try {
      if (await attachSkillToAgent(agentId as string | number, name)) {
        setAttachedHere((prev) => [...prev, name]);
      }
    } catch {
      setError(`Could not attach ${name} to ${agentLabel}.`);
    }
  };

  return (
    <Box sx={{ display: 'flex', flexDirection: 'column', gap: 1 }}>
      <Typography variant="caption" color="text.secondary">
        Your model&apos;s domain knowledge (business vocabulary, fiscal calendar, naming
        conventions) is a <b>Skill</b> attached to {agentLabel}. It is passed to this tool as
        its domain context (LLM fallback on). Write it from the example or learn it from
        deployed UCMV YAMLs.
      </Typography>

      {error && <Alert severity="error" variant="outlined" sx={{ py: 0 }}>{error}</Alert>}

      {contextSkills !== null && (
        <Box sx={{ display: 'flex', alignItems: 'center', gap: 0.5, flexWrap: 'wrap' }}>
          {feeding.length > 0 ? (
            <>
              <Typography variant="caption">Used for this task:</Typography>
              {feeding.map((s) => (
                <Chip key={s.name} size="small" color="success" variant="outlined" label={s.name} />
              ))}
            </>
          ) : (
            agentKnown && (
              <Typography variant="caption" color="warning.main">
                {agentLabel} has no domain-context skill attached.
              </Typography>
            )
          )}
          {agentKnown && available.length > 0 && (
            <>
              <Typography variant="caption" sx={{ ml: 1 }}>Attach:</Typography>
              {available.map((s) => (
                <Chip
                  key={s.name}
                  size="small"
                  variant="outlined"
                  icon={<AddIcon fontSize="small" />}
                  label={s.name}
                  onClick={disabled ? undefined : () => void attach(s.name)}
                />
              ))}
            </>
          )}
        </Box>
      )}

      <Box sx={{ display: 'flex', gap: 1, flexWrap: 'wrap' }}>
        <Button size="small" variant="outlined" onClick={() => setDialog({})} disabled={disabled}>
          Create domain-context skill
        </Button>
        <Button size="small" onClick={() => openSettingsSection({ section: 'skills' })} disabled={disabled}>
          Manage skills
        </Button>
      </Box>

      {inline && (
        <Alert severity="warning" variant="outlined">
          <Typography variant="caption" component="div">
            This task still carries {inline.length} characters of inline domain context from
            before Skills held it. It is still used (after any skill text). Move it into a skill —
            it is removed from the task once the skill is saved.
          </Typography>
          <Box sx={{ display: 'flex', gap: 1, mt: 1 }}>
            <Button size="small" onClick={() => setDialog({ body: inline, fromInline: true })} disabled={disabled}>
              Move to a skill
            </Button>
            <Button size="small" color="warning" onClick={onClearInline} disabled={disabled}>
              Remove inline text
            </Button>
          </Box>
        </Alert>
      )}

      <DomainContextSkillDialog
        open={dialog !== null}
        onClose={() => setDialog(null)}
        initialBody={dialog?.body}
        attachTo={agentKnown ? { id: agentId as string | number, name: agentName } : undefined}
        onSaved={(saved, attached) => {
          if (attached) setAttachedHere((prev) => [...prev, saved.name]);
          if (dialog?.fromInline) onClearInline();
          load();
        }}
      />
    </Box>
  );
};
