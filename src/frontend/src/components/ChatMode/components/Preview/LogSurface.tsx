import React, { useMemo } from 'react';
import A2uiSurface from '../Chat/A2uiSurface';
import { buildResultsSurface } from './runActivitySurface';
import { legacyToNewSurface } from '../../utils/surfaceAdapter';
import { useWorkspaceThemes } from '../../hooks/useWorkspaceThemes';
import { LOGS_THEME, type Theme } from '../../../Configuration/uiConfigShared';

/**
 * Renders a step's retrieved context as a themed A2UI surface in the dedicated
 * "logs" style (a light console — see LOGS_THEME). Reuses the SAME formatter as
 * the deliverable ({@link buildResultsSurface}) so a JSON tool envelope, headings
 * and bullet runs come out readable — then adapts it to the shared Surface shape
 * and draws it through the one A2uiSurface renderer.
 *
 * The console gets its OWN palette: the workspace's 'logs' theme (UI Configurator)
 * if customized, else the built-in LOGS_THEME — passed as A2uiSurface's palette
 * override so it never inherits the deliverable's branding.
 */
const LogSurfaceBase: React.FC<{ body: string; title?: string }> = ({ body, title = '' }) => {
  const themes = useWorkspaceThemes();
  const surface = useMemo(
    () => legacyToNewSurface(buildResultsSurface([{ title, body }])),
    [title, body],
  );
  const logsPalette = (themes?.logs ?? LOGS_THEME) as Theme;
  return <A2uiSurface surface={surface} palette={logsPalette} />;
};

const LogSurface = React.memo(LogSurfaceBase);
export default LogSurface;
