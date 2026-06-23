import React, { useMemo } from 'react';
import UiRenderer from './UiRenderer';
import { applyConfiguredTheme, buildResultsSurface } from '../../utils/uiDocument';
import { useWorkspaceThemes } from '../../hooks/useWorkspaceThemes';

/**
 * Renders a step's retrieved context as a themed A2UI surface in the dedicated
 * "logs" style (a light console — see LOGS_THEME). Reuses the SAME formatter as
 * the deliverable ({@link buildResultsSurface}) so a JSON tool envelope, headings
 * and bullet runs come out readable — not as a raw blob.
 *
 * Self-contained: resolves the workspace's 'logs' palette (UI Configurator) or
 * falls back to the built-in LOGS_THEME. Shared by the clickable run timeline
 * ({@link RunTimeline}) and the auto-advancing activity screens
 * ({@link ActivityScreens}).
 */
const LogSurfaceBase: React.FC<{ body: string; title?: string }> = ({ body, title = '' }) => {
  const themes = useWorkspaceThemes();
  const surface = useMemo(
    () => applyConfiguredTheme(buildResultsSurface([{ title, body }]), themes, 'logs'),
    [title, body, themes],
  );
  return <UiRenderer surface={surface} />;
};

const LogSurface = React.memo(LogSurfaceBase);
export default LogSurface;
