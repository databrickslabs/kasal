import React, { useCallback, useEffect, useRef, useState } from 'react';
import Joyride, { ACTIONS, EVENTS, STATUS, type CallBackProps, type Step, type TooltipRenderProps } from 'react-joyride';
import { Box, Button, ButtonBase, Dialog, DialogContent, IconButton, Paper, Typography } from '@mui/material';
import { alpha, useTheme } from '@mui/material/styles';
import { ArrowForward, Close } from '@mui/icons-material';
import { Bot, MessageCircle, Workflow } from 'lucide-react';
import { usePermissionStore } from '../../../store/permissions';
import { useWorkflowStore } from '../../../store/workflow';
import { useUILayoutStore, type AppMode } from '../../../store/uiLayout';
import { useFlowConfigStore } from '../../../store/flowConfig';
import { switchWorkspaceMode } from '../../../app/sessions/sessionNavigation';
import { useAppStore } from '../../chat/store/appStore';
import type { TutorialProps } from '../../../types/config/tutorial';
import { getTutorialSteps, tutorialPaths, visibleTutorialSteps } from './tutorialSteps';

function TutorialTooltip({ index, size, step, backProps, closeProps, primaryProps, skipProps, tooltipProps, isLastStep }: TooltipRenderProps) {
  const theme = useTheme();
  return (
    <Paper {...tooltipProps} data-testid="tutorial-tooltip" elevation={8} sx={{ width: 360, maxWidth: 'calc(100vw - 32px)', borderRadius: 3, p: 2.5, background: theme.palette.background.paper, color: 'text.primary' }}>
      <Box sx={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', mb: 1 }}>
        <Typography sx={{ fontSize: 11, fontWeight: 600, color: 'text.secondary', letterSpacing: 0.5 }}>QUICK TOUR · {index + 1} OF {size}</Typography>
        <IconButton {...closeProps} aria-label="Close tutorial" size="small"><Close sx={{ fontSize: 18 }} /></IconButton>
      </Box>
      <Typography component="h2" sx={{ fontSize: 18, fontWeight: 600, lineHeight: 1.4, mb: 1 }}>{step.title}</Typography>
      <Typography component="div" sx={{ fontSize: 14, lineHeight: 1.7, color: 'text.secondary' }}>{step.content}</Typography>
      <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, mt: 2.5 }}>
        <Button {...skipProps} color="inherit" size="small" sx={{ mr: 'auto', color: 'text.secondary' }}>End tour</Button>
        {index > 0 && <Button {...backProps} color="inherit" size="small">Back</Button>}
        <Button {...primaryProps} aria-label={isLastStep ? 'Finish tutorial' : 'Next step'} size="small" sx={{ borderRadius: 2, px: 2, bgcolor: 'text.primary', color: 'background.paper', '&:hover': { bgcolor: alpha(theme.palette.text.primary, 0.85) } }}>{isLastStep ? 'Done' : 'Next'}</Button>
      </Box>
    </Paper>
  );
}

const icons = { chat: MessageCircle, crew: Bot, flow: Workflow };

const InteractiveTutorial: React.FC<TutorialProps> = ({ isOpen, onClose }) => {
  const theme = useTheme();
  const appMode = useUILayoutStore(state => state.appMode);
  const allowAgent = usePermissionStore(state => state.allowAgentBuilder);
  const allowFlow = usePermissionStore(state => state.allowFlowBuilder);
  const flowsEnabled = useFlowConfigStore(state => state.kasalFlowEnabled);
  const [path, setPath] = useState<AppMode | null>(null);
  const [steps, setSteps] = useState<Step[]>([]);
  const [stepIndex, setStepIndex] = useState(0);
  const [run, setRun] = useState(false);
  const sidebarBeforeTour = useRef<boolean | null>(null);
  const restoreSidebar = useCallback(() => {
    if (sidebarBeforeTour.current !== null) {
      useAppStore.getState().setSidebarOpen(sidebarBeforeTour.current);
      sidebarBeforeTour.current = null;
    }
  }, []);

  const close = useCallback(() => {
    setRun(false);
    setPath(null);
    restoreSidebar();
    useWorkflowStore.getState().setHasSeenTutorial(true);
    onClose();
  }, [onClose, restoreSidebar]);

  useEffect(() => {
    if (!isOpen) { setRun(false); setPath(null); setSteps([]); restoreSidebar(); }
  }, [isOpen, restoreSidebar]);
  useEffect(() => restoreSidebar, [restoreSidebar]);

  // Wait for the selected mode and composer portals to mount, and cancel on close.
  useEffect(() => {
    if (!isOpen || !path) return;
    const timer = window.setTimeout(() => {
      setSteps(visibleTutorialSteps(getTutorialSteps(path, { crew: allowAgent, flow: allowFlow && flowsEnabled })));
      setStepIndex(0);
      setRun(true);
    }, 400);
    return () => window.clearTimeout(timer);
  }, [isOpen, path, allowAgent, allowFlow, flowsEnabled]);

  const start = (mode: AppMode) => {
    if ((mode === 'crew' && !allowAgent) || (mode === 'flow' && (!allowFlow || !flowsEnabled))) return;
    const layout = useUILayoutStore.getState();
    switchWorkspaceMode(mode);
    if (useUILayoutStore.getState().appMode !== mode) return;
    sidebarBeforeTour.current ??= useAppStore.getState().sidebarOpen;
    useAppStore.getState().setSidebarOpen(true);
    window.dispatchEvent(new Event('collapseBuilderConversation'));
    if (mode === 'flow') layout.setFlowPanelTab('crews');
    if (mode === 'crew') layout.setAssistantPanelVisible(true);
    setPath(mode);
  };

  const handleCallback = (data: CallBackProps) => {
    if (data.status === STATUS.FINISHED || data.status === STATUS.SKIPPED || data.action === ACTIONS.CLOSE) {
      close();
    } else if (data.type === EVENTS.STEP_AFTER || data.type === EVENTS.TARGET_NOT_FOUND) {
      const next = data.index + (data.action === ACTIONS.PREV ? -1 : 1);
      if (next >= steps.length) close();
      else setStepIndex(Math.max(0, next));
    }
  };

  if (!isOpen) return null;
  const modes = (['chat', 'crew', 'flow'] as AppMode[]).filter(mode => mode === 'chat' || (mode === 'crew' ? allowAgent : allowFlow && flowsEnabled));
  return (
    <>
      <Dialog open={!path} onClose={close} maxWidth="sm" fullWidth aria-labelledby="tutorial-title" PaperProps={{ sx: { borderRadius: 4, background: theme.palette.background.paper, border: 0 } }}>
        <DialogContent sx={{ p: { xs: 2.5, sm: 3.5 } }}>
          <Box sx={{ display: 'flex', alignItems: 'flex-start', gap: 2, mb: 3 }}>
            <Box sx={{ flex: 1 }}>
              <Typography sx={{ fontSize: 11, fontWeight: 600, letterSpacing: 1, color: 'text.secondary', mb: 1 }}>GET TO KNOW KASAL</Typography>
              <Typography id="tutorial-title" component="h2" sx={{ fontSize: 24, fontWeight: 600, mb: 1 }}>Get comfortable with Kasal</Typography>
              <Typography sx={{ fontSize: 14, lineHeight: 1.6, color: 'text.secondary' }}>Explore sessions, your conversation, and the controls around it. Choose a tour to get started.</Typography>
            </Box>
            <IconButton aria-label="Close tutorial" onClick={close} size="small"><Close sx={{ fontSize: 20 }} /></IconButton>
          </Box>
          <Box sx={{ display: 'flex', flexDirection: 'column', gap: 1.25 }}>
            {modes.map(mode => {
              const Icon = icons[mode];
              return <ButtonBase key={mode} aria-label={`Tour ${tutorialPaths[mode].title}`} onClick={() => start(mode)} sx={{ width: '100%', textAlign: 'left', justifyContent: 'flex-start', gap: 2, p: 2, borderRadius: 2.5, bgcolor: 'action.hover', '&:hover': { bgcolor: 'action.selected' }, '&.Mui-focusVisible': { outline: `2px solid ${theme.palette.text.secondary}`, outlineOffset: 2 } }}>
                <Box sx={{ width: 40, height: 40, borderRadius: 2, display: 'flex', alignItems: 'center', justifyContent: 'center', bgcolor: 'background.paper', color: 'text.secondary', flexShrink: 0 }}><Icon size={20} strokeWidth={1.7} /></Box>
                <Box sx={{ flex: 1 }}><Typography sx={{ fontSize: 15, fontWeight: 600, mb: 0.5 }}>{tutorialPaths[mode].title}{mode === appMode && <Box component="span" sx={{ ml: 1, fontSize: 11, fontWeight: 400, color: 'text.secondary' }}>Current mode</Box>}</Typography><Typography sx={{ fontSize: 13, color: 'text.secondary', lineHeight: 1.5 }}>{tutorialPaths[mode].description}</Typography></Box>
                <ArrowForward sx={{ fontSize: 17, color: 'text.secondary', flexShrink: 0 }} />
              </ButtonBase>;
            })}
          </Box>
        </DialogContent>
      </Dialog>
      {path && steps.length > 0 && <Joyride key={path} steps={steps} stepIndex={stepIndex} run={run} continuous showProgress showSkipButton tooltipComponent={TutorialTooltip} callback={handleCallback} disableScrolling disableOverlayClose spotlightPadding={6}
        styles={{ options: { backgroundColor: theme.palette.background.paper, arrowColor: theme.palette.background.paper, primaryColor: theme.palette.text.primary, textColor: theme.palette.text.primary, overlayColor: alpha(theme.palette.common.black, theme.palette.mode === 'dark' ? 0.6 : 0.35), zIndex: 10000 }, spotlight: { borderRadius: 12 } }}
      />}
    </>
  );
};

export default InteractiveTutorial;
