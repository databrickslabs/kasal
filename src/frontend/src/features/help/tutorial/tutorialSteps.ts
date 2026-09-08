import type { Step } from 'react-joyride';
import type { AppMode } from '../../../store/uiLayout';

const step = (target: string, title: string, content: string, placement: Step['placement'] = 'auto'): Step => ({
  target, title, content, placement, disableBeacon: true,
});

export const tutorialPaths = {
  chat: { title: 'Chat', description: 'Ask questions, create content, and work with your results.' },
  crew: { title: 'Agent Builder', description: 'Design a crew, connect agents and tasks, and run your plan.' },
  flow: { title: 'Flow Builder', description: 'Turn saved crews into a connected workflow.' },
};

export function getTutorialSteps(mode: AppMode, access = { crew: true, flow: true }): Step[] {
  const modes = ['Chat', ...(access.crew ? ['Agent Builder'] : []), ...(access.flow ? ['Flow Builder'] : [])];
  const sharedStart = [
    step('[data-tour="new-session"]', 'Start a session', modes.length === 1
      ? 'New session starts a fresh Chat conversation. Your previous conversations stay in the sidebar.'
      : `Use New session to choose ${modes.join(', ')}. Each session keeps its own conversation${access.crew || access.flow ? ' and, for builders, its canvas' : ''}.`),
    step('[data-tour="session-list"]', 'Pick up where you left off', 'Use the shared session list to return to your work. Search by name, and use a session’s three-dot menu to rename, pin, or archive it. Selecting a session opens it in its original mode.'),
    step('[data-tour="session-archive"]', 'Keep the sidebar tidy', 'Archived sessions stay out of your main list. Open the archive to restore one, or choose Delete session in its three-dot menu to delete it immediately, without confirmation. Saved catalog items and execution records in Activity remain.'),
  ];
  const activityStep = step('[data-tour="workspace-activity"]', 'Find your runs in Activity',
    'Open Activity in the left sidebar to review execution results, traces, and memory. Choose This session for linked runs, or All teamspace runs for the wider history, including scheduled and API runs.'
    + (access.crew || access.flow ? ' Schedules and Assistant logs are also here.' : ''));
  const accountSteps = [
    step('[data-tour="workspace-account-actions"]', 'Your teamspace and appearance', 'The bottom row brings your teamspace, appearance, and settings together. Select the profile or teamspace name to switch teamspaces. The sun or moon changes the appearance across every mode.'),
    step('[data-tour="configuration-button"]', 'Configure Kasal', 'The gear in the same bottom row opens Configuration. It shows the settings available to you, including models, tools, and memory.'),
  ];
  if (mode === 'chat') return [
    ...sharedStart,
    step('[data-tour="chat-composer"]', 'Start with what you need', 'Describe your question or the content you want to create. Continue in the same conversation to refine the result.'),
    step('[data-tour="chat-composer"] [aria-label="Improve prompt"]', 'Refine your prompt', 'Write a prompt, then use the sparkle button to improve it before sending.'),
    step('[data-tour="chat-composer"] [aria-label="Composer settings and tools"]', 'Models, files, and tools', 'Open the plus menu on the right of the input to choose a model, attach files, and select the tools available in your teamspace.'),
    step('[data-tour="chat-catalog"]', 'Use the published catalog', 'Catalog in the left sidebar contains only crews and flows published to Chat for this teamspace. Select an item to use it in your conversation.'),
    step('[data-tour="chat-composer"]', 'Follow the response at your pace', 'Watch progress in the conversation and scroll back while a response is streaming. The send button becomes Stop while work is running. Open a result or trace entry to inspect its details in the preview pane.'),
    activityStep,
    ...accountSteps,
    step('[data-tour="help-button"]', 'Help is always nearby', 'Open this tutorial again from the lower-right corner to explore any mode available to you.'),
  ];
  const isFlow = mode === 'flow';
  return [
    ...sharedStart,
    ...(isFlow ? [step('#builder-available-crews-host', 'Start with your saved crews', 'Flow Builder opens on Available Crews. Search your teamspace’s crews, refresh the list, or add a crew to the canvas. Save a crew in Agent Builder first if you need one here.')] : []),
    step('#builder-assistant-composer-host', isFlow ? 'Describe how the crews should work together' : 'Describe your crew', isFlow
      ? 'Ask Kasal to build a flow using your saved crews. Describe their order, parallel work, or conditions. Submitting opens the conversation so you can follow generation and review the plan.'
      : 'Ask Kasal to create a plan, add an agent, or add a task. Review the generated nodes and connections on the canvas before running.'),
    step('[aria-label="Files and run settings"]', 'Choose the model and run settings', 'The plus menu on the right of the input contains the model selector, files, and run settings. Use the sparkle button beside it to improve a written prompt.'),
    step('.react-flow', isFlow ? 'Review the flow' : 'Review agents and tasks', isFlow
      ? 'Each node represents a crew. Review its connections and the tasks that pass work between crews. Select a connection to configure its routing.'
      : 'Agents define who does the work; tasks define what to do and the expected result. Drag nodes to arrange them and double-click a node to edit it. Connect tasks to the agents that will perform them.', 'center'),
    step('[data-tour="canvas-tools"]', 'Control the canvas here', 'Fit the nodes into view, zoom in or out, change the layout orientation, or clear the canvas using these controls on the canvas itself.'),
    step('[data-tour="workspace-panel-tabs"]', 'Make room for your work', 'The controls at the top of this pane swap the conversation and canvas between left and right, expand the conversation, or close the pane. Drag the divider to resize. In full screen, the input stays below the conversation; Back to canvas returns to the split view.'),
    step('[data-tour="play-execution"]', isFlow ? 'Run the flow' : 'Run the crew', isFlow
      ? 'Use Play in the canvas controls after reviewing the flow and its connections. Follow live calls and answers in the conversation. While it runs, use the dedicated Stop control beside the viewport controls at the bottom-right of the canvas.'
      : 'Use Play in the canvas controls when your crew is ready. Follow live calls and answers in the conversation, and select trace entries to inspect the details beside it. While it runs, use the dedicated Stop control beside the viewport controls at the bottom-right of the canvas.'),
    step('[data-tour="builder-catalog"]', isFlow ? 'Open the flow catalog' : 'Open the crew catalog', isFlow
      ? 'Flow catalog now lives in the left sidebar. Use Save to catalog in the conversation after a plan finishes generating, then browse and load saved flows into their own sessions.'
      : 'Crew catalog now lives in the left sidebar. Use Save to catalog in the conversation after a plan finishes generating. Browse saved crews, agents, and tasks here. Each crew has publish and Deploy to Databricks Apps actions. Loading a crew opens it in its own session.'),
    activityStep,
    ...accountSteps,
    step('[data-tour="help-button"]', 'Return whenever you need', 'Open this tutorial again from the lower-right corner, just like Chat. Event triggers are in Configuration when the preview feature is enabled.'),
  ];
}

/** Hidden Chat remains mounted during builder work; never highlight its controls. */
export function visibleTutorialSteps(steps: Step[]): Step[] {
  return steps.flatMap(item => {
    if (typeof item.target !== 'string') return [item];
    const target = Array.from(document.querySelectorAll<HTMLElement>(item.target)).find(element => {
      const style = window.getComputedStyle(element);
      return element.getClientRects().length > 0 && style.visibility !== 'hidden' && style.display !== 'none';
    });
    return target ? [{ ...item, target }] : [];
  });
}
