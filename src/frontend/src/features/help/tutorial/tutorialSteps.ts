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

export function getTutorialSteps(mode: AppMode): Step[] {
  const sharedStart = [step('[aria-label^="Workspace mode:"]', 'One input, three ways to work', 'Switch between Chat, Agent Builder, and Flow Builder from the mode selector inside the input. The available modes depend on your teamspace access.')];
  const accountSteps = [
    step('[data-tour="configuration-button"]', 'Configuration, always in the same place', 'Open Configuration above the theme toggle to manage the settings available to you, including models, tools, memory, and teamspace access.'),
    step('[data-tour="workspace-account-actions"]', 'Your appearance and teamspace', 'The light and dark mode toggle applies across all three modes. The profile icon below it switches your active teamspace.'),
  ];
  if (mode === 'chat') return [
    ...sharedStart,
    step('[data-tour="chat-composer"]', 'Start with what you need', 'Describe your question or the content you want to create. Continue in the same conversation to refine the result.'),
    step('[data-tour="chat-composer"] [aria-label="Improve prompt"]', 'Refine your prompt', 'Write a prompt, then use the sparkle button to improve it before sending.'),
    step('[data-tour="chat-composer"] [aria-label="Composer settings and tools"]', 'Models, files, and tools', 'Open the plus menu on the right of the input to choose a model, attach files, and select the tools available in your teamspace.'),
    step('[aria-label$="chat history"]', 'Keep your conversations organized', 'Expand the left sidebar to return to recent conversations and browse saved crews and flows. Use New chat for a fresh conversation.'),
    ...accountSteps,
    step('[data-tour="help-button"]', 'Help is always nearby', 'Open this tutorial again from the lower-right corner to explore any mode available to you.'),
  ];
  const isFlow = mode === 'flow';
  return [
    ...sharedStart,
    ...(isFlow ? [step('#builder-available-crews-host', 'Start with your saved crews', 'Flow Builder opens on Available Crews. Search your teamspace’s crews, refresh the list, or add a crew to the canvas. Save a crew in Agent Builder first if you need one here.')] : []),
    step('#builder-assistant-composer-host', isFlow ? 'Describe how the crews should work together' : 'Describe your crew', isFlow
      ? 'Ask Kasal to build a flow using your saved crews. Describe their order, parallel work, or conditions. Submitting switches to Conversation while the flow is generated.'
      : 'Ask Kasal to create a plan, add an agent, or add a task. Review the generated nodes and connections on the canvas before running.'),
    step('[aria-label="Files and run settings"]', 'Choose the model and run settings', 'The plus menu on the right of the input contains the model selector, files, and run settings. Use the sparkle button beside it to improve a written prompt.'),
    step('.react-flow', isFlow ? 'Review the flow' : 'Review agents and tasks', isFlow
      ? 'Each node represents a crew. Review its connections and the tasks that pass work between crews. Select a connection to configure its routing.'
      : 'Agents define who does the work; tasks define what to do and the expected result. Drag nodes to arrange them and double-click a node to edit it. Connect tasks to the agents that will perform them.', 'center'),
    step('[data-tour="workspace-panel-tabs"]', 'Conversation and execution history', isFlow
      ? 'Available Crews, Execution history, and Conversation share this panel. Execution history shows past runs; Conversation shows the conversation. The input stays underneath each tab.'
      : 'Conversation shows the conversation and generated plans. Execution history lets you inspect run progress and results. The input stays underneath both tabs.'),
    step('[aria-label^="Move workspace panel to"]', 'Give the canvas room', 'Swap the conversation panel and canvas between left and right. Drag the divider between them to adjust their widths.'),
    step('[data-tour="play-execution"]', isFlow ? 'Run the flow' : 'Run the crew', isFlow
      ? 'Use Play after reviewing the flow and configuring its connections. Follow execution from Execution history.'
      : 'Use Play when your crew is ready. The button becomes available when the canvas contains agents or tasks. Follow execution from Execution history.'),
    step('[data-tour="save-context"]', isFlow ? 'Save your flow' : 'Save your crew', isFlow
      ? 'Save the flow for reuse. The catalog button below lets you open saved workflows.'
      : 'Save the crew so you can reuse it, share it in your teamspace, or use it in Flow Builder. Open Catalog to load saved crews, agents, and tasks.'),
    ...accountSteps,
    step('[data-tour="help-button"]', 'Return whenever you need', 'The tutorial now lives at the bottom of the right sidebar. Open it again to explore another mode.'),
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
