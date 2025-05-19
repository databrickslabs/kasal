import { useFlowWizardStore } from '../../store/flowWizard';

export const useFlowWizard = () => {
  return useFlowWizardStore();
};

export { WizardStep } from '../../types/flow'; 