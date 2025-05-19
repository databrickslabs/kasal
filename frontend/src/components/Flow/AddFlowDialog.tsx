import React from 'react';
import { useCallback, useEffect, useState } from 'react';
import { Dialog, DialogTitle, DialogContent, DialogActions, Button, Box, Typography } from '@mui/material';
import { CrewResponse } from '../../types/crews';
import { WizardStep, FlowConfiguration } from '../../types/flow';
import { useFlowWizard } from '../../hooks/global/useFlowWizard';
import CrewSelectionStep from './CrewSelectionStep';
import StartingPointsStep from './StartingPointsStep';
import ListenerConfigurationStep from './ListenerConfigurationStep';
import StateConfigurationStep from './StateConfigurationStep';
import ReviewStep from './ReviewStep';

export interface AddFlowDialogProps {
  open: boolean;
  onClose: () => void;
  onAddCrews: (
    selectedCrews: CrewResponse[],
    positions: { [key: string]: { x: number; y: number } },
    flowConfig?: FlowConfiguration,
    shouldSave?: boolean
  ) => void;
}

const AddFlowDialog: React.FC<AddFlowDialogProps> = ({
  open,
  onClose,
  onAddCrews
}) => {
  const {
    activeStep,
    crews,
    selectedCrewIds: storeSelectedCrewIds,
    listeners,
    tasks,
    actions,
    startingPoints,
    loadCrews,
    toggleCrewSelection,
    handleNext,
    handleBack,
    handleListenerTaskChange,
    handleListenToTaskChange,
    handleStateUpdate,
    handleToggleStartingPoint,
    addListener,
    deleteListener,
    updateListenerName,
    updateListenerConditionType,
    updateRouterConfig,
    resetFlowWizard
  } = useFlowWizard();

  const [flowName, setFlowName] = useState('');

  useEffect(() => {
    if (open) {
      loadCrews();
    }
  }, [open, loadCrews]);

  const handleClose = useCallback(() => {
    resetFlowWizard();
    setFlowName('');
    onClose();
  }, [resetFlowWizard, onClose]);

  const handleCrewSelect = useCallback((crew: CrewResponse) => {
    toggleCrewSelection(crew.id);
  }, [toggleCrewSelection]);

  const handleSave = useCallback(() => {
    // Create flow configuration and save
    const flowConfig: FlowConfiguration = {
      id: `flow-${Date.now()}`,
      name: flowName,
      listeners,
      actions,
      startingPoints: startingPoints.filter(sp => sp.isStartPoint)
    };
    
    // Filter only the selected crews
    const selectedCrews = crews.filter(crew => storeSelectedCrewIds.includes(crew.id));
    
    // Generate default positions for the crews
    const positions: { [key: string]: { x: number; y: number } } = {};
    selectedCrews.forEach((crew, index) => {
      // Place crews in a grid pattern with decent spacing
      const col = index % 3; // 3 columns grid
      const row = Math.floor(index / 3);
      positions[crew.id.toString()] = {
        x: 150 + col * 300, // 300px spacing horizontally
        y: 150 + row * 200  // 200px spacing vertically
      };
    });
    
    onAddCrews(selectedCrews, positions, flowConfig, true);
    handleClose();
  }, [flowName, listeners, actions, startingPoints, onAddCrews, handleClose, crews, storeSelectedCrewIds]);

  const renderStep = () => {
    switch (activeStep) {
      case WizardStep.SelectCrews:
        return (
          <CrewSelectionStep
            crews={crews}
            selectedCrewIds={storeSelectedCrewIds}
            onCrewSelect={handleCrewSelect}
          />
        );
      case WizardStep.DefineStartingPoints:
        return (
          <StartingPointsStep
            tasks={tasks}
            startingPoints={startingPoints}
            onToggleStartingPoint={handleToggleStartingPoint}
          />
        );
      case WizardStep.ConfigureListeners:
        return (
          <ListenerConfigurationStep
            tasks={tasks}
            listeners={listeners}
            selectedCrewIds={storeSelectedCrewIds}
            crews={crews}
            onListenerTaskChange={handleListenerTaskChange}
            onListenToTaskChange={handleListenToTaskChange}
            onAddListener={addListener}
            onDeleteListener={deleteListener}
            onUpdateListenerName={updateListenerName}
            onUpdateConditionType={updateListenerConditionType}
            onUpdateRouterConfig={updateRouterConfig}
          />
        );
      case WizardStep.ConfigureState:
        return (
          <StateConfigurationStep
            listeners={listeners}
            onStateUpdate={handleStateUpdate}
          />
        );
      case WizardStep.Review:
        return (
          <ReviewStep
            flowName={flowName}
            setFlowName={setFlowName}
            listeners={listeners}
            actions={actions}
            startingPoints={startingPoints}
          />
        );
      default:
        return null;
    }
  };

  return (
    <Dialog open={open} onClose={handleClose} maxWidth="md" fullWidth>
      <DialogTitle>Add Flow</DialogTitle>
      <DialogContent>
        <Box sx={{ mt: 2 }}>
          <Typography variant="subtitle1" gutterBottom>
            Step {activeStep + 1} of {Object.keys(WizardStep).length / 2}
          </Typography>
          {renderStep()}
        </Box>
      </DialogContent>
      <DialogActions>
        <Button onClick={handleClose}>Cancel</Button>
        <Button onClick={handleBack} disabled={activeStep === 0}>
          Back
        </Button>
        {activeStep === WizardStep.Review ? (
          <Button onClick={handleSave} variant="contained" color="primary">
            Save
          </Button>
        ) : (
          <Button onClick={handleNext} variant="contained" color="primary">
            Next
          </Button>
        )}
      </DialogActions>
    </Dialog>
  );
};

export default AddFlowDialog; 