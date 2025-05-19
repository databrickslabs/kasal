import React, { useState, useEffect } from 'react';
import { 
  Dialog, 
  DialogTitle, 
  DialogContent, 
  DialogActions, 
  Button, 
  Typography, 
  Box,
  IconButton,
  MobileStepper
} from '@mui/material';
import KeyboardArrowLeft from '@mui/icons-material/KeyboardArrowLeft';
import KeyboardArrowRight from '@mui/icons-material/KeyboardArrowRight';
import CloseIcon from '@mui/icons-material/Close';
import { TutorialProps } from '../../types/tutorial';

// Define our own tutorial steps without using Joyride
interface TutorialStep {
  title: string;
  content: string;
  target: string; // For information only - we don't use this anymore
}

const Tutorial: React.FC<TutorialProps> = ({ isOpen, onClose }) => {
  const [showWelcomeDialog, setShowWelcomeDialog] = useState(true);
  const [activeStep, setActiveStep] = useState(0);
  const [showTutorial, setShowTutorial] = useState(false);

  // Define tutorial steps
  const steps: TutorialStep[] = [
    {
      title: 'Welcome to Kasal!',
      content: 'Let\'s walk through how to create and manage your AI crews.',
      target: 'body',
    },
    {
      title: 'Add Agents',
      content: 'Start by adding agents to your crew. Use the "Add Agent" button to create new agents and define their specific roles and capabilities.',
      target: 'button[data-tour="add-agent"]',
    },
    {
      title: 'Add Tasks',
      content: 'Next, add tasks for your agents. Use the "Add Task" button to create tasks and assign them to specific agents. You can also connect tasks on the canvas to establish dependencies between them.',
      target: 'button[data-tour="add-task"]',
    },
    {
      title: 'Save Plan',
      content: 'Once you\'ve set up your agents and tasks, save your plan. This stores your entire workflow for future use.',
      target: 'button[data-tour="save-plan"]',
    },
    {
      title: 'Load Plan',
      content: 'Alternatively, you can load previously saved plans here instead of creating them from scratch.',
      target: 'button[data-tour="load-plan"]',
    },
    {
      title: 'Generate Plan',
      content: 'Another option is to use the Generate Plan feature, which will automatically create all the agents, tasks, and dependencies for you. After generation, you can either run the plan directly or fine-tune it to your needs.',
      target: 'button[data-tour="generate-plan"]',
    },
    {
      title: 'Run Plan',
      content: 'When you\'re ready, click "Run Plan" to execute your plan and watch your AI crew in action!',
      target: 'button[data-tour="run-plan"]',
    }
  ];

  const handleStartTutorial = () => {
    setShowWelcomeDialog(false);
    setActiveStep(0);
    setShowTutorial(true);
  };

  const handleSkipTutorial = () => {
    setShowWelcomeDialog(false);
    setShowTutorial(false);
    onClose();
  };

  const handleNext = () => {
    setActiveStep((prevStep) => {
      const nextStep = prevStep + 1;
      if (nextStep >= steps.length) {
        // We've reached the end
        setShowTutorial(false);
        onClose();
        return 0;
      }
      return nextStep;
    });
  };

  const handleBack = () => {
    setActiveStep((prevStep) => Math.max(0, prevStep - 1));
  };

  const handleClose = () => {
    setShowTutorial(false);
    onClose();
  };

  useEffect(() => {
    if (isOpen) {
      setShowWelcomeDialog(true);
    } else {
      setShowTutorial(false);
    }
  }, [isOpen]);

  return (
    <>
      {/* Welcome dialog */}
      <Dialog 
        open={showWelcomeDialog && isOpen} 
        onClose={handleSkipTutorial}
        PaperProps={{
          sx: { borderRadius: 2 }
        }}
      >
        <DialogTitle>Welcome to Kasal!</DialogTitle>
        <DialogContent>
          <Typography>
            Would you like to take a quick tour to learn how to use the application?
          </Typography>
        </DialogContent>
        <DialogActions>
          <Button onClick={handleSkipTutorial}>Skip Tutorial</Button>
          <Button onClick={handleStartTutorial} variant="contained" color="primary">
            Start Tutorial
          </Button>
        </DialogActions>
      </Dialog>

      {/* Tutorial dialog - replacing Joyride */}
      <Dialog
        open={showTutorial && isOpen}
        onClose={handleClose}
        PaperProps={{
          sx: { 
            borderRadius: 2,
            maxWidth: 500,
            width: '100%'
          }
        }}
      >
        <DialogTitle>
          <Box display="flex" justifyContent="space-between" alignItems="center">
            <Typography variant="h6">{steps[activeStep].title}</Typography>
            <IconButton edge="end" onClick={handleClose} aria-label="close">
              <CloseIcon />
            </IconButton>
          </Box>
        </DialogTitle>
        <DialogContent>
          <Typography sx={{ mb: 2 }}>
            {steps[activeStep].content}
          </Typography>
        </DialogContent>
        <Box sx={{ px: 2, pb: 2 }}>
          <MobileStepper
            variant="dots"
            steps={steps.length}
            position="static"
            activeStep={activeStep}
            sx={{ flexGrow: 1, bgcolor: 'background.paper' }}
            nextButton={
              <Button 
                size="small" 
                onClick={handleNext}
                variant="contained"
                color="primary"
              >
                {activeStep === steps.length - 1 ? 'Finish' : 'Next'}
                <KeyboardArrowRight />
              </Button>
            }
            backButton={
              <Button 
                size="small" 
                onClick={handleBack} 
                disabled={activeStep === 0}
                variant="outlined"
              >
                <KeyboardArrowLeft />
                Back
              </Button>
            }
          />
        </Box>
      </Dialog>
    </>
  );
};

export default Tutorial; 
