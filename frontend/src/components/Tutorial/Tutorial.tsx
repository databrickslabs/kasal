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
      title: '🤖 Meet Your Kasal Assistant',
      content: 'Welcome to Kasal! Your AI assistant is your main companion for everything. Click the Kasal chat icon to open your personal AI helper who will guide you through creating and managing AI crews.',
      target: 'button[data-tour="kasal-chat"]',
    },
    {
      title: '💬 Chat with Kasal',
      content: 'This is where the magic happens! Chat with Kasal to create agents, tasks, or entire crews. Try commands like: "Create agent: Marketing Specialist" or "Create task: Write blog posts" or "Create crew: Content marketing team". Kasal understands natural language and will build exactly what you need.',
      target: 'div[data-tour="chat-panel"]',
    },
    {
      title: '📚 Your Crew Catalog',
      content: 'Click "Open Workflow" to see your crew catalog. Initially empty, this becomes your team\'s library of reusable AI crews. Once you create useful templates, your entire team can load and deploy them instantly for similar projects.',
      target: 'button[data-tour="open-workflow"]',
    },
    {
      title: '🎬 Watch Your Team Work',
      content: 'Once you have a crew (from chat generation or catalog), hit "Execute Crew" to watch your AI team spring into action! You\'ll see real-time updates as agents collaborate and deliver results automatically.',
      target: 'button[data-tour="execute-crew"]',
    },
    {
      title: '👀 Monitor the Action',
      content: 'Your workflow appears on this canvas! Agents (WHO) have person icons - they\'re AI specialists like Marketing Expert or Data Analyst. Tasks (WHAT) have task icons - the actual work like "Write report" or "Analyze data". Watch connections light up as agents complete tasks and pass results. Remember: iterate to improve quality by adjusting tools, models, guardrails, and agent skills.',
      target: 'canvas[data-tour="workflow-canvas"]',
    },
    {
      title: '💾 Save Your Success',
      content: 'Found a crew that works perfectly? Save it to your personal catalog! Your saved crews become instant solutions for similar future projects. Build your library of AI specialists.',
      target: 'button[data-tour="save-crew"]',
    },
    {
      title: '🚀 Keep Chatting with Kasal',
      content: 'Remember: Kasal chat is your command center for everything. Ask questions, request modifications, get help, or create new crews. Your AI assistant is always ready to help you accomplish more with AI teams!',
      target: 'button[data-tour="kasal-chat"]',
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
        <DialogTitle>🚀 Welcome to Kasal!</DialogTitle>
        <DialogContent>
          <Typography sx={{ mb: 2 }}>
            Ready to build AI teams that work autonomously? Kasal makes it as easy as having a conversation.
          </Typography>
          <Typography variant="body2" color="text.secondary">
            Take a 2-minute tour to discover how to chat with AI, load proven crew templates, and watch autonomous agents collaborate in real-time.
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
