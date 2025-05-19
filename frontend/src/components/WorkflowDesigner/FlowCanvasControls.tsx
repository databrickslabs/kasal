import React from 'react';
import CanvasControls, { CanvasControlsProps } from './CanvasControls';

// This is essentially just the base CanvasControls, but having a separate component
// allows for future FlowCanvas-specific controls to be added
const FlowCanvasControls: React.FC<CanvasControlsProps> = (props) => {
  return <CanvasControls {...props} />;
};

export default FlowCanvasControls; 