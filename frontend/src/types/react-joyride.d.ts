declare module 'react-joyride' {
  import { ReactNode } from 'react';

  export interface Step {
    target: string;
    content: ReactNode;
    disableBeacon?: boolean;
    placement?: 'top' | 'top-start' | 'top-end' | 'bottom' | 'bottom-start' | 'bottom-end' | 'left' | 'left-start' | 'left-end' | 'right' | 'right-start' | 'right-end' | 'auto' | 'center';
    title?: string;
  }

  export interface CallBackProps {
    action: string;
    controlled: boolean;
    index: number;
    lifecycle: string;
    size: number;
    status: 'running' | 'paused' | 'skipped' | 'finished' | 'error';
    step: Step;
    type: string;
  }

  export interface Props {
    steps: Step[];
    run: boolean;
    continuous?: boolean;
    showProgress?: boolean;
    showSkipButton?: boolean;
    callback?: (data: CallBackProps) => void;
    disableOverlayClose?: boolean;
    disableCloseOnEsc?: boolean;
    hideCloseButton?: boolean;
    styles?: {
      options?: {
        primaryColor?: string;
        zIndex?: number;
      };
    };
  }

  export default function Joyride(props: Props): JSX.Element;
} 