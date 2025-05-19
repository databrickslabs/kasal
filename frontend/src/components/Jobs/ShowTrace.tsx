import React, { useState, useEffect, useCallback } from 'react';
import {
  Dialog,
  DialogTitle,
  DialogContent,
  IconButton,
  Typography,
  Box,
  Stepper,
  Step,
  StepLabel,
  StepContent,
  Paper,
  CircularProgress,
  Theme,
  Button,
} from '@mui/material';
import CloseIcon from '@mui/icons-material/Close';
import ReactMarkdown from 'react-markdown';
import remarkGfm from 'remark-gfm';
import type { Components } from 'react-markdown';
import { SxProps } from '@mui/system';
import { ShowTraceProps, Trace, TaskDetails } from '../../types/trace';
import { MarkdownProps } from '../../types/markdown';
import TraceService from '../../api/TraceService';
import { useTranslation } from 'react-i18next';

// Define a better type for Axios errors
interface AxiosError {
  response?: {
    status: number;
    statusText: string;
    data?: unknown;
  };
  message?: string;
}

const ShowTrace: React.FC<ShowTraceProps> = ({ open, onClose, runId }) => {
  const { t } = useTranslation();
  const [traces, setTraces] = useState<Trace[]>([]);
  const [taskDetails, setTaskDetails] = useState<Record<string, TaskDetails>>({});
  const [loading, setLoading] = useState<boolean>(true);
  const [error, setError] = useState<string | null>(null);
  const [isRetrying, setIsRetrying] = useState<boolean>(false);

  const fetchTraceData = useCallback(async () => {
    if (!runId) return;
    
    try {
      setLoading(true);
      setIsRetrying(false);

      // Check if runId is in UUID format (has dashes)
      const _isUuid = typeof runId === 'string' && runId.includes('-');
      
      // First check if the run exists to avoid 500 errors
      const runExists = await TraceService.checkRunExists(runId);
      if (!runExists) {
        setError(t('trace.error.runNotExists', { runId }) || `Run ID ${runId} does not exist or is no longer available.`);
        setLoading(false);
        return;
      }

      // Fetch run details
      const runData = await TraceService.getRunDetails(runId);
      
      // Define a new interface for run data to type the response properly
      interface RunData {
        task_id?: string;
        job_id?: string;
        [key: string]: unknown;
      }

      // Type cast the run data to the interface
      const typedRunData = runData as RunData;
      
      // Get task ID from run data, ensuring it's a string
      const taskId = typedRunData.task_id || '';
      
      // Use job_id for trace retrieval if available and in UUID format
      const traceId = (typedRunData.job_id && typedRunData.job_id.includes('-')) 
                      ? typedRunData.job_id 
                      : runId;

      if (taskId) {
        try {
          // Fetch task details and name with the properly typed taskId string
          const [taskData, taskNameData] = await Promise.all([
            TraceService.getTaskDetails(taskId),
            TraceService.getTaskName(taskId)
          ]);
          
          if (taskData && taskNameData) {
            // Use the string taskId as a key in the object
            setTaskDetails(prevDetails => ({
              ...prevDetails,
              [taskId]: {
                ...taskData,
                name: taskNameData.name
              }
            }));
          }
        } catch (taskErr) {
          // Continue anyway - we can show traces without task details
        }
      }

      try {
        // Fetch traces using the appropriate ID (job_id if available from run details)
        const traces = await TraceService.getTraces(traceId);
        
        if (!traces || !Array.isArray(traces) || traces.length === 0) {
          setError(t('trace.error.noTraces') || 'No trace data is available for this run.');
          setTraces([]);
        } else {
          // Make sure traces is an array before sorting
          // Sort traces by creation date
          const sortedTraces = [...traces].sort((a: Trace, b: Trace) => {
            // Safely handle missing dates by using current time as fallback
            const dateA = a.created_at ? new Date(a.created_at).getTime() : Date.now();
            const dateB = b.created_at ? new Date(b.created_at).getTime() : Date.now();
            return dateA - dateB;
          });

          // Add task_id to each trace from the run data
          const tracesWithTaskId = sortedTraces.map(trace => ({
            ...trace,
            task_id: taskId as string
          }));

          setTraces(tracesWithTaskId as Trace[]);
          setError(null);
        }
      } catch (traceErr: unknown) {
        // Type guard to check for response property
        const isAxiosError = (err: unknown): err is AxiosError => 
          err !== null && typeof err === 'object' && 'response' in err;
        
        console.error("ShowTrace: Error fetching traces:", traceErr);
        
        // Handle 404 not found errors specifically
        if (isAxiosError(traceErr) && traceErr.response && traceErr.response.status === 404) {
          setError(t('trace.error.tracesNotFound', { runId }) || `No traces found for Run ID ${runId}.`);
        } else {
          const errorMessage = traceErr instanceof Error ? traceErr.message : t('trace.error.unknown') || 'Unknown error';
          setError(`${t('trace.error.failedToLoad') || 'Failed to load traces'}: ${errorMessage}`);
        }
        
        // We can still continue with an empty trace list
        setTraces([]);
      }
    } catch (err) {
      const errorMessage = err instanceof Error ? err.message : t('trace.error.unknown');
      console.error("ShowTrace: Error in fetchTraceData:", err);
      
      // Check for axios error with status code
      if (err && typeof err === 'object' && 'response' in err && err.response) {
        const axiosError = err as AxiosError;
        if (axiosError.response?.status) {
          if (axiosError.response.status === 500) {
            setError(t('trace.error.serverError', { runId }));
          } else if (axiosError.response.status === 404) {
            setError(t('trace.error.notFound', { runId }));
          } else {
            setError(`${t('trace.error.failedToLoad')}: ${t('trace.error.statusCode', { code: axiosError.response.status })}`);
          }
        } else {
          setError(`${t('trace.error.failedToLoad')}: ${errorMessage}`);
        }
      } else {
        setError(`${t('trace.error.failedToLoad')}: ${errorMessage}`);
      }
    } finally {
      setLoading(false);
    }
  }, [runId, t]);

  // Handle retrying the trace load
  const handleRetry = () => {
    setIsRetrying(true);
    fetchTraceData();
  };

  useEffect(() => {
    if (open) {
      // Store a reference to know if the component is still mounted and visible
      let isMounted = true;
      
      // Only fetch if we don't already have data or if we're retrying
      if ((!traces.length || isRetrying) && isMounted) {
        fetchTraceData();
      }
      
      return () => {
        isMounted = false;
      };
    }
  }, [open, fetchTraceData, traces.length, isRetrying]);

  const getTaskName = (trace: Trace): string => {
    if (trace.task_id && taskDetails[trace.task_id] && taskDetails[trace.task_id].name) {
      return `Task: ${taskDetails[trace.task_id].name}`;
    }
    return `Task: ${trace.task_name}`;
  };

  const MarkdownParagraph = React.forwardRef<HTMLParagraphElement, MarkdownProps>((props, ref) => (
    <Typography
      ref={ref}
      variant="body2"
      component="p"
      sx={{ mb: 1 }}
      {...props}
    />
  ));
  MarkdownParagraph.displayName = 'MarkdownParagraph';

  const MarkdownPreBlock = React.forwardRef<HTMLPreElement, MarkdownProps>((props, ref) => (
    <Box
      ref={ref}
      sx={{
        bgcolor: 'background.paper',
        p: 1,
        borderRadius: 1,
        overflow: 'auto'
      }}
      {...props}
    />
  ));
  MarkdownPreBlock.displayName = 'MarkdownPreBlock';

  const MarkdownCode = React.forwardRef<HTMLElement, MarkdownProps & { inline?: boolean }>((props, ref) => {
    const { inline, ...rest } = props;
    const sx: SxProps = inline ? {
      bgcolor: 'background.paper',
      p: 0.5,
      borderRadius: 0.5
    } : {
      bgcolor: 'background.paper',
      p: 1,
      borderRadius: 1,
      display: 'block'
    };
    return (
      <Typography
        ref={ref}
        component="code"
        sx={sx}
        {...rest}
      />
    );
  });
  MarkdownCode.displayName = 'MarkdownCode';

  const MarkdownH1 = React.forwardRef<HTMLHeadingElement, MarkdownProps>((props, ref) => (
    <Typography
      ref={ref}
      variant="h1"
      component="h1"
      sx={{ mb: 1 }}
      {...props}
    />
  ));
  MarkdownH1.displayName = 'MarkdownH1';

  const MarkdownH2 = React.forwardRef<HTMLHeadingElement, MarkdownProps>((props, ref) => (
    <Typography
      ref={ref}
      variant="h2"
      component="h2"
      sx={{ mb: 1 }}
      {...props}
    />
  ));
  MarkdownH2.displayName = 'MarkdownH2';

  const MarkdownH3 = React.forwardRef<HTMLHeadingElement, MarkdownProps>((props, ref) => (
    <Typography
      ref={ref}
      variant="h3"
      component="h3"
      sx={{ mb: 1 }}
      {...props}
    />
  ));
  MarkdownH3.displayName = 'MarkdownH3';

  const MarkdownH4 = React.forwardRef<HTMLHeadingElement, MarkdownProps>((props, ref) => (
    <Typography
      ref={ref}
      variant="h4"
      component="h4"
      sx={{ mb: 1 }}
      {...props}
    />
  ));
  MarkdownH4.displayName = 'MarkdownH4';

  const markdownComponents = {
    p: MarkdownParagraph,
    pre: MarkdownPreBlock,
    code: MarkdownCode,
    h1: MarkdownH1,
    h2: MarkdownH2,
    h3: MarkdownH3,
    h4: MarkdownH4,
  } as unknown as Components;

  // Display trace output based on its type
  const renderTraceOutput = (trace: Trace) => {
    // Display nothing if no output
    if (!trace.output) {
      return <Typography variant="body2">{t('trace.noOutput')}</Typography>;
    }

    // Handle string output (common for agent outputs)
    if (typeof trace.output === 'string') {
      return (
        <Box sx={{ mt: 1 }}>
          <ReactMarkdown components={markdownComponents} remarkPlugins={[remarkGfm]}>
            {trace.output}
          </ReactMarkdown>
        </Box>
      );
    }
    
    // Handle object output (common for API responses and structured data)
    // This could be a complex object or the new format with additional metadata
    if (typeof trace.output === 'object') {
      // Check for the most common output patterns based on your data
      let contentToRender = '';
      
      // Look for content in various possible locations based on the trace structure
      
      // 1. Special case for the data format with agent_execution field (from the example data)
      if (trace.output.agent_execution && typeof trace.output.agent_execution === 'string') {
        contentToRender = trace.output.agent_execution;
      }
      // 2. If the output has a content field, use that (typical for newer format)
      else if (trace.output.content && typeof trace.output.content === 'string') {
        contentToRender = trace.output.content;
      } 
      // 3. Try to find the actual response content in common patterns
      else if (trace.output.response && typeof trace.output.response === 'string') {
        contentToRender = trace.output.response;
      }
      // 4. Check for text field (sometimes used for agent outputs)
      else if (trace.output.text && typeof trace.output.text === 'string') {
        contentToRender = trace.output.text;
      }
      // 5. Check for raw_output field
      else if (trace.output.raw_output && typeof trace.output.raw_output === 'string') {
        contentToRender = trace.output.raw_output;
      }
      // 6. Default case - stringify the object
      else {
        try {
          // Beautify the JSON output
          contentToRender = "```json\n" + JSON.stringify(trace.output, null, 2) + "\n```";
        } catch (e) {
          contentToRender = t('trace.error.cannotParse') || 'Cannot parse output data';
        }
      }
      
      return (
        <Box sx={{ mt: 1 }}>
          <ReactMarkdown components={markdownComponents} remarkPlugins={[remarkGfm]}>
            {contentToRender}
          </ReactMarkdown>
        </Box>
      );
    }
    
    // Fallback for unknown types
    return (
      <Typography variant="body2">
        {t('trace.unknownOutputType')}
      </Typography>
    );
  };

  if (!open) return null;

  return (
    <Dialog 
      open={open} 
      onClose={onClose}
      maxWidth="md"
      fullWidth
      PaperProps={{
        sx: {
          minHeight: '70vh',
          maxHeight: '90vh'
        }
      }}
    >
      <DialogTitle sx={{ m: 0, p: 2, display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
        <Typography variant="h6">{t('trace.title')}</Typography>
        <IconButton
          aria-label={t('common.close')}
          onClick={onClose}
          sx={{
            position: 'absolute',
            right: 8,
            top: 8,
            color: (theme: Theme) => theme.palette.grey[500],
          }}
        >
          <CloseIcon />
        </IconButton>
      </DialogTitle>

      <DialogContent dividers>
        {loading ? (
          <Box display="flex" justifyContent="center" alignItems="center" minHeight="200px">
            <CircularProgress />
          </Box>
        ) : error ? (
          <Box sx={{ p: 2, display: 'flex', flexDirection: 'column', alignItems: 'center', gap: 2 }}>
            <Typography color="error" sx={{ textAlign: 'center' }}>
              {error}
            </Typography>
            <Button 
              variant="contained" 
              color="primary" 
              onClick={handleRetry}
              disabled={isRetrying}
              startIcon={isRetrying ? <CircularProgress size={20} color="inherit" /> : null}
            >
              {isRetrying ? t('trace.retrying') : t('trace.retry')}
            </Button>
          </Box>
        ) : traces.length === 0 ? (
          <Typography sx={{ p: 2 }}>
            {t('trace.noTraces')}
          </Typography>
        ) : (
          <Stepper orientation="vertical">
            {traces.map((trace: Trace, index: number) => (
              <Step key={index} active={true}>
                <StepLabel
                  StepIconProps={{
                    sx: {
                      '& .MuiStepIcon-root': {
                        color: 'primary.main',
                      }
                    }
                  }}
                >
                  <Box sx={{ display: 'flex', flexDirection: 'column', gap: 0.5 }}>
                    <Typography variant="subtitle1" color="primary">
                      {trace.agent_name}
                    </Typography>
                    <Typography variant="caption" color="text.secondary">
                      {getTaskName(trace)}
                    </Typography>
                    <Typography variant="caption" color="text.secondary">
                      {new Date(trace.created_at).toLocaleString()}
                    </Typography>
                  </Box>
                </StepLabel>
                <StepContent>
                  <Paper 
                    elevation={0}
                    sx={{ 
                      p: 2, 
                      bgcolor: 'grey.50',
                      borderRadius: 1,
                      maxHeight: '300px',
                      overflow: 'auto'
                    }}
                  >
                    {renderTraceOutput(trace)}
                  </Paper>
                </StepContent>
              </Step>
            ))}
          </Stepper>
        )}
      </DialogContent>
    </Dialog>
  );
};

export default ShowTrace; 