export interface TaskCallbackOption {
  value: string;
  label: string;
  description: string;
  requiresPath?: boolean;
}

export const TASK_CALLBACKS: TaskCallbackOption[] = [
  // Logging
  {
    value: 'DetailedOutputLogger',
    label: 'Detailed Output Logger',
    description: 'Logs detailed task output and execution metrics'
  },
  
  // Validation
  {
    value: 'SchemaValidator',
    label: 'Schema Validator',
    description: 'Validates output against a JSON schema'
  },
  {
    value: 'ContentValidator',
    label: 'Content Validator',
    description: 'Validates output content against specified rules'
  },
  {
    value: 'TypeValidator',
    label: 'Type Validator',
    description: 'Validates output data types'
  },
  
  // Storage
  {
    value: 'JsonFileStorage',
    label: 'JSON File Storage',
    description: 'Stores output in a JSON file',
    requiresPath: true
  },
  {
    value: 'DatabaseStorage',
    label: 'Database Storage',
    description: 'Stores output in a database'
  },
  {
    value: 'FileSystemStorage',
    label: 'File System Storage',
    description: 'Stores output in the file system',
    requiresPath: true
  },
  
  // Transformation
  {
    value: 'OutputFormatter',
    label: 'Output Formatter',
    description: 'Formats output according to specified rules'
  },
  {
    value: 'DataExtractor',
    label: 'Data Extractor',
    description: 'Extracts specific data from the output'
  },
  {
    value: 'OutputEnricher',
    label: 'Output Enricher',
    description: 'Enriches output with additional data'
  },
  {
    value: 'OutputSummarizer',
    label: 'Output Summarizer',
    description: 'Creates a summary of the output'
  }
]; 