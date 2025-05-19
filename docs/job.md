# Jobs Component Documentation

This document provides an overview of the Jobs components in the CrewAI Manager frontend application.

## Overview

The Jobs components are responsible for managing, displaying, and analyzing job executions within the CrewAI system. These components handle run history, execution logs, trace visualization, result presentation, and job status monitoring.

## Components Structure

### RunHistory

`RunHistory.tsx` is the primary component for displaying the history of workflow runs.

**Key Features:**
- Tabular view of all past workflow executions
- Sorting and filtering capabilities
- Pagination for large result sets
- Job management actions (view, delete, schedule)
- Integration with other job components (logs, trace, results)
- Real-time status updates through polling
- User activity tracking to optimize polling

### ShowLogs

`ShowLogs.tsx` displays the execution logs for a specific job.

**Key Features:**
- Real-time log streaming
- Historical log viewing
- Auto-scroll functionality
- Log filtering
- Pagination through infinite scrolling
- Manual refresh option

### ShowResult

`ShowResult.tsx` displays the results of a completed job execution.

**Key Features:**
- Formatted display of execution results
- Markdown rendering with proper styling
- Link detection and formatting
- Support for various result types (text, objects, arrays)
- Tabbed interface for multiple result sections

### ShowTrace

`ShowTrace.tsx` visualizes the execution trace of a job.

**Key Features:**
- Step-by-step visualization of job execution
- Timeline representation
- Detailed information for each trace step
- Error handling and retry capabilities
- Markdown rendering for trace content

### JobStatusIndicator

`JobStatusIndicator.tsx` provides a real-time status indicator for running jobs.

**Key Features:**
- Real-time status updates
- Error reporting
- Auto-close on job completion
- Visual feedback on job status

### LLMLogs

`LLMLogs.tsx` displays logs specific to LLM (Language Learning Model) interactions.

**Key Features:**
- Detailed view of model API calls
- Performance metrics (token usage, duration)
- Expandable details for each log entry
- Filtering by endpoint
- Pagination
- Auto-refresh functionality

### RunActions

`RunActions.tsx` provides action buttons for job management.

**Key Features:**
- View result action
- Download PDF report
- View inputs (load workflow)
- View execution trace
- View logs
- Schedule job
- Delete job

### RunDialogs

`RunDialogs.tsx` contains dialog components for job-related actions.

**Key Features:**
- Delete confirmation dialog
- Job scheduling dialog with cron expression support
- Delete run confirmation dialog

## Data Flow

1. The `RunHistory` component displays a list of all job executions
2. Users can interact with each job through `RunActions` buttons
3. Detail views (`ShowLogs`, `ShowResult`, `ShowTrace`) display specific aspects of a job execution
4. Status updates are shown through `JobStatusIndicator`
5. Dialog components in `RunDialogs` handle confirmations for destructive actions

## Key Interfaces

The Jobs components use several key interfaces:

- **Run**: Represents a job execution with properties like status, duration, results
- **LogEntry**: Represents a log message from the job execution
- **LLMLog**: Represents a log entry specific to LLM API calls
- **Trace**: Represents an execution trace step
- **ResultValue**: Represents a job execution result value

## Services Integration

The Jobs components integrate with several services:

- **RunService**: For CRUD operations on runs and job management
- **LogService**: For retrieving and streaming logs
- **TraceService**: For retrieving execution traces
- **ScheduleService**: For scheduling recurring jobs

## Real-time Features

The Jobs components implement several real-time features:

- **Status polling**: Regular checks for job status updates
- **Log streaming**: Real-time display of logs during execution
- **User activity detection**: Optimization of polling based on user activity

## PDF Generation

Job results can be exported as PDF reports using the `generateRunPDF` utility function, which is accessible through the `RunActions` component.

## Best Practices

When working with the Jobs components:

1. Use proper error handling to provide useful feedback to users
2. Implement pagination for large datasets to improve performance
3. Provide sorting and filtering options for better user experience
4. Optimize real-time updates to balance freshness and performance
5. Use proper formatting for different types of content (logs, results, traces)
6. Implement responsive designs that work well on different screen sizes
7. Use internationalization (i18n) for all user-facing text
8. Maintain consistent styling with the rest of the application using MUI components 