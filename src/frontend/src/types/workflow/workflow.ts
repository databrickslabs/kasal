// This file can import types used by the workflow functionality
// but should not import the useFlowManager hook as it creates a circular dependency

// Any specific workflow type definitions should be placed here
// For example, you might have interfaces for workflow node types, 
// edge types, configuration, etc.

// Previously this file contained Context-related interfaces, but
// as we've fully migrated to Zustand, those are no longer needed.

// Empty export to make this file a module
export {}; 