# Tab-Crew Relationship Feature

## Overview

This feature establishes a relationship between workflow tabs and saved crews, providing better workflow management and preventing accidental loss of unsaved work.

## Key Features

### 1. **Tab State Tracking**
- Each tab now tracks whether it has unsaved changes (`isDirty` flag)
- Tabs display visual indicators:
  - Orange dot: Unsaved changes
  - Green checkmark: Saved crew (no unsaved changes)

### 2. **Save Confirmation Dialog**
When closing a tab with unsaved changes, users see a confirmation dialog with three options:
- **Save & Close**: Opens the save crew dialog, then closes the tab after saving
- **Discard Changes**: Closes the tab without saving
- **Cancel**: Keeps the tab open

### 3. **Automatic Tab Naming**
- When loading a crew, the tab name automatically updates to match the crew name
- When saving a crew, the tab name updates to the saved crew name

### 4. **Crew Metadata Storage**
Each tab stores:
- `savedCrewId`: The ID of the associated saved crew
- `savedCrewName`: The name of the saved crew
- `lastSavedAt`: Timestamp of the last save

### 5. **Context Menu Integration**
Right-click on any tab to:
- Save the crew for that specific tab
- Rename the tab
- Duplicate the tab (creates a new unsaved copy)
- Run the tab's workflow

## Implementation Details

### TabData Interface Extension
```typescript
export interface TabData {
  // ... existing fields ...
  savedCrewId?: string;
  savedCrewName?: string;
  lastSavedAt?: Date;
}
```

### New Store Methods
```typescript
// Update tab with crew information
updateTabCrewInfo(tabId: string, crewId: string, crewName: string): void

// Clear crew information from tab
clearTabCrewInfo(tabId: string): void

// Check if tab is saved
isTabSaved(tabId: string): boolean
```

### Event Flow

1. **When Saving a Crew:**
   - User triggers save (toolbar, right sidebar, or tab context menu)
   - SaveCrew dialog opens
   - After successful save, `saveCrewComplete` event is dispatched
   - Tab updates with crew info and name

2. **When Loading a Crew:**
   - User selects a crew from the dialog
   - Crew nodes and edges are loaded
   - Tab name and metadata are updated automatically

3. **When Closing a Tab:**
   - System checks if tab has unsaved changes
   - If unsaved, confirmation dialog appears
   - User chooses action (save, discard, or cancel)

## User Experience Benefits

1. **Prevents Accidental Data Loss**: Users are warned before closing tabs with unsaved work
2. **Clear Visual Feedback**: Users can see at a glance which tabs are saved
3. **Seamless Workflow**: Tab names automatically reflect the crew being worked on
4. **Flexible Saving**: Users can save from multiple entry points

## Usage Example

```typescript
// The tab automatically tracks changes
const { updateTabNodes } = useTabManagerStore();
updateTabNodes(tabId, newNodes); // Tab is marked as dirty

// When saving, the tab is updated
const savedCrew = await CrewService.saveCrew(crewData);
updateTabCrewInfo(activeTabId, savedCrew.id, savedCrew.name);

// When loading a crew
const crew = await CrewService.getCrew(crewId);
updateTabCrewInfo(activeTabId, crew.id, crew.name);
```

## Future Enhancements

1. **Auto-save**: Periodically save crews with unsaved changes
2. **Version History**: Track multiple versions of saved crews
3. **Collaborative Editing**: Show which users are editing which tabs
4. **Recovery**: Restore tabs from browser storage after crashes 