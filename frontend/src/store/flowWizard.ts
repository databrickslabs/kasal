import { create } from 'zustand';
import { CrewResponse } from '../types/crews';
import { CrewTask } from '../types/crewPlan';
import { Action, Listener, StartingPoint as _FlowStartingPoint, WizardStep, FlowEdgeFormData } from '../types/flow';
import { CrewService } from '../api/CrewService';

interface FlowWizardState {
  activeStep: WizardStep;
  crews: CrewResponse[];
  selectedCrewIds: string[];
  listeners: Listener[];
  tasks: CrewTask[];
  actions: Action[];
  startingPoints: StartingPoint[];
  flowName: string;
  loading: boolean;
  error: string | null;
  currentListenerIndex: number;
  selectedListenerTasks: string[];
  selectedListenToTasks: string[];
  selectedActionTasks: { [key: string]: string[] };

  // Actions
  setActiveStep: (step: WizardStep) => void;
  setCrews: (crews: CrewResponse[]) => void;
  setSelectedCrewIds: (ids: string[]) => void;
  setListeners: (listeners: Listener[]) => void;
  setTasks: (tasks: CrewTask[]) => void;
  setActions: (actions: Action[]) => void;
  setStartingPoints: (points: StartingPoint[]) => void;
  setFlowName: (name: string) => void;
  setLoading: (loading: boolean) => void;
  setError: (error: string | null) => void;
  setCurrentListenerIndex: (index: number) => void;
  setSelectedListenerTasks: (tasks: string[]) => void;
  setSelectedListenToTasks: (tasks: string[]) => void;
  setSelectedActionTasks: (tasks: { [key: string]: string[] }) => void;
  resetFlowWizard: () => void;

  // Additional functionality
  loadCrews: () => Promise<void>;
  toggleCrewSelection: (crewId: string) => void;
  handleNext: () => void;
  handleBack: () => void;
  handleListenerTaskChange: (taskIds: string[]) => void;
  handleListenToTaskChange: (taskIds: string[]) => void;
  handleStateUpdate: (formData: FlowEdgeFormData) => void;
  handleActionTaskChange: (crewId: string, taskIds: string[]) => void;
  handleDeleteAction: (taskId: string) => void;
  handleToggleStartingPoint: (taskId: string) => void;
  addListener: () => void;
  deleteListener: (id: string) => void;
  updateListenerName: (index: number, name: string) => void;
  updateListenerConditionType: (index: number, conditionType: 'NONE' | 'AND' | 'OR' | 'ROUTER') => void;
  updateRouterConfig: (index: number, defaultRoute: string, routes: Array<{name: string; condition: string; taskIds: string[]}>) => void;
}

interface StartingPoint {
  crewId: string;
  taskId: string;
  isStartPoint: boolean;
  taskName: string;
  crewName: string;
}

const initialState = {
  activeStep: WizardStep.SelectCrews,
  crews: [],
  selectedCrewIds: [],
  listeners: [],
  tasks: [],
  actions: [],
  startingPoints: [],
  flowName: 'New Flow',
  loading: false,
  error: null,
  currentListenerIndex: -1,
  selectedListenerTasks: [],
  selectedListenToTasks: [],
  selectedActionTasks: {},
};

export const useFlowWizardStore = create<FlowWizardState>((set, get) => ({
  ...initialState,

  // Basic actions
  setActiveStep: (step) => set({ activeStep: step }),
  setCrews: (crews) => set({ crews }),
  setSelectedCrewIds: (ids) => set({ selectedCrewIds: ids }),
  setListeners: (listeners) => set({ listeners }),
  setTasks: (tasks) => set({ tasks }),
  setActions: (actions) => set({ actions }),
  setStartingPoints: (points) => set({ startingPoints: points }),
  setFlowName: (name) => set({ flowName: name }),
  setLoading: (loading) => set({ loading }),
  setError: (error) => set({ error }),
  setCurrentListenerIndex: (index) => set({ currentListenerIndex: index }),
  setSelectedListenerTasks: (tasks) => set({ selectedListenerTasks: tasks }),
  setSelectedListenToTasks: (tasks) => set({ selectedListenToTasks: tasks }),
  setSelectedActionTasks: (tasks) => set({ selectedActionTasks: tasks }),
  resetFlowWizard: () => {
    set({
      activeStep: WizardStep.SelectCrews,
      crews: [],
      selectedCrewIds: [],
      listeners: [],
      tasks: [],
      actions: [],
      startingPoints: [],
      flowName: '',
      loading: false,
      error: null,
      currentListenerIndex: -1,
      selectedListenerTasks: [],
      selectedListenToTasks: [],
      selectedActionTasks: {}
    });
  },

  // Additional functionality
  loadCrews: async () => {
    try {
      set({ loading: true, error: null });
      const crewData = await CrewService.getCrews();
      set({ crews: crewData });

      // Load tasks for all crews
      const allTasks: CrewTask[] = [];
      for (const crew of crewData) {
        try {
          const crewTasks = await CrewService.getTasks(crew.id);
          allTasks.push(...crewTasks);
        } catch (error) {
          console.error(`Error loading tasks for crew ${crew.id}:`, error);
        }
      }
      set({ tasks: allTasks });
    } catch (error) {
      console.error('Error loading crews:', error);
      set({ error: 'Failed to load crews. Please try again.' });
    } finally {
      set({ loading: false });
    }
  },

  toggleCrewSelection: (crewId) => {
    const { selectedCrewIds } = get();
    const newSelectedCrewIds = selectedCrewIds.includes(crewId)
      ? selectedCrewIds.filter(id => id !== crewId)
      : [...selectedCrewIds, crewId];
    set({ selectedCrewIds: newSelectedCrewIds });
  },

  handleNext: () => {
    const { activeStep, selectedCrewIds, startingPoints, crews, tasks } = get();

    if (activeStep === WizardStep.SelectCrews) {
      if (selectedCrewIds.length === 0) {
        set({ error: 'Please select at least one crew' });
        return;
      }

      // Get tasks for selected crews
      const crewTasks = tasks.filter(task => 
        selectedCrewIds.includes(task.agent_id)
      );

      // Prepare starting points
      const availableStartingPoints = crewTasks.map(task => {
        const crew = crews.find(c => c.id === task.agent_id);
        return {
          crewId: task.agent_id,
          taskId: task.id,
          isStartPoint: false,
          taskName: task.name,
          crewName: crew?.name || 'Unknown Crew'
        };
      });

      set({ startingPoints: availableStartingPoints, activeStep: WizardStep.DefineStartingPoints });
    } else if (activeStep === WizardStep.DefineStartingPoints) {
      if (!startingPoints.some(point => point.isStartPoint)) {
        set({ error: 'Please select at least one task as a starting point' });
        return;
      }
      set({ activeStep: WizardStep.ConfigureListeners });
    } else if (activeStep === WizardStep.ConfigureListeners) {
      set({ activeStep: WizardStep.ConfigureState });
    } else if (activeStep === WizardStep.ConfigureState) {
      set({ activeStep: WizardStep.Review });
    }
  },

  handleBack: () => {
    const { activeStep } = get();
    if (activeStep > 0) {
      set({ activeStep: activeStep - 1 });
    }
  },

  handleListenerTaskChange: (taskIds) => {
    const { currentListenerIndex, listeners } = get();
    if (currentListenerIndex >= 0) {
      const updatedListeners = [...listeners];
      const selectedTasks = taskIds.map(id => {
        const task = get().tasks.find(t => t.id === id);
        return task || { 
          id, 
          name: id, 
          agent_id: '', 
          expected_output: '', 
          description: '',
          context: [] 
        };
      });
      updatedListeners[currentListenerIndex] = {
        ...updatedListeners[currentListenerIndex],
        tasks: selectedTasks
      };
      set({ 
        listeners: updatedListeners,
        selectedListenerTasks: taskIds
      });
    }
  },

  handleListenToTaskChange: (taskIds) => {
    const { currentListenerIndex, listeners } = get();
    if (currentListenerIndex >= 0) {
      const updatedListeners = [...listeners];
      updatedListeners[currentListenerIndex] = {
        ...updatedListeners[currentListenerIndex],
        listenToTaskIds: taskIds,
        listenToTaskNames: taskIds.map(id => {
          const task = get().tasks.find(t => t.id === id);
          return task?.name || id;
        })
      };
      set({ listeners: updatedListeners });
    }
  },

  handleStateUpdate: (formData) => {
    const { currentListenerIndex, listeners } = get();
    if (currentListenerIndex >= 0) {
      const updatedListeners = [...listeners];
      updatedListeners[currentListenerIndex] = {
        ...updatedListeners[currentListenerIndex],
        state: formData
      };
      set({ listeners: updatedListeners });
    }
  },

  handleActionTaskChange: (crewId, taskIds) => {
    set(state => ({
      selectedActionTasks: {
        ...state.selectedActionTasks,
        [crewId]: taskIds
      }
    }));
  },

  handleDeleteAction: (taskId) => {
    const updatedActions = get().actions.filter(action => {
      const task = get().tasks.find(t => t.id === taskId);
      return task && task.id !== taskId;
    });
    set({ actions: updatedActions });
  },

  handleToggleStartingPoint: (taskId) => {
    const { startingPoints } = get();
    const updatedStartingPoints = startingPoints.map(point => 
      point.taskId === taskId ? { ...point, isStartPoint: !point.isStartPoint } : point
    );
    set({ startingPoints: updatedStartingPoints });
  },

  addListener: () => {
    const { listeners, crews } = get();
    const selectedCrew = crews[0];
    const newListener: Listener = {
      id: `listener-${Date.now()}-${listeners.length}`,
      name: `Listener ${listeners.length + 1}`,
      crewId: selectedCrew?.id || '',
      crewName: selectedCrew?.name || 'Unknown Crew',
      listenToTaskIds: [],
      listenToTaskNames: [],
      tasks: [],
      waitForAll: false,
      state: {
        stateType: 'unstructured',
        stateDefinition: '',
        stateData: {}
      },
      conditionType: 'NONE'
    };
    set({ 
      listeners: [...listeners, newListener],
      currentListenerIndex: listeners.length,
      selectedListenerTasks: []
    });
  },

  deleteListener: (id) => {
    const { listeners, currentListenerIndex } = get();
    const updatedListeners = listeners.filter(l => l.id !== id);
    set({ 
      listeners: updatedListeners,
      currentListenerIndex: currentListenerIndex >= updatedListeners.length 
        ? updatedListeners.length - 1 
        : currentListenerIndex
    });
  },

  updateListenerName: (index, name) => {
    const { listeners } = get();
    const updatedListeners = [...listeners];
    updatedListeners[index] = {
      ...updatedListeners[index],
      name
    };
    set({ listeners: updatedListeners });
  },

  updateListenerConditionType: (index, conditionType) => {
    const { listeners } = get();
    if (index >= 0 && index < listeners.length) {
      const updatedListeners = [...listeners];
      updatedListeners[index] = {
        ...updatedListeners[index],
        conditionType
      };
      set({ listeners: updatedListeners });
    }
  },

  updateRouterConfig: (index, defaultRoute, routes) => {
    const { listeners } = get();
    if (index >= 0 && index < listeners.length) {
      const updatedListeners = [...listeners];
      updatedListeners[index] = {
        ...updatedListeners[index],
        routerConfig: {
          defaultRoute,
          routes
        }
      };
      set({ listeners: updatedListeners });
    }
  }
})); 