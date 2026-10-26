/**
 * Global Shortcut Manager
 * Ensures only one instance of shortcuts is active at a time
 */

type ShortcutInstance = {
  id: string;
  priority: number;
  isActive: boolean;
};

class ShortcutManager {
  private static instance: ShortcutManager | null = null;
  private instances: Map<string, ShortcutInstance> = new Map();
  private activeInstanceId: string | null = null;

  private constructor() {
    // Singleton pattern - private constructor
  }

  static getInstance(): ShortcutManager {
    if (!ShortcutManager.instance) {
      ShortcutManager.instance = new ShortcutManager();
    }
    return ShortcutManager.instance;
  }

  /**
   * Register a shortcut instance
   * @param id Unique identifier for the instance
   * @param priority Higher priority instances take precedence (default 0)
   * @returns true if this instance should be active
   */
  register(id: string, priority = 0): boolean {
    const instance: ShortcutInstance = {
      id,
      priority,
      isActive: false
    };

    this.instances.set(id, instance);
    return this.updateActiveInstance();
  }

  /**
   * Unregister a shortcut instance
   * @param id Instance identifier
   */
  unregister(id: string): void {
    this.instances.delete(id);
    if (this.activeInstanceId === id) {
      this.activeInstanceId = null;
      this.updateActiveInstance();
    }
  }

  /**
   * Check if an instance is currently active
   * @param id Instance identifier
   * @returns true if the instance is active
   */
  isActive(id: string): boolean {
    return this.activeInstanceId === id;
  }

  /**
   * Request to become the active instance
   * @param id Instance identifier
   * @returns true if the instance is now active
   */
  requestActive(id: string): boolean {
    const instance = this.instances.get(id);
    if (!instance) return false;

    // Check if this instance has higher priority than current
    if (this.activeInstanceId) {
      const currentInstance = this.instances.get(this.activeInstanceId);
      if (currentInstance && currentInstance.priority > instance.priority) {
        return false;
      }
    }

    this.activeInstanceId = id;
    this.updateInstanceStates();
    return true;
  }

  /**
   * Update which instance should be active based on priority
   */
  private updateActiveInstance(): boolean {
    // Find the instance with highest priority
    let highestPriority = -1;
    let highestPriorityId: string | null = null;

    this.instances.forEach((instance, id) => {
      if (instance.priority > highestPriority) {
        highestPriority = instance.priority;
        highestPriorityId = id;
      }
    });

    const previousActive = this.activeInstanceId;
    this.activeInstanceId = highestPriorityId;
    this.updateInstanceStates();

    // Return true if the active instance changed
    return previousActive !== this.activeInstanceId;
  }

  /**
   * Update the isActive state for all instances
   */
  private updateInstanceStates(): void {
    this.instances.forEach((instance, id) => {
      instance.isActive = id === this.activeInstanceId;
    });
  }

  /**
   * Get debug information about registered instances
   */
  getDebugInfo(): { activeId: string | null; instances: Array<{ id: string; priority: number; isActive: boolean }> } {
    const instancesArray = Array.from(this.instances.entries()).map(([id, instance]) => ({
      id,
      priority: instance.priority,
      isActive: instance.isActive
    }));

    return {
      activeId: this.activeInstanceId,
      instances: instancesArray
    };
  }
}

export default ShortcutManager.getInstance();