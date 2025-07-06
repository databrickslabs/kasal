import { EventEmitter } from 'events';
import { config } from '../config/api/ApiConfig';

export interface LogMessage {
  id?: number;
  job_id?: string;
  execution_id?: string;
  content?: string;
  output?: string;
  timestamp: string;
  type?: 'live' | 'historical';
}

export interface LogEntry {
  id?: number;
  output?: string;
  content?: string;
  timestamp: string;
  logType?: string;
}

// Add a type definition for the backend log response
interface BackendLogEntry {
  id?: number;
  content: string;
  timestamp: string;
}

class ExecutionLogService {
  private connections: { [key: string]: WebSocket } = {};
  private reconnectTimers: { [key: string]: NodeJS.Timeout } = {};
  private eventEmitter: EventEmitter;
  private apiUrl: string;
  private wsUrl: string;
  private isReconnecting: { [key: string]: boolean } = {};

  constructor() {
    this.eventEmitter = new EventEmitter();
    this.apiUrl = config.apiUrl;
    this.wsUrl = this.apiUrl.replace(/^http/, 'ws');
  }

  async getHistoricalLogs(jobId: string, limit = 1000, offset = 0): Promise<LogMessage[]> {
    try {
      const response = await fetch(`${this.apiUrl}/runs/${jobId}/outputs?limit=${limit}&offset=${offset}`);
      if (!response.ok) {
        if (response.status === 404) {
          console.warn(`No logs found for job ${jobId}`);
          return [];
        }
        throw new Error(`HTTP error! status: ${response.status}`);
      }
      const data = await response.json();
      
      const logs = data.logs || [];
      return logs.map((log: BackendLogEntry) => ({
        id: log.id || Date.now(),
        job_id: jobId,
        execution_id: jobId,
        output: log.content,
        content: log.content,
        timestamp: log.timestamp,
        type: 'historical'
      }));
    } catch (error) {
      console.error('Error fetching historical logs:', error);
      throw error;
    }
  }

  private calculateBackoffDelay(attempt: number): number {
    // Exponential backoff: 3s, 6s, 12s, 24s, 48s
    return Math.min(3000 * Math.pow(2, attempt), 48000);
  }

  connectToJobLogs(jobId: string): void {
    if (this.connections[jobId] || this.isReconnecting[jobId]) {
      return; // Prevent multiple connection attempts
    }

    try {
      // Get tenant email from localStorage for development mode
      const mockUserEmail = localStorage.getItem('mockUserEmail');
      const tenantParam = mockUserEmail ? `?tenant_email=${encodeURIComponent(mockUserEmail)}` : '';
      
      const ws = new WebSocket(`${this.wsUrl}/logs/executions/${jobId}/stream${tenantParam}`);
      let reconnectAttempts = 0;
      const maxReconnectAttempts = 5;

      ws.onopen = () => {
        console.log(`WebSocket connection established for job ${jobId}`);
        reconnectAttempts = 0;
        this.isReconnecting[jobId] = false;
        this.eventEmitter.emit(`connected-${jobId}`);
      };
      
      ws.onmessage = (event) => {
        try {
          const data = JSON.parse(event.data);
          
          const logMessage: LogMessage = {
            id: data.id || Date.now(),
            job_id: jobId,
            execution_id: data.execution_id || jobId,
            output: data.content,
            content: data.content,
            timestamp: data.timestamp || new Date().toISOString(),
            type: data.type || 'live'
          };
          
          this.eventEmitter.emit(`logs-${jobId}`, logMessage);
        } catch (error) {
          console.error('Error parsing WebSocket message:', error);
          this.eventEmitter.emit(`error-${jobId}`, new Error('Failed to parse message'));
        }
      };

      ws.onerror = (error) => {
        console.error(`WebSocket error for job ${jobId}:`, error);
        this.eventEmitter.emit(`error-${jobId}`, error);
      };

      ws.onclose = (event) => {
        console.log(`WebSocket closed for job ${jobId}:`, event.code, event.reason);
        this.eventEmitter.emit(`close-${jobId}`, event);
        delete this.connections[jobId];
        
        // Clear any existing reconnection timer
        if (this.reconnectTimers[jobId]) {
          clearTimeout(this.reconnectTimers[jobId]);
          delete this.reconnectTimers[jobId];
        }
        
        if (!event.wasClean && reconnectAttempts < maxReconnectAttempts && !this.isReconnecting[jobId]) {
          const delay = this.calculateBackoffDelay(reconnectAttempts);
          console.log(`Attempting to reconnect for job ${jobId} in ${delay}ms... (Attempt ${reconnectAttempts + 1}/${maxReconnectAttempts})`);
          
          this.isReconnecting[jobId] = true;
          this.reconnectTimers[jobId] = setTimeout(() => {
            reconnectAttempts++;
            this.connectToJobLogs(jobId);
          }, delay);
        } else if (reconnectAttempts >= maxReconnectAttempts) {
          console.log(`Max reconnection attempts reached for job ${jobId}`);
          delete this.isReconnecting[jobId];
          this.eventEmitter.emit(`error-${jobId}`, new Error('Failed to establish WebSocket connection after multiple attempts'));
        }
      };

      this.connections[jobId] = ws;
    } catch (error) {
      console.error('Failed to create WebSocket connection:', error);
      this.eventEmitter.emit(`error-${jobId}`, error);
      delete this.isReconnecting[jobId];
    }
  }

  disconnectFromJobLogs(jobId: string): void {
    // Clear any pending reconnection attempts
    if (this.reconnectTimers[jobId]) {
      clearTimeout(this.reconnectTimers[jobId]);
      delete this.reconnectTimers[jobId];
    }
    
    delete this.isReconnecting[jobId];
    
    const connection = this.connections[jobId];
    if (connection) {
      connection.close(1000, 'Client disconnected');
      delete this.connections[jobId];
    }
  }

  onConnected(jobId: string, callback: () => void): () => void {
    const eventName = `connected-${jobId}`;
    this.eventEmitter.on(eventName, callback);
    return () => {
      this.eventEmitter.off(eventName, callback);
    };
  }

  onJobLogs(jobId: string, callback: (message: LogMessage) => void): () => void {
    const eventName = `logs-${jobId}`;
    this.eventEmitter.on(eventName, callback);
    return () => {
      this.eventEmitter.off(eventName, callback);
    };
  }

  onError(jobId: string, callback: (error: Event | Error) => void): () => void {
    const eventName = `error-${jobId}`;
    this.eventEmitter.on(eventName, callback);
    return () => {
      this.eventEmitter.off(eventName, callback);
    };
  }

  onClose(jobId: string, callback: (event: CloseEvent) => void): () => void {
    const eventName = `close-${jobId}`;
    this.eventEmitter.on(eventName, callback);
    return () => {
      this.eventEmitter.off(eventName, callback);
    };
  }

  cleanup(): void {
    // Clear all reconnection timers
    Object.keys(this.reconnectTimers).forEach(jobId => {
      clearTimeout(this.reconnectTimers[jobId]);
      delete this.reconnectTimers[jobId];
    });
    
    // Clear reconnection flags
    this.isReconnecting = {};
    
    // Disconnect all WebSocket connections
    Object.keys(this.connections).forEach(jobId => {
      this.disconnectFromJobLogs(jobId);
    });
    
    this.eventEmitter.removeAllListeners();
  }
}

export const executionLogService = new ExecutionLogService();
export default executionLogService; 