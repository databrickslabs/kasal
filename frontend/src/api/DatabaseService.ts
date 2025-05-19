import apiClient from '../config/api/ApiConfig';
import { AxiosError } from 'axios';

export interface DatabaseStatus {
  exists: boolean;
  size: number;
  size_human: string;
  path: string;
  last_modified: string | null;
}

export class DatabaseService {
  private static instance: DatabaseService;
  private readonly baseUrl: string;

  private constructor() {
    this.baseUrl = `/db`;
  }

  public static getInstance(): DatabaseService {
    if (!DatabaseService.instance) {
      DatabaseService.instance = new DatabaseService();
    }
    return DatabaseService.instance;
  }

  /**
   * Get current database status
   */
  public async getDatabaseStatus(): Promise<DatabaseStatus> {
    try {
      const response = await apiClient.get<DatabaseStatus>(`${this.baseUrl}/status`);
      return response.data;
    } catch (error) {
      if (error instanceof AxiosError) {
        const errorMessage = error.response?.data?.detail || 'Failed to get database status';
        throw new Error(errorMessage);
      }
      throw new Error('Failed to connect to the server');
    }
  }

  /**
   * Export the database file
   */
  public async exportDatabase(): Promise<void> {
    try {
      // Use direct fetch for blob handling
      const response = await fetch(`${apiClient.defaults.baseURL}${this.baseUrl}/export`, {
        method: 'GET',
        headers: {
          'Accept': 'application/json',
        },
      });

      if (!response.ok) {
        const errorData = await response.json();
        throw new Error(errorData.detail || 'Failed to export database');
      }

      // Get blob from response
      const blob = await response.blob();
      
      // Create filename from Content-Disposition or default
      let filename = 'crewai_backup.db';
      const contentDisposition = response.headers.get('Content-Disposition');
      if (contentDisposition) {
        const filenameMatch = /filename="?([^"]*)"?/i.exec(contentDisposition);
        if (filenameMatch && filenameMatch[1]) {
          filename = filenameMatch[1];
        }
      }

      // Create object URL and trigger download
      const url = window.URL.createObjectURL(blob);
      const a = document.createElement('a');
      a.style.display = 'none';
      a.href = url;
      a.download = filename;
      document.body.appendChild(a);
      a.click();
      
      // Clean up
      window.URL.revokeObjectURL(url);
      document.body.removeChild(a);
    } catch (error) {
      if (error instanceof Error) {
        throw error;
      }
      throw new Error('Failed to export database');
    }
  }

  /**
   * Import a database file
   */
  public async importDatabase(file: File): Promise<{ message: string }> {
    try {
      const formData = new FormData();
      formData.append('file', file);

      const response = await fetch(`${apiClient.defaults.baseURL}${this.baseUrl}/import`, {
        method: 'POST',
        body: formData,
      });

      if (!response.ok) {
        const errorData = await response.json();
        throw new Error(errorData.detail || 'Failed to import database');
      }

      return response.json();
    } catch (error) {
      if (error instanceof Error) {
        throw error;
      }
      throw new Error('Failed to import database');
    }
  }
} 