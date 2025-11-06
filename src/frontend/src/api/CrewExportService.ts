import { apiClient as API } from '../config/api/ApiConfig';
import {
  CrewExportRequest,
  CrewExportResponse,
  DeploymentRequest,
  DeploymentResponse,
  DeploymentStatusResponse,
  ExportFormat
} from '../types/crewExport';

/**
 * Service for crew export and deployment operations
 */
export class CrewExportService {
  /**
   * Export crew to specified format (Python Project or Databricks Notebook)
   */
  static async exportCrew(
    crewId: string,
    request: CrewExportRequest
  ): Promise<CrewExportResponse> {
    try {
      const response = await API.post<CrewExportResponse>(
        `/crews/${crewId}/export`,
        request
      );
      return response.data;
    } catch (error) {
      console.error('Error exporting crew:', error);
      throw error;
    }
  }

  /**
   * Download exported crew as file
   * Returns a blob that can be used to trigger browser download
   */
  static async downloadExport(
    crewId: string,
    format: ExportFormat,
    options?: {
      include_custom_tools?: boolean;
      include_comments?: boolean;
      include_tracing?: boolean;
      include_evaluation?: boolean;
      include_deployment?: boolean;
      model_override?: string;
    }
  ): Promise<Blob> {
    try {
      const response = await API.get(
        `/crews/${crewId}/export/download`,
        {
          params: {
            format,
            ...(options || {})
          },
          responseType: 'blob'
        }
      );
      return response.data;
    } catch (error) {
      console.error('Error downloading export:', error);
      throw error;
    }
  }

  /**
   * Deploy crew to Databricks Model Serving endpoint
   */
  static async deployCrew(
    crewId: string,
    request: DeploymentRequest
  ): Promise<DeploymentResponse> {
    try {
      const response = await API.post<DeploymentResponse>(
        `/crews/${crewId}/deploy`,
        request
      );
      return response.data;
    } catch (error) {
      console.error('Error deploying crew:', error);
      throw error;
    }
  }

  /**
   * Get status of deployed endpoint
   */
  static async getDeploymentStatus(
    crewId: string,
    endpointName: string
  ): Promise<DeploymentStatusResponse> {
    try {
      const response = await API.get<DeploymentStatusResponse>(
        `/crews/${crewId}/deployment/status`,
        {
          params: { endpoint_name: endpointName }
        }
      );
      return response.data;
    } catch (error) {
      console.error('Error fetching deployment status:', error);
      throw error;
    }
  }

  /**
   * Delete Model Serving endpoint
   */
  static async deleteDeployment(
    crewId: string,
    endpointName: string
  ): Promise<{ message: string; endpoint_name: string }> {
    try {
      const response = await API.delete(
        `/crews/${crewId}/deployment/${endpointName}`
      );
      return response.data;
    } catch (error) {
      console.error('Error deleting deployment:', error);
      throw error;
    }
  }

  /**
   * Helper method to trigger browser download of a blob
   */
  static triggerDownload(blob: Blob, filename: string): void {
    const url = window.URL.createObjectURL(blob);
    const link = document.createElement('a');
    link.href = url;
    link.download = filename;
    document.body.appendChild(link);
    link.click();
    document.body.removeChild(link);
    window.URL.revokeObjectURL(url);
  }
}
