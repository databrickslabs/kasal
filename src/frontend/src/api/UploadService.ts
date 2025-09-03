import { apiClient } from '../config/api/ApiConfig';

export interface UploadedFileInfo {
  filename: string;
  path: string;
  full_path: string;
  file_size_bytes?: number;
  is_uploaded: boolean;
  exists?: boolean;
  success?: boolean;
}

export interface MultiUploadResponse {
  files: UploadedFileInfo[];
  success: boolean;
}

/**
 * Service for handling file uploads
 */
export class UploadService {
  /**
   * Upload a knowledge source file
   * @param file The file to upload
   * @returns Object containing file information
   */
  async uploadKnowledgeFile(file: File): Promise<UploadedFileInfo> {
    const formData = new FormData();
    formData.append('file', file);

    const response = await apiClient.post<UploadedFileInfo>('/upload/knowledge', formData, {
      headers: {
        'Content-Type': 'multipart/form-data',
      },
    });

    return response.data;
  }

  /**
   * Upload multiple knowledge source files
   * @param files Array of files to upload
   * @returns Object containing information about all uploaded files
   */
  async uploadMultipleKnowledgeFiles(files: File[]): Promise<MultiUploadResponse> {
    const formData = new FormData();
    files.forEach(file => {
      formData.append('files', file);
    });

    const response = await apiClient.post<MultiUploadResponse>('/upload/knowledge/multi', formData, {
      headers: {
        'Content-Type': 'multipart/form-data',
      },
    });

    return response.data;
  }
  
  /**
   * Check if a knowledge file exists and get its metadata
   * @param filename Name of the file to check
   * @returns Object containing file information if it exists
   */
  async checkKnowledgeFile(filename: string): Promise<UploadedFileInfo> {
    const response = await apiClient.get<UploadedFileInfo>(
      `/upload/knowledge/check?filename=${encodeURIComponent(filename)}`
    );
    return response.data;
  }
  
  /**
   * Get a list of all uploaded knowledge files
   * @returns Array of file information objects
   */
  async listKnowledgeFiles(): Promise<MultiUploadResponse> {
    const response = await apiClient.get<MultiUploadResponse>('/upload/knowledge/list');
    return response.data;
  }
  
  /**
   * Format file size for display
   * @param bytes File size in bytes
   * @returns Formatted file size string
   */
  formatFileSize(bytes?: number): string {
    if (bytes === undefined) return 'Unknown size';
    
    const units = ['B', 'KB', 'MB', 'GB'];
    let size = bytes;
    let unitIndex = 0;
    
    while (size >= 1024 && unitIndex < units.length - 1) {
      size /= 1024;
      unitIndex++;
    }
    
    return `${size.toFixed(1)} ${units[unitIndex]}`;
  }
}

export const uploadService = new UploadService(); 