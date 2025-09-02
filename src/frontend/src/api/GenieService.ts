/**
 * Genie Service
 * 
 * Service for interacting with Databricks Genie API endpoints.
 * Handles fetching Genie spaces and related operations.
 */

import { apiClient } from '../config/api/ApiConfig';

/**
 * Represents a Genie space
 */
export interface GenieSpace {
  id: string;
  name: string;
  description?: string;
  type?: string;
  created_at?: string;
  updated_at?: string;
}

/**
 * Response from the Genie spaces API with pagination
 */
export interface GenieSpacesResponse {
  spaces: GenieSpace[];
  next_page_token?: string;
  page_size?: number;
  has_more?: boolean;
  filtered?: boolean;
  total_fetched?: number;
}

/**
 * Request parameters for searching Genie spaces with pagination
 */
export interface GenieSpacesSearchRequest {
  search_query?: string;
  space_ids?: string[];
  enabled_only?: boolean;
  page_token?: string;
  page_size?: number;
}

/**
 * Service class for Genie-related operations
 */
export class GenieService {
  /**
   * Fetch available Genie spaces with pagination
   * @param pageToken Optional token for fetching next page
   * @param pageSize Number of items per page (default 50)
   * @returns Promise containing the response with spaces and pagination info
   */
  static async getSpaces(pageToken?: string, pageSize = 50): Promise<GenieSpacesResponse> {
    try {
      const params: Record<string, string | number> = { page_size: pageSize };
      if (pageToken) {
        params.page_token = pageToken;
      }
      const response = await apiClient.get<GenieSpacesResponse>('/api/genie/spaces', { params });
      return response.data;
    } catch (error) {
      console.error('Error fetching Genie spaces:', error);
      // Return empty response on error so UI can handle gracefully
      return { spaces: [] };
    }
  }

  /**
   * Search and filter Genie spaces with pagination
   * @param searchParams Search parameters including pagination
   * @returns Promise containing the response with filtered spaces and pagination info
   */
  static async searchSpaces(searchParams: GenieSpacesSearchRequest): Promise<GenieSpacesResponse> {
    try {
      const response = await apiClient.post<GenieSpacesResponse>(
        '/api/genie/spaces/search',
        searchParams
      );
      return response.data;
    } catch (error) {
      console.error('Error searching Genie spaces:', error);
      return { spaces: [] };
    }
  }

  /**
   * Get details for a specific Genie space
   * @param spaceId The ID of the space to fetch
   * @returns Promise containing the space details
   */
  static async getSpaceDetails(spaceId: string): Promise<GenieSpace> {
    try {
      const response = await apiClient.get(`/api/genie/spaces/${spaceId}`);
      return response.data;
    } catch (error) {
      console.error(`Error fetching Genie space ${spaceId}:`, error);
      throw error;
    }
  }

  /**
   * Helper to format space display name
   * @param space The Genie space
   * @returns Formatted display name
   */
  static formatSpaceName(space: GenieSpace): string {
    if (space.description) {
      return `${space.name} - ${space.description}`;
    }
    return space.name;
  }

  /**
   * Helper to get a default space ID if configured
   * This can be used as a fallback when no space is selected
   */
  static getDefaultSpaceId(): string {
    // This could be configured in environment variables or settings
    return '01efdd2cd03211d0ab74f620f0023b77';
  }
}

export default GenieService;