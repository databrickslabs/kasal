import { getClient } from './client';

export interface GenieSpace {
  id: string;
  name: string;
  description?: string;
  type?: string;
  enabled?: boolean;
}

interface GenieSpacesResponse {
  spaces: GenieSpace[];
  has_more?: boolean;
}

/**
 * The genie router is mounted inside the main api_router with prefix="/api/genie",
 * so relative to the client's baseURL (/api/v1) the path is /api/genie/spaces/search.
 */
export async function searchGenieSpaces(query?: string): Promise<GenieSpace[]> {
  const response = await getClient().post<GenieSpacesResponse>(
    '/api/genie/spaces/search',
    { search_query: query || '', enabled_only: true, page_size: 50 },
  );
  return response.data.spaces || [];
}
