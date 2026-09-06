import { getClient } from './client';

export interface GenieSpace {
  id: string;
  name: string;
  description?: string;
  type?: string;
  enabled?: boolean;
  /** Deep link to open the space in the Databricks Genie UI (to validate the selection). */
  url?: string;
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

/**
 * Fetch a single Genie space by id (includes its `url` deep link). Used to
 * resolve the link for an already-selected space without opening/loading the
 * full list (e.g. a restored session). Returns null on any error.
 */
export async function getGenieSpace(spaceId: string): Promise<GenieSpace | null> {
  try {
    const response = await getClient().get<GenieSpace>(
      `/api/genie/spaces/${encodeURIComponent(spaceId)}`,
    );
    return response.data || null;
  } catch {
    return null;
  }
}
