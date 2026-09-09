import axios from 'axios';
import { reportApiFailure } from './errors';

export const config = {
  apiUrl:
    import.meta.env.VITE_API_URL ||
    (import.meta.env.DEV
      ? 'http://localhost:8000/api/v1'
      : '/api/v1'),
};

export const apiClient = axios.create({
  baseURL: config.apiUrl,
  headers: {
    'Content-Type': 'application/json',
  },
});

// Add a request interceptor to include group context headers
apiClient.interceptors.request.use(
  (config) => {
    // Add group context headers if available
    const selectedGroupId = localStorage.getItem('selectedGroupId');

    // Identity/membership discovery must work even when a saved selection is stale.
    const identityDiscovery = (config.method ?? 'get').toLowerCase() === 'get'
      && /\/(?:users\/me|groups\/my-groups)\/?(?:\?|$)/.test(config.url ?? '');
    if (identityDiscovery) {
      delete config.headers['group_id'];
    } else if (selectedGroupId && !('group_id' in config.headers)) {
      config.headers['group_id'] = selectedGroupId;  // Use 'group_id' to match database column name
    }

    // For local development: simulate Databricks Apps auth header
    // In production, this header is set by the Databricks Apps gateway
    if (import.meta.env.DEV) {
      const devEmail = import.meta.env.VITE_DEV_USER_EMAIL || 'dev@localhost';
      config.headers['X-Forwarded-Email'] = devEmail;
    }

    return config;
  },
  (error) => {
    return Promise.reject(error);
  }
);

// Add a response interceptor to handle errors
apiClient.interceptors.response.use(
  (response) => response,
  async (error) => {
    // Recover read requests from a workspace the user can no longer access.
    // Writes must never be replayed silently in another workspace.
    const original = error.config;
    const detail = error.response?.data?.detail;
    const isGroupAccessDenied =
      error.response?.status === 403 &&
      typeof detail === 'string' &&
      detail.toLowerCase().includes('access to group');
    if (isGroupAccessDenied && original && !original._groupAccessRetry
      && (original.method ?? 'get').toLowerCase() === 'get'
      && !/\/chat-history(?:\/|$)/.test(original.url ?? '')) {
      original._groupAccessRetry = true;
      try {
        localStorage.removeItem('selectedGroupId');
      } catch { /* localStorage may be unavailable */ }
      if (original.headers) {
        delete original.headers['group_id'];
      }
      // Retry with the header cleared — the request interceptor reads the (now
      // removed) localStorage value, so no group_id header is attached.
      return apiClient(original);
    }

    if (error.response) {
      // List of endpoints where 404 is expected and shouldn't be logged as an error
      const expectedNotFoundEndpoints = [
        '/databricks/config',
        '/memory-backend/config',
        '/default-config',
        '/knowledge/config'
      ];

      const isExpected404 = error.response.status === 404 &&
        expectedNotFoundEndpoints.some(endpoint =>
          error.config?.url?.includes(endpoint)
        );

      reportApiFailure({ status: error.response.status, url: error.config?.url });

      // Don't log 404 errors for configuration endpoints or other expected cases
      if (!isExpected404 && error.response.status !== 404) {
        const status = error.response.status;
        const url = error.config?.url;
        console.error('API error:', status, url);
      }
    } else if (error.request) {
      // The request was made but no response was received
      console.error('API No Response:', error.request);
    } else {
      // Something happened in setting up the request that triggered an Error
      console.error('API Request Error:', error.message);
    }
    return Promise.reject(error);
  }
);

export default apiClient;
