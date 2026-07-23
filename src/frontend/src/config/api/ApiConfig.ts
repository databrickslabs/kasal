import axios from 'axios';
import toast from 'react-hot-toast';

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

    if (selectedGroupId) {
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
    // Recover from a stale selected workspace. The `group_id` header can point at
    // a workspace the user can no longer access — most commonly right after a
    // redeploy, when the DB is temporarily on local storage (SQLite) until the
    // admin reconnects Lakebase, so the previously-selected workspace doesn't
    // exist yet. The backend then 403s EVERY group-scoped call, including
    // /users/me, which would otherwise leave the app stuck on a blank screen with
    // no way to self-correct. Clear the stale selection and retry once WITHOUT the
    // header so the request falls back to the personal workspace; the group store
    // then re-validates and the user can re-select the workspace once it returns.
    const original = error.config;
    const detail = error.response?.data?.detail;
    const isGroupAccessDenied =
      error.response?.status === 403 &&
      typeof detail === 'string' &&
      detail.toLowerCase().includes('access to group');
    if (isGroupAccessDenied && original && !original._groupAccessRetry) {
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

      // Show a single deduplicated toast for Lakebase / database outages
      if (error.response.status === 503) {
        toast.error('Database connection issue — please try again shortly.', {
          id: 'lakebase-503',
          duration: 5000,
        });
      }

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