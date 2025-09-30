import axios from 'axios';

export const config = {
  apiUrl:
    process.env.REACT_APP_API_URL ||
    (process.env.NODE_ENV === 'development'
      ? 'http://localhost:8000/api/v1'
      : '/api/v1'),
};

export const apiClient = axios.create({
  baseURL: config.apiUrl,
  headers: {
    'Content-Type': 'application/json',
  },
});

// Add a request interceptor to include authentication tokens and group context
apiClient.interceptors.request.use(
  (config) => {
    const token = localStorage.getItem('token');
    if (token) {
      config.headers.Authorization = `Bearer ${token}`;
    }

    // Add group context headers if available
    const selectedGroupId = localStorage.getItem('selectedGroupId');

    if (selectedGroupId) {
      config.headers['group_id'] = selectedGroupId;  // Use 'group_id' to match database column name
    }

    return config;
  },
  (error) => {
    return Promise.reject(error);
  }
);

// Track refresh token request to prevent multiple simultaneous refreshes
let isRefreshing = false;
let failedQueue: Array<{
  resolve: (value?: any) => void;
  reject: (reason?: any) => void;
}> = [];

const processQueue = (error: any, token: string | null = null) => {
  failedQueue.forEach((prom) => {
    if (error) {
      prom.reject(error);
    } else {
      prom.resolve(token);
    }
  });

  failedQueue = [];
};

// Add a response interceptor to handle errors and token refresh
apiClient.interceptors.response.use(
  (response) => response,
  async (error) => {
    const originalRequest = error.config;

    if (error.response) {
      // The request was made and the server responded with a status code
      // that falls out of the range of 2xx

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

      // Handle 401 Unauthorized - token expired
      if (error.response.status === 401 && !originalRequest._retry) {
        // Avoid refreshing token for login/register endpoints
        if (originalRequest.url?.includes('/auth/login') ||
            originalRequest.url?.includes('/auth/register') ||
            originalRequest.url?.includes('/auth/refresh-token')) {
          return Promise.reject(error);
        }

        if (isRefreshing) {
          // If already refreshing, queue this request
          return new Promise((resolve, reject) => {
            failedQueue.push({ resolve, reject });
          })
            .then((token) => {
              originalRequest.headers['Authorization'] = 'Bearer ' + token;
              return apiClient(originalRequest);
            })
            .catch((err) => {
              return Promise.reject(err);
            });
        }

        originalRequest._retry = true;
        isRefreshing = true;

        try {
          // Attempt to refresh the token
          const response = await axios.post(
            `${config.apiUrl}/auth/refresh-token`,
            {},
            { withCredentials: true } // Include httpOnly cookie
          );

          const { access_token } = response.data;

          if (access_token) {
            // Update token in localStorage
            localStorage.setItem('token', access_token);

            // Update Authorization header
            apiClient.defaults.headers.common['Authorization'] = `Bearer ${access_token}`;
            originalRequest.headers['Authorization'] = `Bearer ${access_token}`;

            // Process queued requests with new token
            processQueue(null, access_token);

            // Retry the original request
            return apiClient(originalRequest);
          }
        } catch (refreshError) {
          // Refresh failed - clear auth and redirect to login
          processQueue(refreshError, null);

          // Clear authentication state
          localStorage.removeItem('token');
          localStorage.removeItem('user');
          localStorage.removeItem('userEmail');
          localStorage.removeItem('userId');
          localStorage.removeItem('selectedGroupId');

          // Redirect to login page
          window.location.href = '/login';

          return Promise.reject(refreshError);
        } finally {
          isRefreshing = false;
        }
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