import axios from 'axios';

export const config = {
  //apiUrl: process.env.REACT_APP_API_URL || 'https://your-app.aws.databricksapps.com/api/v1',
  //apiUrl: 'https://your-staging-app.aws.databricksapps.com/api/v1',
  //apiUrl: 'http://localhost:8000/api/v1',
  apiUrl: process.env.NODE_ENV === 'development' 
    ? 'http://localhost:8000/api/v1'
    : '/api/v1', // Use relative URL in production
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

// Add a response interceptor to handle errors
apiClient.interceptors.response.use(
  (response) => response,
  (error) => {
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

      // Don't log 404 errors for configuration endpoints or other expected cases
      if (!isExpected404 && error.response.status !== 404) {
        console.error('API Error Response:', {
          status: error.response.status,
          data: error.response.data,
          headers: error.response.headers,
        });
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