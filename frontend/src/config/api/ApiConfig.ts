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

// Add a request interceptor to include authentication tokens and tenant headers
apiClient.interceptors.request.use(
  (config) => {
    const token = localStorage.getItem('token');
    if (token) {
      config.headers.Authorization = `Bearer ${token}`;
    }
    
    // For local development: add mock tenant headers
    const mockUserEmail = localStorage.getItem('mockUserEmail');
    if (mockUserEmail && process.env.NODE_ENV === 'development') {
      config.headers['X-Forwarded-Email'] = mockUserEmail;
      config.headers['X-Forwarded-Access-Token'] = 'mock-token-for-dev';
      console.log(`[DEV] Using mock user: ${mockUserEmail}`);
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
      console.error('API Error Response:', {
        status: error.response.status,
        data: error.response.data,
        headers: error.response.headers,
      });
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