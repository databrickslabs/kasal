import axios from 'axios';

// Configuration for API requests
const config = {
  // Use environment variable for API URL if available, otherwise default to local development URL
  apiUrl: process.env.REACT_APP_API_URL || 'http://localhost:8000/api/v1',
};

// Create axios instance with base URL
const axiosInstance = axios.create({
  baseURL: config.apiUrl,
  headers: {
    'Content-Type': 'application/json',
  },
});

// Add a request interceptor to include authentication tokens and tenant headers
axiosInstance.interceptors.request.use(
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
axiosInstance.interceptors.response.use(
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

// API Service
export const ApiService = {
  // GET request
  get: async (url: string, params = {}) => {
    console.log('GET Request to:', url, 'with params:', params);
    return axiosInstance.get(url, { params });
  },

  // POST request
  post: async (url: string, data = {}) => {
    console.log('POST Request to:', url, 'with data:', data);
    return axiosInstance.post(url, data);
  },

  // PUT request
  put: async (url: string, data = {}) => {
    console.log('PUT Request to:', url, 'with data:', data);
    return axiosInstance.put(url, data);
  },

  // DELETE request
  delete: async (url: string) => {
    console.log('DELETE Request to:', url);
    return axiosInstance.delete(url);
  },

  // PATCH request
  patch: async (url: string, data = {}) => {
    console.log('PATCH Request to:', url, 'with data:', data);
    return axiosInstance.patch(url, data);
  }
};

// Development utilities for mock user management
export const DevUtils = {
  getCurrentMockUser: () => {
    return localStorage.getItem('mockUserEmail') || null;
  },
  
  setMockUser: (email: string) => {
    localStorage.setItem('mockUserEmail', email);
  },
  
  clearMockUser: () => {
    localStorage.removeItem('mockUserEmail');
  },
  
  isDevelopment: () => {
    return process.env.NODE_ENV === 'development';
  }
};

export default ApiService; 