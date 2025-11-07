// DEPRECATED WRAPPER: All API calls must go through apiClient from ApiConfig
// This file now forwards to the centralized apiClient to preserve backward compatibility.
import { apiClient } from '../config/api/ApiConfig';

export const ApiService = {
  // GET request
  get: async (url: string, params = {}) => {
    return apiClient.get(url, { params });
  },

  // POST request
  post: async (url: string, data = {}) => {
    return apiClient.post(url, data);
  },

  // PUT request
  put: async (url: string, data = {}) => {
    return apiClient.put(url, data);
  },

  // DELETE request
  delete: async (url: string) => {
    return apiClient.delete(url);
  },

  // PATCH request
  patch: async (url: string, data = {}) => {
    return apiClient.patch(url, data);
  }
};

// Development utilities for mock user management (unchanged)
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