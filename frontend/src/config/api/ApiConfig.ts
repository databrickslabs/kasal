import axios from 'axios';

export const config = {
  //apiUrl: process.env.REACT_APP_API_URL || 'https://your-app.aws.databricksapps.com/api/v1',
  //apiUrl: 'https://kasal-6051921418418893.staging.aws.databricksapps.com/api/v1',
  //apiUrl: 'http://localhost:8000/api/v1',
  apiUrl: process.env.NODE_ENV === 'development' 
    ? 'http://localhost:8000/api/v1'
    : '/api/v1', // Use relative URL in production
};

export const apiClient = axios.create({
  baseURL: config.apiUrl,
});

export default apiClient; 