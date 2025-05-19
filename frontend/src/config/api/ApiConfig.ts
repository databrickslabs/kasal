import axios from 'axios';

export const config = {
  //apiUrl: process.env.REACT_APP_API_URL || 'https://your-app.aws.databricksapps.com/api/v1',
  //apiUrl: 'https://kasal-6051921418418893.staging.aws.databricksapps.com/api/v1',
  //apiUrl: 'http://localhost:8000/api/v1',
  apiUrl: process.env.REACT_APP_API_URL || 'http://localhost:8000/api/v1',
};

export const apiClient = axios.create({
  baseURL: config.apiUrl,
});

export default apiClient; 