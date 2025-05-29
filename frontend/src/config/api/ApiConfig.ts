import axios from 'axios';

export const config = {
  //apiUrl: process.env.REACT_APP_API_URL || 'https://kasal-1444828305810485.aws.databricksapps.com/api/v1',
  //apiUrl: 'https://kasal-6051921418418893.staging.aws.databricksapps.com/api/v1',
  //apiUrl: 'http://localhost:8000/api/v1',
  apiUrl: process.env.DATABRICKS_APP_URL ? `${process.env.DATABRICKS_APP_URL}/api/v1` : 'http://localhost:8000/api/v1',
};

export const apiClient = axios.create({
  baseURL: config.apiUrl,
});

export default apiClient; 