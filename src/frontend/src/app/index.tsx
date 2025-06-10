import React from 'react';
import ReactDOM from 'react-dom/client';
import App from './__tests__/App';
import reportWebVitals from '../reportWebVitals';
import { BrowserRouter } from 'react-router-dom';
import { Provider } from 'react-redux';
import { store } from '../store';

// Prevent ResizeObserver loop limit exceeded errors
const originalError = console.error;
console.error = (...args) => {
  if (args[0]?.includes?.('ResizeObserver loop limit exceeded') ||
      args[0]?.includes?.('ResizeObserver loop completed with undelivered notifications')) {
    // Ignore ResizeObserver errors
    return;
  }
  originalError(...args);
};

const root = ReactDOM.createRoot(
  document.getElementById('root') as HTMLElement
);
root.render(
  <React.StrictMode>
    <Provider store={store}>
      <BrowserRouter>
        <App />
      </BrowserRouter>
    </Provider>
  </React.StrictMode>
);

// If you want to start measuring performance in your app, pass a function
// to log results (for example: reportWebVitals(console.log))
// or send to an analytics endpoint. Learn more: https://bit.ly/CRA-vitals
reportWebVitals();
