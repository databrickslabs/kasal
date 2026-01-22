import React from 'react';
import ReactDOM from 'react-dom/client';
import App from '../App';
import reportWebVitals from '../reportWebVitals';
import { BrowserRouter } from 'react-router-dom';
import { Provider } from 'react-redux';
import { store } from '../store';

// Console logging configuration - currently disabled to debug webpack issues
// Uncomment this section once the application is working properly
/*
(() => {
  const noop = (..._args: unknown[]): void => { return; };
  if (import.meta.env.MODE !== 'test') {
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    console.log = noop as any;
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    console.debug = noop as any;
    // Keep warnings in development; silence in production
    if (import.meta.env.PROD) {
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      console.warn = noop as any;
    }
    // Do NOT silence console.error so we can see runtime issues in prod preview
  }
})();
*/

const root = ReactDOM.createRoot(
  document.getElementById('root') as HTMLElement
);
const app = (
  <Provider store={store}>
    <BrowserRouter>
      <App />
    </BrowserRouter>
  </Provider>
);

if (import.meta.env.DEV) {
  root.render(app);
} else {
  root.render(
    <React.StrictMode>
      {app}
    </React.StrictMode>
  );
}

// If you want to start measuring performance in your app, pass a function
// to log results (for example: reportWebVitals(console.log))
// or send to an analytics endpoint. Learn more: https://bit.ly/CRA-vitals
reportWebVitals();