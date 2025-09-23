import React from 'react';
import ReactDOM from 'react-dom/client';
import App from './__tests__/App';
import reportWebVitals from '../reportWebVitals';
import { BrowserRouter } from 'react-router-dom';
import { Provider } from 'react-redux';
import { store } from '../store';

// Reduce console noise, but keep errors visible in production for debugging
(() => {
  const noop = (..._args: unknown[]): void => { return; };
  if (process.env.NODE_ENV !== 'test') {
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    console.log = noop as any;
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    console.debug = noop as any;
    // Keep warnings in development; silence in production
    if (process.env.NODE_ENV === 'production') {
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      console.warn = noop as any;
    }
    // Do NOT silence console.error so we can see runtime issues in prod preview
  }
})();

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

if (process.env.NODE_ENV === 'development') {
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