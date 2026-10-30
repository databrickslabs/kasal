import React from 'react';
import ReactDOM from 'react-dom/client';
import { BrowserRouter } from 'react-router-dom';
import App from './app/App';
import { ErrorBoundary } from './shared/errors/ErrorBoundary';

const root = ReactDOM.createRoot(
  document.getElementById('root') as HTMLElement
);

// Last resort: an error outside the route boundary in App (the shell, the
// providers) still shows a reload prompt instead of a blank page.
const app = (
  <ErrorBoundary variant="page">
    <BrowserRouter>
      <App />
    </BrowserRouter>
  </ErrorBoundary>
);

// StrictMode double-invokes effects, which duplicates the live SSE/stream
// subscriptions the workflow and chat surfaces open — so it is applied to
// production builds only, where those extra dev-time checks do not run anyway.
if (import.meta.env.DEV) {
  root.render(app);
} else {
  root.render(
    <React.StrictMode>
      {app}
    </React.StrictMode>
  );
}
