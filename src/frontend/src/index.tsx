import React from 'react';
import ReactDOM from 'react-dom/client';
import { BrowserRouter } from 'react-router-dom';
import App from './App';

const root = ReactDOM.createRoot(
  document.getElementById('root') as HTMLElement
);

const app = (
  <BrowserRouter>
    <App />
  </BrowserRouter>
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
