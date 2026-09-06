import toast from 'react-hot-toast';
import { subscribeToApiFailures } from '../../shared/api/errors';

// Install before app startup requests; Axios remains independent of presentation.
const unsubscribe = subscribeToApiFailures(({ status }) => {
  if (status === 503) {
    toast.error('Database connection issue — please try again shortly.', {
      id: 'lakebase-503',
      duration: 5000,
    });
  }
});

if (import.meta.hot) import.meta.hot.dispose(unsubscribe);
