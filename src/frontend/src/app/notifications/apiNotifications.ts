import toast from 'react-hot-toast';
import { subscribeToApiFailures } from '../../shared/api/errors';
import { TEAMSPACE_REQUIRED_MESSAGE } from '../../shared/api/tabGroup';

// Install before app startup requests; Axios remains independent of presentation.
const unsubscribe = subscribeToApiFailures(({ status, code }) => {
  if (code === 'teamspace_required') {
    toast.error(TEAMSPACE_REQUIRED_MESSAGE, { id: 'teamspace-required', duration: 8000 });
    return;
  }
  if (status === 503) {
    toast.error('Database connection issue — please try again shortly.', {
      id: 'lakebase-503',
      duration: 5000,
    });
  }
});

if (import.meta.hot) import.meta.hot.dispose(unsubscribe);
