export interface ApiFailure {
  /** HTTP status; 0 when the client refused to send the request. */
  status: number;
  url?: string;
  /** Why the client refused to send it. */
  code?: 'teamspace_required';
}

type Listener = (failure: ApiFailure) => void;
const listeners = new Set<Listener>();

export function subscribeToApiFailures(listener: Listener): () => void {
  listeners.add(listener);
  return () => { listeners.delete(listener); };
}

export function reportApiFailure(failure: ApiFailure): void {
  for (const listener of listeners) listener(failure);
}
