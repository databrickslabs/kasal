export interface ApiFailure {
  status: number;
  url?: string;
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
