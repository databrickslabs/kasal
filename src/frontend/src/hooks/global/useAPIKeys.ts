import { useShallow } from 'zustand/react/shallow';
import { useEffect } from 'react';
import { useAPIKeysStore } from '../../store/apiKeys';
import { ApiKey } from '../../types/config/apiKeys';

export const useAPIKeys = () => {
  const { secrets, loading, error, fetchAPIKeys, updateSecrets: updateSecretsList } = useAPIKeysStore(useShallow(state => ({
    secrets: state.secrets,
    loading: state.loading,
    error: state.error,
    fetchAPIKeys: state.fetchAPIKeys,
    updateSecrets: state.updateSecrets,
  })));

  useEffect(() => {
    fetchAPIKeys();
  }, [fetchAPIKeys]);

  const updateSecrets = (updatedSecrets: ApiKey[]) => {
    updateSecretsList(updatedSecrets);
  };

  return {
    secrets,
    loading,
    error,
    updateSecrets,
  };
}; 