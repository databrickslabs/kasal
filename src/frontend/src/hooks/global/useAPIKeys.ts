import { useEffect } from 'react';
import { useAPIKeysStore } from '../../store/apiKeys';
import { ApiKey } from '../../types/apiKeys';

export const useAPIKeys = () => {
  const { secrets, loading, error, fetchAPIKeys, updateSecrets: updateSecretsList } = useAPIKeysStore();

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