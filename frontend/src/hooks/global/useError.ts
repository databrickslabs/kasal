import { useCallback } from 'react';
import { useErrorStore } from '../../store/error';

export const useError = () => {
  const { showError, errorMessage, clearError, showErrorMessage: handleShowErrorMessage } = useErrorStore();

  const handleCloseError = useCallback(() => {
    clearError();
  }, [clearError]);

  return {
    showError,
    errorMessage,
    handleCloseError,
    showErrorMessage: handleShowErrorMessage,
  };
}; 