import { useState, useCallback } from 'react';

export const useErrorManager = () => {
  const [showError, setShowError] = useState(false);
  const [errorMessage, setErrorMessage] = useState<string>('');

  const handleCloseError = useCallback(() => {
    setShowError(false);
  }, []);

  const showErrorMessage = useCallback((message: string) => {
    setErrorMessage(message);
    setShowError(true);
  }, []);

  return {
    showError,
    errorMessage,
    handleCloseError,
    showErrorMessage
  };
}; 