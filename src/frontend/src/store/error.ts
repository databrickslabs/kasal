import { create } from 'zustand';

interface ErrorState {
  showError: boolean;
  errorMessage: string;
  setShowError: (show: boolean) => void;
  setErrorMessage: (message: string) => void;
  showErrorMessage: (message: string) => void;
  clearError: () => void;
  resetError: () => void;
}

const initialState = {
  showError: false,
  errorMessage: '',
};

export const useErrorStore = create<ErrorState>((set) => ({
  ...initialState,
  
  setShowError: (show: boolean) => 
    set(() => ({ showError: show })),
  
  setErrorMessage: (message: string) => 
    set(() => ({ errorMessage: message })),
  
  showErrorMessage: (message: string) => 
    set(() => ({ 
      errorMessage: message,
      showError: true 
    })),
  
  clearError: () => 
    set(() => ({ 
      showError: false,
      errorMessage: '' 
    })),
  
  resetError: () => 
    set(() => ({ ...initialState })),
})); 