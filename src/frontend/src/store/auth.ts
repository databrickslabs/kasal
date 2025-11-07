import { create } from 'zustand';

interface User {
  id: string;
  email: string;
  name?: string;
}

interface AuthStore {
  user: User | null;
  token: string | null;
  selectedGroupId: string | null;

  setUser: (user: User | null) => void;
  setToken: (token: string | null) => void;
  setSelectedGroup: (groupId: string | null) => void;

  login: (token: string, user: User) => void;
  logout: () => void;

  initializeFromStorage: () => void;
}

export const useAuthStore = create<AuthStore>((set) => ({
  user: null,
  token: null,
  selectedGroupId: null,

  setUser: (user) => {
    set({ user });
    if (user) {
      localStorage.setItem('user', JSON.stringify(user));
    } else {
      localStorage.removeItem('user');
    }
  },

  setToken: (token) => {
    set({ token });
    if (token) {
      localStorage.setItem('token', token);
    } else {
      localStorage.removeItem('token');
    }
  },

  setSelectedGroup: (groupId) => {
    set({ selectedGroupId: groupId });
    if (groupId) {
      localStorage.setItem('selectedGroupId', groupId);
    } else {
      localStorage.removeItem('selectedGroupId');
    }
  },

  login: (token, user) => {
    set({ token, user });
    localStorage.setItem('token', token);
    localStorage.setItem('user', JSON.stringify(user));
    // Also save user email and ID separately for easy access
    if (user.email) {
      localStorage.setItem('userEmail', user.email);
    }
    if (user.id) {
      localStorage.setItem('userId', user.id);
    }
  },

  logout: () => {
    set({
      user: null,
      token: null,
      selectedGroupId: null
    });
    localStorage.removeItem('token');
    localStorage.removeItem('user');
    localStorage.removeItem('userEmail');
    localStorage.removeItem('userId');
    localStorage.removeItem('selectedGroupId');
  },

  initializeFromStorage: () => {
    const token = localStorage.getItem('token');
    const userStr = localStorage.getItem('user');
    const selectedGroupId = localStorage.getItem('selectedGroupId');

    if (token && userStr) {
      try {
        const user = JSON.parse(userStr);
        set({ token, user, selectedGroupId });
      } catch (e) {
        console.error('Failed to parse stored user:', e);
      }
    }
  }
}));