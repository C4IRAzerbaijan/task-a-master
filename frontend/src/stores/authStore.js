// src/stores/authStore.js
import { create } from 'zustand';
import { authService } from '../services/api';

const useAuthStore = create((set, get) => ({
  user: null,
  isAuthenticated: false,
  isLoading: false,
  error: null,

  login: async (username, password) => {
    set({ isLoading: true, error: null });
    try {
      const data = await authService.login(username, password);
      
      // Set authenticated state immediately after successful login
      set({ 
        user: data.user, 
        isAuthenticated: true, 
        isLoading: false,
        error: null 
      });
      
      return { success: true };
    } catch (error) {
      const errorMessage = error.response?.data?.error || 'Giriş xətası';
      set({ 
        error: errorMessage, 
        isLoading: false,
        isAuthenticated: false,
        user: null
      });
      return { success: false, error: errorMessage };
    }
  },

  register: async (username, password, email) => {
    set({ isLoading: true, error: null });
    try {
      const data = await authService.register(username, password, email);
      
      // Set authenticated state immediately after successful registration
      set({ 
        user: data.user, 
        isAuthenticated: true, 
        isLoading: false,
        error: null
      });
      
      return { success: true };
    } catch (error) {
      const errorMessage = error.response?.data?.error || 'Qeydiyyat xətası';
      set({ 
        error: errorMessage, 
        isLoading: false,
        isAuthenticated: false,
        user: null
      });
      return { success: false, error: errorMessage };
    }
  },

  logout: async () => {
    set({ isLoading: true });
    try {
      await authService.logout();
    } catch (error) {
      console.error('Logout error:', error);
    } finally {
      localStorage.removeItem('access_token');
      set({ 
        user: null, 
        isAuthenticated: false,
        error: null,
        isLoading: false 
      });
    }
  },

  checkAuth: async () => {
    try {
      const data = await authService.checkAuth();
      
      if (data.authenticated && data.user) {
        set({ 
          user: data.user, 
          isAuthenticated: true,
          isLoading: false 
        });
        return true;
      } else {
        set({ 
          user: null, 
          isAuthenticated: false,
          isLoading: false 
        });
        return false;
      }
    } catch (error) {
      console.error('Auth check failed:', error);
      set({ 
        user: null, 
        isAuthenticated: false,
        isLoading: false 
      });
      return false;
    }
  },

  clearError: () => set({ error: null }),
}));

export default useAuthStore;
