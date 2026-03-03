// src/stores/authStore.js
import { create } from 'zustand';
import { authService } from '../services/api';

// Decode JWT payload without verification (just to read claims client-side)
function decodeToken(token) {
  try {
    const base64Url = token.split('.')[1];
    const base64 = base64Url.replace(/-/g, '+').replace(/_/g, '/');
    const payload = JSON.parse(atob(base64));
    // Check expiry
    if (payload.exp && payload.exp * 1000 < Date.now()) {
      return null; // expired
    }
    return payload;
  } catch {
    return null;
  }
}

// Initialize auth state from stored token (synchronous, no API call needed)
function getInitialAuthState() {
  const token = localStorage.getItem('access_token');
  if (token) {
    const payload = decodeToken(token);
    if (payload) {
      return {
        user: {
          id: payload.user_id,
          username: payload.username,
          role: payload.role || 'user'
        },
        isAuthenticated: true
      };
    }
    // Token exists but invalid/expired - clean up
    localStorage.removeItem('access_token');
  }
  return { user: null, isAuthenticated: false };
}

const initialAuth = getInitialAuthState();

const useAuthStore = create((set, get) => ({
  user: initialAuth.user,
  isAuthenticated: initialAuth.isAuthenticated,
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
    // First: check if we have a valid token locally (instant, no API call)
    const token = localStorage.getItem('access_token');
    if (token) {
      const payload = decodeToken(token);
      if (payload) {
        const user = {
          id: payload.user_id,
          username: payload.username,
          role: payload.role || 'user'
        };
        set({ user, isAuthenticated: true, isLoading: false });
        return true;
      }
      // Token expired/invalid - clean up
      localStorage.removeItem('access_token');
    }

    // No valid token - try server session (for local dev with filesystem sessions)
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
