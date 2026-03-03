// src/App.jsx
import React, { useEffect } from 'react';
import { BrowserRouter as Router, Routes, Route, Navigate } from 'react-router-dom';
import { Toaster } from 'react-hot-toast';
import useAuthStore from './stores/authStore';
import Login from './components/Login';
import Register from './components/Register';
import SmartDashboard from './components/SmartDashboard';
import FileManagement from './components/FileManagement';
import LoadingScreen from './components/LoadingScreen';

// Error Boundary to prevent white screen on React errors
class ErrorBoundary extends React.Component {
  constructor(props) {
    super(props);
    this.state = { hasError: false, error: null };
  }
  static getDerivedStateFromError(error) {
    return { hasError: true, error };
  }
  componentDidCatch(error, info) {
    console.error('React Error Boundary caught:', error, info);
  }
  render() {
    if (this.state.hasError) {
      return (
        <div style={{ padding: 40, textAlign: 'center', color: '#fff', background: '#1e1b4b', minHeight: '100vh', display: 'flex', alignItems: 'center', justifyContent: 'center' }}>
          <div>
            <h2>Xəta baş verdi</h2>
            <p style={{ opacity: 0.7 }}>{this.state.error?.message || 'Bilinməyən xəta'}</p>
            <button onClick={() => { localStorage.removeItem('access_token'); window.location.href = '/login'; }}
              style={{ marginTop: 16, padding: '10px 24px', borderRadius: 8, background: '#6366f1', color: '#fff', border: 'none', cursor: 'pointer' }}>
              Yenidən giriş
            </button>
          </div>
        </div>
      );
    }
    return this.props.children;
  }
}

// Protected Route Component
const ProtectedRoute = ({ children }) => {
  const { isAuthenticated, checkAuth } = useAuthStore();
  const [checking, setChecking] = React.useState(!isAuthenticated);

  React.useEffect(() => {
    // If already authenticated (from token in localStorage), skip API check
    if (isAuthenticated) {
      setChecking(false);
      return;
    }
    // Otherwise verify with server
    checkAuth().finally(() => setChecking(false));
  }, [checkAuth, isAuthenticated]);

  if (checking) {
    return <LoadingScreen />;
  }

  return isAuthenticated ? children : <Navigate to="/login" replace />;
};

// Public Route Component
const PublicRoute = ({ children }) => {
  const { isAuthenticated } = useAuthStore();
  return !isAuthenticated ? children : <Navigate to="/dashboard" replace />;
};

function App() {
  const { checkAuth } = useAuthStore();

  useEffect(() => {
    // Check authentication on app load
    checkAuth();
  }, []);

  return (
    <ErrorBoundary>
    <Router>
      <Toaster
        position="top-right"
        toastOptions={{
          duration: 4000,
          style: {
            background: '#363636',
            color: '#fff',
          },
          success: {
            duration: 3000,
            iconTheme: {
              primary: '#10B981',
              secondary: '#fff',
            },
          },
          error: {
            duration: 4000,
            iconTheme: {
              primary: '#EF4444',
              secondary: '#fff',
            },
          },
        }}
      />
      
      <Routes>
        <Route path="/" element={<Navigate to="/dashboard" />} />
        <Route
          path="/login"
          element={
            <PublicRoute>
              <Login />
            </PublicRoute>
          }
        />
        <Route
          path="/register"
          element={
            <PublicRoute>
              <Register />
            </PublicRoute>
          }
        />
        <Route
          path="/dashboard"
          element={
            <ProtectedRoute>
              <SmartDashboard />
            </ProtectedRoute>
          }
        />
        <Route
          path="/file-management"
          element={
            <ProtectedRoute>
              <FileManagement />
            </ProtectedRoute>
          }
        />
        <Route path="*" element={<Navigate to="/" />} />
      </Routes>
    </Router>
    </ErrorBoundary>
  );
}

export default App;
