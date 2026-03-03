// src/services/api.js - URL uyğunsuzluğu düzəldilmiş
import axios from 'axios';

const API_BASE_URL = import.meta.env.VITE_API_URL || '';

// Detect if running on the same origin (Vercel) or cross-origin (local dev)
const isLocalDev = API_BASE_URL && API_BASE_URL.includes('localhost');

// Create axios instance with default config
const api = axios.create({
  baseURL: API_BASE_URL,
  withCredentials: isLocalDev,  // Only send cookies for local dev (cross-origin sessions)
  headers: {
    'Content-Type': 'application/json',
  },
});

// Request interceptor - attach JWT token
api.interceptors.request.use(
  (config) => {
    const token = localStorage.getItem('access_token');
    if (token) {
      config.headers.Authorization = `Bearer ${token}`;
    }
    console.log('API Request:', config.method?.toUpperCase(), config.url);
    return config;
  },
  (error) => {
    return Promise.reject(error);
  }
);

// Response interceptor
api.interceptors.response.use(
  (response) => {
    console.log('API Response:', response.status, response.config.url);
    return response;
  },
  (error) => {
    console.error('API Error:', error.response?.status, error.config?.url, error.message);
    if (error.response?.status === 401) {
      // Don't redirect for auth endpoints - let the component handle the error
      const url = error.config?.url || '';
      const isAuthEndpoint = url.includes('/auth/login') || url.includes('/auth/register');
      if (!isAuthEndpoint) {
        localStorage.removeItem('access_token');
        window.location.href = '/login';
      }
    }
    return Promise.reject(error);
  }
);

// Auth Service
export const authService = {
  login: async (username, password) => {
    const response = await api.post('/api/auth/login', { username, password });
    if (response.data.access_token) {
      localStorage.setItem('access_token', response.data.access_token);
    }
    return response.data;
  },
  
  register: async (username, password, email) => {
    const response = await api.post('/api/auth/register', { username, password, email });
    if (response.data.access_token) {
      localStorage.setItem('access_token', response.data.access_token);
    }
    return response.data;
  },
  
  logout: async () => {
    localStorage.removeItem('access_token');
    const response = await api.post('/api/auth/logout');
    return response.data;
  },
  
  getProfile: async () => {
    const response = await api.get('/api/auth/me');
    return response.data;
  },
  
  checkAuth: async () => {
    const response = await api.get('/api/auth/check');
    return response.data;
  }
};

// Default document types as fallback
const DEFAULT_DOCUMENT_TYPES = [
  { value: 'contact', label: 'Əlaqə məlumatları' },
  { value: 'contract', label: 'Müqavilə' },
  { value: 'vacation', label: 'Məzuniyyət' },
  { value: 'business_trip', label: 'Ezamiyyət' },
  { value: 'memorandum', label: 'Anlaşma memorandumu' },
  { value: 'report', label: 'Hesabat' },
  { value: 'letter', label: 'Məktub' },
  { value: 'invoice', label: 'Qaimə' },
  { value: 'other', label: 'Digər' }
];

// Document Service
export const documentService = {
  // Get all documents
  getDocuments: async () => {
    try {
      const response = await api.get('/api/documents');
      return response.data.documents || [];
    } catch (error) {
      console.error('Error fetching documents:', error);
      throw error;
    }
  },
  
  // Upload document
  uploadDocument: async (file, documentType = 'other', isTemplate = false) => {
    try {
      console.log('Uploading document:', {
        fileName: file.name,
        fileSize: file.size,
        documentType,
        isTemplate
      });

      const formData = new FormData();
      formData.append('file', file);
      formData.append('document_type', documentType);
      formData.append('is_template', isTemplate.toString());
      
      // Log FormData contents
      for (let [key, value] of formData.entries()) {
        console.log(`FormData ${key}:`, value);
      }

      const response = await api.post('/api/documents', formData, {
        headers: {
          'Content-Type': 'multipart/form-data',
        },
        onUploadProgress: (progressEvent) => {
          const percentCompleted = Math.round((progressEvent.loaded * 100) / progressEvent.total);
          console.log(`Upload progress: ${percentCompleted}%`);
        },
        timeout: 60000, // 60 seconds timeout
      });
      
      console.log('Upload response:', response.data);
      return response.data;
    } catch (error) {
      console.error('Error uploading document:', error);
      
      if (error.response) {
        console.error('Error response:', error.response.data);
        console.error('Error status:', error.response.status);
      }
      
      throw error;
    }
  },
  
  // Delete document
  deleteDocument: async (docId) => {
    try {
      const response = await api.delete(`/api/documents/${docId}`);
      return response.data;
    } catch (error) {
      console.error('Error deleting document:', error);
      throw error;
    }
  },
  
  // Download document by ID with cache busting
  downloadDocument: async (docId) => {
    try {
      console.log(`Downloading document ID: ${docId}`);
      
      // Add cache busting parameter
      const timestamp = new Date().getTime();
      
      const response = await api.get(`/api/documents/${docId}/download?t=${timestamp}`, {
        responseType: 'blob',
        timeout: 60000, // 60 seconds timeout
        headers: {
          'Cache-Control': 'no-cache',
          'Pragma': 'no-cache'
        }
      });
      
      console.log('Download response received:', {
        status: response.status,
        contentType: response.headers['content-type'],
        contentLength: response.headers['content-length']
      });
      
      // Validate blob
      if (!response.data || response.data.size === 0) {
        throw new Error('Boş fayl alındı');
      }
      
      return response.data;
    } catch (error) {
      console.error('Error downloading document:', error);
      
      if (error.response) {
        console.error('Download error response:', error.response.status);
        if (error.response.data instanceof Blob) {
          // Try to read error message from blob
          const text = await error.response.data.text();
          console.error('Error blob content:', text);
        }
      }
      
      throw error;
    }
  },
  
  // Reprocess document
  reprocessDocument: async (docId) => {
    try {
      const response = await api.post(`/api/documents/${docId}/reprocess`);
      return response.data;
    } catch (error) {
      console.error('Error reprocessing document:', error);
      throw error;
    }
  },
  
  // Search documents
  searchDocuments: async (query) => {
    try {
      const response = await api.get('/api/documents/search', {
        params: { q: query }
      });
      return response.data.documents || [];
    } catch (error) {
      console.error('Error searching documents:', error);
      return [];
    }
  },
  
  // Get document types
  getDocumentTypes: async () => {
    try {
      console.log('Fetching document types from API...');
      const response = await api.get('/api/documents/types');
      
      if (response.data && response.data.types && Array.isArray(response.data.types)) {
        console.log('Document types loaded from API:', response.data.types.length);
        return response.data.types;
      } else {
        console.warn('Invalid response format from /api/documents/types:', response.data);
        return DEFAULT_DOCUMENT_TYPES;
      }
    } catch (error) {
      console.error('Error loading document types from API:', error);
      console.log('Using default document types as fallback');
      return DEFAULT_DOCUMENT_TYPES;
    }
  },

  // Get all templates
  getTemplates: async () => {
    try {
      const response = await api.get('/api/documents/templates');
      return response.data.templates || [];
    } catch (error) {
      console.error('Error fetching templates:', error);
      return [];
    }
  },

  // Download template by name/type
  downloadTemplate: async (templateName) => {
    try {
      const response = await api.get(`/api/documents/templates/${templateName}`, {
        responseType: 'blob',
      });
      return response.data;
    } catch (error) {
      console.error('Error downloading template:', error);
      throw error;
    }
  },

  // Handle download from any URL (for links in chat)
  handleDownloadLink: async (url, filename = 'download') => {
    try {
      const response = await fetch(url, {
        method: 'GET',
        credentials: 'include',
      });
      
      if (!response.ok) {
        throw new Error(`Download failed: ${response.statusText}`);
      }
      
      const blob = await response.blob();
      
      // Try to get filename from response headers
      const contentDisposition = response.headers.get('Content-Disposition');
      if (contentDisposition && contentDisposition.includes('filename=')) {
        const match = contentDisposition.match(/filename[^;=\n]*=((['"]).*?\2|[^;\n]*)/);
        if (match && match[1]) {
          filename = match[1].replace(/['"]/g, '');
        }
      }
      
      // Create download URL and trigger download
      const downloadUrl = window.URL.createObjectURL(blob);
      const link = document.createElement('a');
      link.href = downloadUrl;
      link.download = filename;
      document.body.appendChild(link);
      link.click();
      
      // Clean up
      window.URL.revokeObjectURL(downloadUrl);
      document.body.removeChild(link);
      
      return true;
    } catch (error) {
      console.error('Error handling download link:', error);
      throw error;
    }
  },

  // Download template by ID with proper filename
  downloadTemplateById: async (templateId, templateName = 'template') => {
    try {
      const blob = await documentService.downloadDocument(templateId);
      
      // Generate proper filename
      const extension = 'docx'; // Default extension for templates
      const cleanName = templateName
        .toLowerCase()
        .replace(/[^a-z0-9əıöüçşğ]/g, '_')
        .replace(/_{2,}/g, '_')
        .replace(/^_|_$/g, '');
      
      const filename = `${cleanName}_template.${extension}`;
      
      // Create download
      const url = window.URL.createObjectURL(blob);
      const link = document.createElement('a');
      link.href = url;
      link.download = filename;
      document.body.appendChild(link);
      link.click();
      
      // Clean up
      window.URL.revokeObjectURL(url);
      document.body.removeChild(link);
      
      return true;
    } catch (error) {
      console.error('Error downloading template by ID:', error);
      throw error;
    }
  }
};

// Chat Service
export const chatService = {
  // Ask question
  askQuestion: async (question, documentId = null, conversationId = null) => {
    try {
      const response = await api.post('/api/chat/ask', {
        question,
        document_id: documentId,
        conversation_id: conversationId,
      });
      return response.data;
    } catch (error) {
      console.error('Error asking question:', error);
      throw error;
    }
  },
  
  // Get conversations
  getConversations: async () => {
    try {
      const response = await api.get('/api/chat/conversations');
      return response.data.conversations || [];
    } catch (error) {
      console.error('Error fetching conversations:', error);
      throw error;
    }
  },
  
  // Get conversation by ID
  getConversation: async (conversationId) => {
    try {
      const response = await api.get(`/api/chat/conversations/${conversationId}`);
      return response.data.conversation;
    } catch (error) {
      console.error('Error fetching conversation:', error);
      throw error;
    }
  },
  
  // Rename conversation
  renameConversation: async (conversationId, title) => {
    try {
      const response = await api.put(`/api/chat/conversations/${conversationId}/rename`, {
        title,
      });
      return response.data;
    } catch (error) {
      console.error('Error renaming conversation:', error);
      throw error;
    }
  },
  
  // Delete conversation
  deleteConversation: async (conversationId) => {
    try {
      const response = await api.delete(`/api/chat/conversations/${conversationId}`);
      return response.data;
    } catch (error) {
      console.error('Error deleting conversation:', error);
      throw error;
    }
  },
};

// Admin Service
export const adminService = {
  // Get users
  getUsers: async () => {
    try {
      const response = await api.get('/api/admin/users');
      return response.data.users || [];
    } catch (error) {
      console.error('Error fetching users:', error);
      throw error;
    }
  },
  
  // Create user
  createUser: async (userData) => {
    try {
      const response = await api.post('/api/admin/users', userData);
      return response.data;
    } catch (error) {
      console.error('Error creating user:', error);
      throw error;
    }
  },
  
  // Update user
  updateUser: async (userId, userData) => {
    try {
      const response = await api.put(`/api/admin/users/${userId}`, userData);
      return response.data;
    } catch (error) {
      console.error('Error updating user:', error);
      throw error;
    }
  },
  
  // Delete user
  deleteUser: async (userId) => {
    try {
      const response = await api.delete(`/api/admin/users/${userId}`);
      return response.data;
    } catch (error) {
      console.error('Error deleting user:', error);
      throw error;
    }
  },
};

// Health check
export const healthService = {
  check: async () => {
    try {
      const response = await api.get('/api/health');
      return response.data;
    } catch (error) {
      console.error('Health check failed:', error);
      throw error;
    }
  }
};

// Template utilities for frontend
export const templateUtils = {
  // Detect template type from message
  detectTemplateType: (message) => {
    const types = {
      vacation: ['məzuniyyət', 'vacation', 'tətil', 'istirahət'],
      business_trip: ['ezamiyyət', 'business_trip', 'səfər', 'komandirovka'],
      contract: ['müqavilə', 'contract', 'razılaşma', 'saziş'],
      memorandum: ['memorandum', 'anlaşma']
    };
    
    const messageLower = message.toLowerCase();
    
    for (const [type, keywords] of Object.entries(types)) {
      if (keywords.some(kw => messageLower.includes(kw))) {
        return type;
      }
    }
    
    return null;
  },

  // Parse download links from text
  parseDownloadLinks: (text) => {
    const patterns = [
      // Markdown links: [text](url)
      /\[([^\]]+)\]\((https?:\/\/[^\)]+)\)/g,
      // Direct URLs with context
      /📥[^:]*:[\s]*(https?:\/\/[^\s\n]+)/gi,
      // Bold download links
      /\*\*[^*]*yükləmə[^*]*linki[^*]*\*\*[\s]*(https?:\/\/[^\s\n]+)/gi,
    ];
    
    const links = [];
    
    patterns.forEach(pattern => {
      let match;
      while ((match = pattern.exec(text)) !== null) {
        if (match[2]) {
          // Markdown format
          links.push({
            text: match[1],
            url: match[2],
            type: 'markdown'
          });
        } else if (match[1]) {
          // Direct URL
          links.push({
            text: 'Yüklə',
            url: match[1],
            type: 'direct'
          });
        }
      }
    });
    
    return links;
  },

  // Generate filename from template name
  generateFilename: (templateName, extension = 'docx') => {
    const cleanName = templateName
      .toLowerCase()
      .replace(/[^a-z0-9əıöüçşğ]/g, '_')
      .replace(/_{2,}/g, '_')
      .replace(/^_|_$/g, '');
    
    return `${cleanName}_template.${extension}`;
  }
};

// Test function for debugging
export const testAPI = {
  testDocumentTypes: async () => {
    console.log('Testing document types API...');
    try {
      const types = await documentService.getDocumentTypes();
      console.log('Test result - Document types:', types);
      return types;
    } catch (error) {
      console.error('Test failed:', error);
      return null;
    }
  },

  testTemplateDownload: async () => {
    console.log('Testing template download...');
    try {
      const templates = await documentService.getTemplates();
      console.log('Available templates:', templates);
      return templates;
    } catch (error) {
      console.error('Template test failed:', error);
      return null;
    }
  }
};

export default api;
