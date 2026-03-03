import React, { useState, useEffect, useCallback } from 'react';
import { 
  Upload, Trash2, Download, RefreshCw, Search, 
  FileText, ArrowLeft, Filter, X, Check, AlertCircle, Key,
  Calendar, User, FileIcon, Eye, Settings, ChevronDown,
  ChevronUp, Grid, List, SortAsc, SortDesc
} from 'lucide-react';
import { useNavigate } from 'react-router-dom';
import { documentService } from '../services/api';
import toast from 'react-hot-toast';

const FileManagement = () => {
  const navigate = useNavigate();
  const [documents, setDocuments] = useState([]);
  const [filteredDocs, setFilteredDocs] = useState([]);
  const [searchQuery, setSearchQuery] = useState('');
  const [selectedType, setSelectedType] = useState('all');
  const [documentTypes, setDocumentTypes] = useState([]);
  const [showUploadModal, setShowUploadModal] = useState(false);
  const [uploadData, setUploadData] = useState({
    file: null,
    documentType: 'other',
    isTemplate: false
  });
  const [isLoading, setIsLoading] = useState(false);
  const [isLoadingTypes, setIsLoadingTypes] = useState(false);
  const [reprocessingDocs, setReprocessingDocs] = useState(new Set());
  const [expandedDocs, setExpandedDocs] = useState(new Set());
  const [documentKeywords, setDocumentKeywords] = useState({});
  
  // New state for enhanced features
  const [viewMode, setViewMode] = useState('grid'); // 'grid' or 'list'
  const [sortBy, setSortBy] = useState('name'); // 'name', 'date', 'size', 'type'
  const [sortOrder, setSortOrder] = useState('asc'); // 'asc' or 'desc'
  const [selectedDocs, setSelectedDocs] = useState(new Set());
  const [showFilters, setShowFilters] = useState(false);
  const [dateRange, setDateRange] = useState({ start: '', end: '' });
  const [dragOver, setDragOver] = useState(false);

  // Default document types
  const defaultDocumentTypes = [
    {'value': 'contact', 'label': 'Əlaqə məlumatları'},
    {'value': 'contract', 'label': 'Müqavilə'},
    {'value': 'vacation', 'label': 'Məzuniyyət'},
    {'value': 'business_trip', 'label': 'Ezamiyyət'},
    {'value': 'memorandum', 'label': 'Anlaşma memorandumu'},
    {'value': 'report', 'label': 'Hesabat'},
    {'value': 'letter', 'label': 'Məktub'},
    {'value': 'invoice', 'label': 'Qaimə'},
    {'value': 'other', 'label': 'Digər'}
  ];

  useEffect(() => {
    loadDocuments();
    loadDocumentTypes();
  }, []);

  useEffect(() => {
    filterAndSortDocuments();
  }, [searchQuery, selectedType, documents, sortBy, sortOrder, dateRange]);

  const loadDocuments = async () => {
    setIsLoading(true);
    try {
      const docs = await documentService.getDocuments();
      setDocuments(docs);
    } catch (error) {
      toast.error('Sənədlər yüklənmədi');
      console.error('Load documents error:', error);
    } finally {
      setIsLoading(false);
    }
  };

  const loadDocumentTypes = async () => {
    setIsLoadingTypes(true);
    try {
      if (typeof documentService.getDocumentTypes !== 'function') {
        console.warn('documentService.getDocumentTypes is not a function, using defaults');
        setDocumentTypes(defaultDocumentTypes);
        return;
      }

      const types = await documentService.getDocumentTypes();
      if (types && types.length > 0) {
        setDocumentTypes(types);
      } else {
        setDocumentTypes(defaultDocumentTypes);
      }
    } catch (error) {
      console.error('Failed to load document types:', error);
      setDocumentTypes(defaultDocumentTypes);
    } finally {
      setIsLoadingTypes(false);
    }
  };

  const filterAndSortDocuments = useCallback(async () => {
    let filtered = [...documents];

    // Search filtering
    if (searchQuery) {
      try {
        const searchResults = await documentService.searchDocuments?.(searchQuery);
        if (searchResults) {
          filtered = searchResults;
        } else {
          throw new Error('Search endpoint not available');
        }
      } catch (error) {
        filtered = documents.filter(doc =>
          doc.name.toLowerCase().includes(searchQuery.toLowerCase()) ||
          doc.uploaded_by.toLowerCase().includes(searchQuery.toLowerCase())
        );
      }
    }

    // Type filtering
    if (selectedType !== 'all') {
      filtered = filtered.filter(doc => doc.document_type === selectedType);
    }

    // Date range filtering
    if (dateRange.start || dateRange.end) {
      filtered = filtered.filter(doc => {
        const docDate = new Date(doc.upload_date);
        const startDate = dateRange.start ? new Date(dateRange.start) : null;
        const endDate = dateRange.end ? new Date(dateRange.end) : null;
        
        if (startDate && docDate < startDate) return false;
        if (endDate && docDate > endDate) return false;
        return true;
      });
    }

    // Sorting
    filtered.sort((a, b) => {
      let aValue, bValue;
      
      switch (sortBy) {
        case 'name':
          aValue = a.name.toLowerCase();
          bValue = b.name.toLowerCase();
          break;
        case 'date':
          aValue = new Date(a.upload_date);
          bValue = new Date(b.upload_date);
          break;
        case 'size':
          aValue = a.size;
          bValue = b.size;
          break;
        case 'type':
          aValue = getTypeLabel(a.document_type);
          bValue = getTypeLabel(b.document_type);
          break;
        default:
          aValue = a.name.toLowerCase();
          bValue = b.name.toLowerCase();
      }

      if (aValue < bValue) return sortOrder === 'asc' ? -1 : 1;
      if (aValue > bValue) return sortOrder === 'asc' ? 1 : -1;
      return 0;
    });

    setFilteredDocs(filtered);
  }, [documents, searchQuery, selectedType, sortBy, sortOrder, dateRange]);

  // Drag and drop handlers
  const handleDragOver = (e) => {
    e.preventDefault();
    setDragOver(true);
  };

  const handleDragLeave = (e) => {
    e.preventDefault();
    setDragOver(false);
  };

  const handleDrop = (e) => {
    e.preventDefault();
    setDragOver(false);
    
    const files = Array.from(e.dataTransfer.files);
    if (files.length > 0) {
      setUploadData({ ...uploadData, file: files[0] });
      setShowUploadModal(true);
    }
  };

  const handleFileSelect = (e) => {
    const file = e.target.files?.[0];
    if (file) {
      setUploadData({ ...uploadData, file });
    }
  };

  const handleUpload = async () => {
    if (!uploadData.file) {
      toast.error('Fayl seçin');
      return;
    }

    if (!uploadData.documentType) {
      toast.error('Sənəd növünü seçin');
      return;
    }

    setIsLoading(true);
    try {
      const result = await documentService.uploadDocument(
        uploadData.file,
        uploadData.documentType,
        uploadData.isTemplate
      );
      toast.success('Fayl yükləndi və işləndi');
      await loadDocuments();
      setShowUploadModal(false);
      setUploadData({ file: null, documentType: 'other', isTemplate: false });
    } catch (error) {
      console.error('Upload error:', error);
      const errMsg = error.response?.data?.error;
      toast.error(typeof errMsg === 'string' ? errMsg : 'Yükləmədə xətası');
    } finally {
      setIsLoading(false);
    }
  };

  const handleBulkDelete = async () => {
    if (selectedDocs.size === 0) {
      toast.error('Sənəd seçin');
      return;
    }

    if (!window.confirm(`${selectedDocs.size} sənədi silmək istəyirsiniz?`)) {
      return;
    }

    setIsLoading(true);
    try {
      const promises = Array.from(selectedDocs).map(docId => 
        documentService.deleteDocument(docId)
      );
      
      await Promise.all(promises);
      toast.success(`${selectedDocs.size} sənəd silindi`);
      setSelectedDocs(new Set());
      await loadDocuments();
    } catch (error) {
      toast.error('Silmə xətası');
    } finally {
      setIsLoading(false);
    }
  };

  const handleSelectAll = () => {
    if (selectedDocs.size === filteredDocs.length) {
      setSelectedDocs(new Set());
    } else {
      setSelectedDocs(new Set(filteredDocs.map(doc => doc.id)));
    }
  };

  const handleSelectDoc = (docId) => {
    const newSelected = new Set(selectedDocs);
    if (newSelected.has(docId)) {
      newSelected.delete(docId);
    } else {
      newSelected.add(docId);
    }
    setSelectedDocs(newSelected);
  };

  const handleDelete = async (docId, docName) => {
    if (!window.confirm(`"${docName}" sənədini silmək istədiyinizə əminsiniz?`)) {
      return;
    }

    try {
      await documentService.deleteDocument(docId);
      toast.success('Sənəd silindi');
      await loadDocuments();
    } catch (error) {
      toast.error('Silmə xətası');
    }
  };

  const handleDownload = async (docId, docName) => {
    try {
      console.log(`Starting download for: ${docName} (ID: ${docId})`);
      
      const blob = await documentService.downloadDocument(docId);
      
      if (!blob || blob.size === 0) {
        throw new Error('Boş fayl alındı');
      }
      
      const url = window.URL.createObjectURL(blob);
      const a = document.createElement('a');
      a.href = url;
      a.download = docName;
      a.style.display = 'none';
      
      document.body.appendChild(a);
      a.click();
      
      setTimeout(() => {
        window.URL.revokeObjectURL(url);
        document.body.removeChild(a);
      }, 100);
      
      toast.success('Fayl yükləndi');
    } catch (error) {
      console.error('Download error:', error);
      toast.error(error.message || 'Yükləmə xətası');
    }
  };

  const handleReprocess = async (docId, docName) => {
    setReprocessingDocs(prev => new Set([...prev, docId]));
    
    try {
      const response = await fetch(`http://localhost:5000/api/documents/${docId}/reprocess`, {
        method: 'POST',
        credentials: 'include',
        headers: {
          'Content-Type': 'application/json'
        }
      });

      const data = await response.json();
      
      if (response.ok) {
        toast.success(`"${docName}" uğurla yenidən işləndi`);
        
        if (data.document?.top_keywords) {
          setDocumentKeywords(prev => ({
            ...prev,
            [docId]: data.document.top_keywords
          }));
          
          setExpandedDocs(prev => new Set([...prev, docId]));
        }
        
        await loadDocuments();
      } else {
        const errMsg = data.error;
        toast.error(typeof errMsg === 'string' ? errMsg : 'Reprocess xətası');
      }
    } catch (error) {
      console.error('Reprocess error:', error);
      toast.error('İşləmə xətası: ' + (error.message || ''));
    } finally {
      setReprocessingDocs(prev => {
        const newSet = new Set(prev);
        newSet.delete(docId);
        return newSet;
      });
    }
  };

  const handleBulkReprocess = async () => {
    if (!window.confirm('Bütün sənədləri yenidən işləmək istəyirsiniz?')) {
      return;
    }

    setIsLoading(true);
    try {
      const response = await fetch('http://localhost:5000/api/admin/documents/bulk-reprocess', {
        method: 'POST',
        credentials: 'include',
        headers: {
          'Content-Type': 'application/json'
        },
        body: JSON.stringify({ document_ids: [] })
      });

      const data = await response.json();
      
      if (response.ok) {
        toast.success(typeof data.message === 'string' ? data.message : 'Bütün sənədlər işləndi');
        await loadDocuments();
      } else {
        const errMsg = data.error;
        toast.error(typeof errMsg === 'string' ? errMsg : 'Bulk reprocess xətası');
      }
    } catch (error) {
      toast.error('Bulk reprocess xətası: ' + (error.message || ''));
    } finally {
      setIsLoading(false);
    }
  };

  const fetchKeywords = async (docId) => {
    try {
      const response = await fetch(`http://localhost:5000/api/documents/${docId}/keywords`, {
        credentials: 'include'
      });

      const data = await response.json();
      if (response.ok && data.keywords) {
        setDocumentKeywords(prev => ({
          ...prev,
          [docId]: data.keywords
        }));
      }
    } catch (error) {
      console.error('Keywords fetch error:', error);
    }
  };

  const toggleDocExpand = (docId) => {
    setExpandedDocs(prev => {
      const newSet = new Set(prev);
      if (newSet.has(docId)) {
        newSet.delete(docId);
      } else {
        newSet.add(docId);
        if (!documentKeywords[docId]) {
          fetchKeywords(docId);
        }
      }
      return newSet;
    });
  };

  const getFileIcon = (fileName) => {
    const ext = fileName?.split('.').pop()?.toLowerCase();
    const icons = {
      'pdf': '📄', 'docx': '📘', 'txt': '📃',
      'md': '📋', 'json': '🔧', 'xlsx': '📊', 'xls': '📊'
    };
    return icons[ext] || '📎';
  };

  const getTypeLabel = (type) => {
    const typeObj = documentTypes.find(t => t.value === type);
    return typeObj?.label || type;
  };

  const getTypeColor = (type) => {
    const colors = {
      'contact': 'bg-blue-100 text-blue-800',
      'contract': 'bg-purple-100 text-purple-800',
      'vacation': 'bg-green-100 text-green-800',
      'business_trip': 'bg-orange-100 text-orange-800',
      'memorandum': 'bg-pink-100 text-pink-800',
      'report': 'bg-indigo-100 text-indigo-800',
      'letter': 'bg-yellow-100 text-yellow-800',
      'invoice': 'bg-red-100 text-red-800',
      'other': 'bg-gray-100 text-gray-800'
    };
    return colors[type] || colors.other;
  };

  const formatFileSize = (bytes) => {
    if (bytes === 0) return '0 Bytes';
    const k = 1024;
    const sizes = ['Bytes', 'KB', 'MB', 'GB'];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
  };

  const formatDate = (dateString) => {
    return new Date(dateString).toLocaleDateString('az-AZ', {
      year: 'numeric',
      month: 'short',
      day: 'numeric'
    });
  };

  return (
    <div 
      className={`min-h-screen bg-gray-50 ${dragOver ? 'bg-blue-50' : ''}`}
      onDragOver={handleDragOver}
      onDragLeave={handleDragLeave}
      onDrop={handleDrop}
    >
      {dragOver && (
        <div className="fixed inset-0 bg-blue-500 bg-opacity-20 border-4 border-dashed border-blue-400 z-40 flex items-center justify-center">
          <div className="bg-white p-8 rounded-lg shadow-lg text-center">
            <Upload className="w-12 h-12 text-blue-500 mx-auto mb-4" />
            <p className="text-lg font-semibold text-gray-700">Faylı buraya atın</p>
          </div>
        </div>
      )}

      {/* Header */}
      <div className="bg-white border-b">
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
          <div className="flex justify-between items-center py-4">
            <div className="flex items-center space-x-4">
              <button
                onClick={() => navigate('/dashboard')}
                className="text-gray-500 hover:text-gray-700 transition-colors"
              >
                <ArrowLeft className="w-6 h-6" />
              </button>
              <div>
                <h1 className="text-xl font-bold text-gray-900">Sənədləri İdarə Et</h1>
                <p className="text-sm text-gray-500">
                  {documents.length} sənəd • {selectedDocs.size > 0 && `${selectedDocs.size} seçilmiş`}
                </p>
              </div>
            </div>
            <div className="flex items-center space-x-2">
              {selectedDocs.size > 0 && (
                <button
                  onClick={handleBulkDelete}
                  className="bg-red-100 hover:bg-red-200 text-red-700 px-4 py-2 rounded-lg flex items-center space-x-2 transition-colors"
                  disabled={isLoading}
                >
                  <Trash2 className="w-4 h-4" />
                  <span>Seçilmişləri Sil</span>
                </button>
              )}
              <button
                onClick={handleBulkReprocess}
                className="bg-purple-100 hover:bg-purple-200 text-purple-700 px-4 py-2 rounded-lg flex items-center space-x-2 transition-colors"
                disabled={isLoading}
              >
                <RefreshCw className="w-4 h-4" />
                <span>Hamısını İşlə</span>
              </button>
              <button
                onClick={() => setShowUploadModal(true)}
                className="bg-blue-600 hover:bg-blue-700 text-white px-4 py-2 rounded-lg flex items-center space-x-2 transition-colors"
              >
                <Upload className="w-4 h-4" />
                <span>Yeni Sənəd</span>
              </button>
            </div>
          </div>
        </div>
      </div>

      {/* Enhanced Filters */}
      <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-4">
        <div className="bg-white rounded-lg shadow-sm p-4">
          <div className="flex flex-wrap gap-4 items-center">
            <div className="flex-1 min-w-[200px]">
              <div className="relative">
                <Search className="absolute left-3 top-1/2 transform -translate-y-1/2 w-4 h-4 text-gray-400" />
                <input
                  type="text"
                  placeholder="Sənəd axtar..."
                  value={searchQuery}
                  onChange={(e) => setSearchQuery(e.target.value)}
                  className="w-full pl-9 pr-3 py-2 border border-gray-300 rounded-lg focus:outline-none focus:ring-2 focus:ring-blue-500 transition-colors"
                />
              </div>
            </div>
            
            <select
              value={selectedType}
              onChange={(e) => setSelectedType(e.target.value)}
              disabled={isLoadingTypes}
              className="px-4 py-2 border border-gray-300 rounded-lg focus:outline-none focus:ring-2 focus:ring-blue-500 transition-colors"
            >
              <option value="all">Bütün növlər</option>
              {documentTypes.map(type => (
                <option key={type.value} value={type.value}>{type.label}</option>
              ))}
            </select>

            <button
              onClick={() => setShowFilters(!showFilters)}
              className={`px-4 py-2 border rounded-lg flex items-center space-x-2 transition-colors ${
                showFilters ? 'bg-blue-50 border-blue-300 text-blue-700' : 'border-gray-300 hover:bg-gray-50'
              }`}
            >
              <Filter className="w-4 h-4" />
              <span>Filtreler</span>
              {showFilters ? <ChevronUp className="w-4 h-4" /> : <ChevronDown className="w-4 h-4" />}
            </button>

            <div className="flex items-center space-x-2 border-l pl-4">
              <button
                onClick={() => setViewMode('grid')}
                className={`p-2 rounded-lg transition-colors ${
                  viewMode === 'grid' ? 'bg-blue-100 text-blue-600' : 'text-gray-500 hover:bg-gray-100'
                }`}
              >
                <Grid className="w-4 h-4" />
              </button>
              <button
                onClick={() => setViewMode('list')}
                className={`p-2 rounded-lg transition-colors ${
                  viewMode === 'list' ? 'bg-blue-100 text-blue-600' : 'text-gray-500 hover:bg-gray-100'
                }`}
              >
                <List className="w-4 h-4" />
              </button>
            </div>

            <select
              value={`${sortBy}-${sortOrder}`}
              onChange={(e) => {
                const [field, order] = e.target.value.split('-');
                setSortBy(field);
                setSortOrder(order);
              }}
              className="px-3 py-2 border border-gray-300 rounded-lg focus:outline-none focus:ring-2 focus:ring-blue-500 text-sm"
            >
              <option value="name-asc">Ad ↑</option>
              <option value="name-desc">Ad ↓</option>
              <option value="date-desc">Tarix ↓</option>
              <option value="date-asc">Tarix ↑</option>
              <option value="size-desc">Ölçü ↓</option>
              <option value="size-asc">Ölçü ↑</option>
              <option value="type-asc">Növ ↑</option>
            </select>
          </div>

          {showFilters && (
            <div className="mt-4 pt-4 border-t border-gray-200 flex flex-wrap gap-4">
              <div className="flex items-center space-x-2">
                <label className="text-sm font-medium text-gray-700">Tarix aralığı:</label>
                <input
                  type="date"
                  value={dateRange.start}
                  onChange={(e) => setDateRange({ ...dateRange, start: e.target.value })}
                  className="px-3 py-1 border border-gray-300 rounded-lg text-sm focus:outline-none focus:ring-2 focus:ring-blue-500"
                />
                <span className="text-gray-500">-</span>
                <input
                  type="date"
                  value={dateRange.end}
                  onChange={(e) => setDateRange({ ...dateRange, end: e.target.value })}
                  className="px-3 py-1 border border-gray-300 rounded-lg text-sm focus:outline-none focus:ring-2 focus:ring-blue-500"
                />
              </div>
              
              {(dateRange.start || dateRange.end) && (
                <button
                  onClick={() => setDateRange({ start: '', end: '' })}
                  className="text-sm text-blue-600 hover:text-blue-700 flex items-center space-x-1"
                >
                  <X className="w-3 h-3" />
                  <span>Tarix filtrini təmizlə</span>
                </button>
              )}
            </div>
          )}
        </div>
      </div>

      {/* Documents Display */}
      <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 pb-8">
        {isLoading ? (
          <div className="text-center py-12">
            <div className="animate-spin rounded-full h-12 w-12 border-b-2 border-blue-600 mx-auto"></div>
            <p className="text-gray-500 mt-4">Yüklənir...</p>
          </div>
        ) : filteredDocs.length === 0 ? (
          <div className="bg-white rounded-lg shadow-sm p-12 text-center">
            <FileText className="w-16 h-16 text-gray-300 mx-auto mb-4" />
            <p className="text-gray-500 text-lg mb-2">Heç bir sənəd tapılmadı</p>
            <p className="text-gray-400 text-sm">
              {searchQuery || selectedType !== 'all' || dateRange.start || dateRange.end
                ? 'Filtr şərtlərinizi dəyişin və ya yeni sənəd yükləyin'
                : 'İlk sənədlərinizi yükləyin'}
            </p>
          </div>
        ) : (
          <>
            {/* Bulk Selection Header */}
            {filteredDocs.length > 0 && (
              <div className="bg-white rounded-lg shadow-sm p-4 mb-4 flex items-center justify-between">
                <label className="flex items-center space-x-3 cursor-pointer">
                  <input
                    type="checkbox"
                    checked={selectedDocs.size === filteredDocs.length && filteredDocs.length > 0}
                    onChange={handleSelectAll}
                    className="h-4 w-4 text-blue-600 focus:ring-blue-500 border-gray-300 rounded"
                  />
                  <span className="text-sm font-medium text-gray-700">
                    {selectedDocs.size === 0 
                      ? 'Hamısını seç' 
                      : selectedDocs.size === filteredDocs.length 
                      ? 'Seçimi ləğv et'
                      : `${selectedDocs.size} sənəd seçilmişdir`
                    }
                  </span>
                </label>
                
                <div className="text-sm text-gray-500">
                  {filteredDocs.length} sənəddən {selectedDocs.size} seçilmiş
                </div>
              </div>
            )}

            {viewMode === 'grid' ? (
              <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
                {filteredDocs.map(doc => (
                  <div key={doc.id} className={`bg-white rounded-lg shadow-sm hover:shadow-md transition-all duration-200 ${selectedDocs.has(doc.id) ? 'ring-2 ring-blue-500 bg-blue-50' : ''}`}>
                    <div className="p-4">
                      <div className="flex items-start justify-between mb-3">
                        <div className="flex items-center space-x-3 flex-1">
                          <input
                            type="checkbox"
                            checked={selectedDocs.has(doc.id)}
                            onChange={() => handleSelectDoc(doc.id)}
                            className="h-4 w-4 text-blue-600 focus:ring-blue-500 border-gray-300 rounded"
                          />
                          <span className="text-2xl flex-shrink-0">{getFileIcon(doc.name)}</span>
                          <div className="flex-1 min-w-0">
                            <h3 className="text-sm font-medium text-gray-900 truncate" title={doc.name}>
                              {doc.name}
                            </h3>
                            <p className="text-xs text-gray-500">
                              {formatFileSize(doc.size)} • {doc.uploaded_by}
                            </p>
                            <p className="text-xs text-gray-400">
                              {formatDate(doc.upload_date)}
                            </p>
                          </div>
                        </div>
                        <div className="flex items-center space-x-1">
                          {doc.is_processed ? (
                            <Check className="w-4 h-4 text-green-500" title="İşlənib" />
                          ) : (
                            <AlertCircle className="w-4 h-4 text-orange-500" title="İşlənməyib" />
                          )}
                        </div>
                      </div>
                      
                      <div className="flex items-center justify-between mb-3">
                        <span className={`inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium ${getTypeColor(doc.document_type)}`}>
                          {getTypeLabel(doc.document_type)}
                        </span>
                        {doc.is_template && (
                          <span className="text-xs bg-blue-100 text-blue-800 px-2 py-1 rounded">
                            Şablon
                          </span>
                        )}
                      </div>
                      
                      <div className="flex space-x-1">
                        <button
                          onClick={() => toggleDocExpand(doc.id)}
                          className="flex-1 bg-gray-100 hover:bg-gray-200 text-gray-700 py-2 px-2 rounded text-sm flex items-center justify-center transition-colors"
                          title="Açar sözlər"
                        >
                          <Key className="w-4 h-4" />
                        </button>
                        
                        <button
                          onClick={() => handleDownload(doc.id, doc.name)}
                          className="flex-1 bg-blue-100 hover:bg-blue-200 text-blue-700 py-2 px-2 rounded text-sm flex items-center justify-center transition-colors"
                          title="Yüklə"
                        >
                          <Download className="w-4 h-4" />
                        </button>
                        
                        <button
                          onClick={() => handleReprocess(doc.id, doc.name)}
                          disabled={reprocessingDocs.has(doc.id)}
                          className="flex-1 bg-purple-100 hover:bg-purple-200 text-purple-700 py-2 px-2 rounded text-sm flex items-center justify-center disabled:opacity-50 disabled:cursor-not-allowed transition-colors"
                          title="Yenidən işlə"
                        >
                          {reprocessingDocs.has(doc.id) ? (
                            <div className="animate-spin">
                              <RefreshCw className="w-4 h-4" />
                            </div>
                          ) : (
                            <RefreshCw className="w-4 h-4" />
                          )}
                        </button>
                        
                        <button
                          onClick={() => handleDelete(doc.id, doc.name)}
                          className="flex-1 bg-red-100 hover:bg-red-200 text-red-700 py-2 px-2 rounded text-sm flex items-center justify-center transition-colors"
                          title="Sil"
                        >
                          <Trash2 className="w-4 h-4" />
                        </button>
                      </div>

                      {/* Expanded Keywords Section */}
                      {expandedDocs.has(doc.id) && (
                        <div className="mt-3 pt-3 border-t border-gray-200">
                          <div className="flex items-center mb-2">
                            <Key className="w-4 h-4 text-gray-600 mr-1" />
                            <span className="text-xs font-medium text-gray-700">Açar Sözlər</span>
                          </div>
                          {documentKeywords[doc.id] ? (
                            <div className="flex flex-wrap gap-1">
                              {documentKeywords[doc.id].slice(0, 10).map((keyword, idx) => (
                                <span
                                  key={idx}
                                  className="inline-block px-2 py-0.5 text-xs bg-gray-100 text-gray-600 rounded-full hover:bg-gray-200 transition-colors"
                                >
                                  {keyword}
                                </span>
                              ))}
                              {documentKeywords[doc.id].length > 10 && (
                                <span className="text-xs text-gray-400 italic">
                                  +{documentKeywords[doc.id].length - 10} daha
                                </span>
                              )}
                            </div>
                          ) : (
                            <div className="flex items-center space-x-2">
                              <div className="animate-spin rounded-full h-4 w-4 border-b-2 border-gray-400"></div>
                              <p className="text-xs text-gray-400 italic">Yüklənir...</p>
                            </div>
                          )}
                        </div>
                      )}
                    </div>
                  </div>
                ))}
              </div>
            ) : (
              // List View
              <div className="bg-white rounded-lg shadow-sm overflow-hidden">
                <div className="px-6 py-3 bg-gray-50 border-b border-gray-200">
                  <div className="flex items-center space-x-4 text-xs font-medium text-gray-700 uppercase tracking-wider">
                    <div className="w-8"></div>
                    <div className="flex-1 min-w-0">Ad</div>
                    <div className="w-24 text-center">Növ</div>
                    <div className="w-20 text-center">Ölçü</div>
                    <div className="w-24 text-center">Tarix</div>
                    <div className="w-24 text-center">Status</div>
                    <div className="w-32 text-center">Əməliyyatlar</div>
                  </div>
                </div>
                
                <div className="divide-y divide-gray-200">
                  {filteredDocs.map(doc => (
                    <div key={doc.id} className={`px-6 py-4 hover:bg-gray-50 transition-colors ${selectedDocs.has(doc.id) ? 'bg-blue-50' : ''}`}>
                      <div className="flex items-center space-x-4">
                        <input
                          type="checkbox"
                          checked={selectedDocs.has(doc.id)}
                          onChange={() => handleSelectDoc(doc.id)}
                          className="h-4 w-4 text-blue-600 focus:ring-blue-500 border-gray-300 rounded"
                        />
                        
                        <div className="flex items-center space-x-3 flex-1 min-w-0">
                          <span className="text-xl">{getFileIcon(doc.name)}</span>
                          <div className="flex-1 min-w-0">
                            <p className="text-sm font-medium text-gray-900 truncate" title={doc.name}>
                              {doc.name}
                            </p>
                            <p className="text-xs text-gray-500">{doc.uploaded_by}</p>
                          </div>
                        </div>
                        
                        <div className="w-24 text-center">
                          <span className={`inline-flex items-center px-2 py-0.5 rounded-full text-xs font-medium ${getTypeColor(doc.document_type)}`}>
                            {getTypeLabel(doc.document_type)}
                          </span>
                        </div>
                        
                        <div className="w-20 text-center text-sm text-gray-500">
                          {formatFileSize(doc.size)}
                        </div>
                        
                        <div className="w-24 text-center text-sm text-gray-500">
                          {formatDate(doc.upload_date)}
                        </div>
                        
                        <div className="w-24 text-center">
                          {doc.is_processed ? (
                            <span className="inline-flex items-center px-2 py-0.5 rounded-full text-xs font-medium bg-green-100 text-green-800">
                              <Check className="w-3 h-3 mr-1" />
                              İşlənib
                            </span>
                          ) : (
                            <span className="inline-flex items-center px-2 py-0.5 rounded-full text-xs font-medium bg-orange-100 text-orange-800">
                              <AlertCircle className="w-3 h-3 mr-1" />
                              Gözləyir
                            </span>
                          )}
                        </div>
                        
                        <div className="w-32 flex items-center justify-center space-x-1">
                          <button
                            onClick={() => toggleDocExpand(doc.id)}
                            className="p-1 bg-gray-100 hover:bg-gray-200 text-gray-700 rounded transition-colors"
                            title="Açar sözlər"
                          >
                            <Key className="w-3 h-3" />
                          </button>
                          
                          <button
                            onClick={() => handleDownload(doc.id, doc.name)}
                            className="p-1 bg-blue-100 hover:bg-blue-200 text-blue-700 rounded transition-colors"
                            title="Yüklə"
                          >
                            <Download className="w-3 h-3" />
                          </button>
                          
                          <button
                            onClick={() => handleReprocess(doc.id, doc.name)}
                            disabled={reprocessingDocs.has(doc.id)}
                            className="p-1 bg-purple-100 hover:bg-purple-200 text-purple-700 rounded disabled:opacity-50 disabled:cursor-not-allowed transition-colors"
                            title="Yenidən işlə"
                          >
                            {reprocessingDocs.has(doc.id) ? (
                              <div className="animate-spin">
                                <RefreshCw className="w-3 h-3" />
                              </div>
                            ) : (
                              <RefreshCw className="w-3 h-3" />
                            )}
                          </button>
                          
                          <button
                            onClick={() => handleDelete(doc.id, doc.name)}
                            className="p-1 bg-red-100 hover:bg-red-200 text-red-700 rounded transition-colors"
                            title="Sil"
                          >
                            <Trash2 className="w-3 h-3" />
                          </button>
                        </div>
                      </div>
                      
                      {/* Expanded Keywords Section for List View */}
                      {expandedDocs.has(doc.id) && (
                        <div className="mt-3 ml-11 pt-3 border-t border-gray-200">
                          <div className="flex items-center mb-2">
                            <Key className="w-4 h-4 text-gray-600 mr-1" />
                            <span className="text-sm font-medium text-gray-700">Açar Sözlər</span>
                          </div>
                          {documentKeywords[doc.id] ? (
                            <div className="flex flex-wrap gap-1">
                              {documentKeywords[doc.id].slice(0, 15).map((keyword, idx) => (
                                <span
                                  key={idx}
                                  className="inline-block px-2 py-0.5 text-xs bg-gray-100 text-gray-600 rounded-full hover:bg-gray-200 transition-colors"
                                >
                                  {keyword}
                                </span>
                              ))}
                              {documentKeywords[doc.id].length > 15 && (
                                <span className="text-xs text-gray-400 italic">
                                  +{documentKeywords[doc.id].length - 15} daha
                                </span>
                              )}
                            </div>
                          ) : (
                            <div className="flex items-center space-x-2">
                              <div className="animate-spin rounded-full h-4 w-4 border-b-2 border-gray-400"></div>
                              <p className="text-xs text-gray-400 italic">Yüklənir...</p>
                            </div>
                          )}
                        </div>
                      )}
                    </div>
                  ))}
                </div>
              </div>
            )}
          </>
        )}
      </div>

      {/* Enhanced Upload Modal */}
      {showUploadModal && (
        <div className="fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center p-4 z-50">
          <div className="bg-white rounded-lg max-w-md w-full p-6 max-h-[90vh] overflow-y-auto">
            <div className="flex justify-between items-center mb-6">
              <h2 className="text-xl font-semibold text-gray-900">Yeni Sənəd Yüklə</h2>
              <button
                onClick={() => setShowUploadModal(false)}
                className="text-gray-400 hover:text-gray-600 transition-colors"
              >
                <X className="w-6 h-6" />
              </button>
            </div>

            <div className="space-y-6">
              <div>
                <label className="block text-sm font-medium text-gray-700 mb-2">
                  Fayl Seçin
                </label>
                
                <div className="border-2 border-dashed border-gray-300 rounded-lg p-6 hover:border-gray-400 transition-colors">
                  <div className="text-center">
                    <Upload className="mx-auto h-12 w-12 text-gray-400" />
                    <div className="mt-2">
                      <label htmlFor="file-upload" className="cursor-pointer">
                        <span className="mt-2 block text-sm font-medium text-gray-900">
                          Faylı seçin və ya buraya atın
                        </span>
                        <input
                          id="file-upload"
                          type="file"
                          onChange={handleFileSelect}
                          accept=".pdf,.docx,.txt,.md,.json,.xlsx,.xls"
                          className="sr-only"
                        />
                      </label>
                    </div>
                    <p className="mt-1 text-xs text-gray-500">
                      PDF, DOCX, TXT, MD, JSON, XLSX dəstəklənir (max 10MB)
                    </p>
                  </div>
                </div>
                
                {uploadData.file && (
                  <div className="mt-3 p-3 bg-blue-50 rounded-lg">
                    <div className="flex items-center space-x-2">
                      <span className="text-lg">{getFileIcon(uploadData.file.name)}</span>
                      <div className="flex-1 min-w-0">
                        <p className="text-sm font-medium text-gray-900 truncate">
                          {uploadData.file.name}
                        </p>
                        <p className="text-xs text-gray-500">
                          {formatFileSize(uploadData.file.size)}
                        </p>
                      </div>
                      <button
                        onClick={() => setUploadData({ ...uploadData, file: null })}
                        className="text-gray-400 hover:text-red-500 transition-colors"
                      >
                        <X className="w-4 h-4" />
                      </button>
                    </div>
                  </div>
                )}
              </div>

              <div>
                <label className="block text-sm font-medium text-gray-700 mb-2">
                  Sənəd Növü
                </label>
                <select
                  value={uploadData.documentType}
                  onChange={(e) => setUploadData({ ...uploadData, documentType: e.target.value })}
                  className="w-full px-3 py-2 border border-gray-300 rounded-lg focus:outline-none focus:ring-2 focus:ring-blue-500 transition-colors"
                >
                  {documentTypes.map(type => (
                    <option key={type.value} value={type.value}>{type.label}</option>
                  ))}
                </select>
              </div>

              <div className="flex items-center space-x-3">
                <input
                  type="checkbox"
                  id="isTemplate"
                  checked={uploadData.isTemplate}
                  onChange={(e) => setUploadData({ ...uploadData, isTemplate: e.target.checked })}
                  className="h-4 w-4 text-blue-600 focus:ring-blue-500 border-gray-300 rounded"
                />
                <label htmlFor="isTemplate" className="text-sm text-gray-700 flex items-center space-x-2">
                  <Settings className="w-4 h-4 text-gray-500" />
                  <span>Bu bir şablon sənəddir</span>
                </label>
              </div>

              {uploadData.isTemplate && (
                <div className="p-3 bg-yellow-50 border border-yellow-200 rounded-lg">
                  <div className="flex items-start space-x-2">
                    <AlertCircle className="w-4 h-4 text-yellow-600 mt-0.5" />
                    <div className="text-xs text-yellow-700">
                      <p className="font-medium mb-1">Şablon sənəd haqqında:</p>
                      <p>Şablon sənədlər digər sənədlər üçün nümunə kimi istifadə olunur və axtarış nəticələrində prioritet verilir.</p>
                    </div>
                  </div>
                </div>
              )}
            </div>

            <div className="flex space-x-3 mt-8">
              <button
                onClick={() => setShowUploadModal(false)}
                className="flex-1 bg-gray-100 hover:bg-gray-200 text-gray-700 py-3 px-4 rounded-lg font-medium transition-colors"
                disabled={isLoading}
              >
                Ləğv et
              </button>
              <button
                onClick={handleUpload}
                className="flex-1 bg-blue-600 hover:bg-blue-700 text-white py-3 px-4 rounded-lg font-medium flex items-center justify-center disabled:opacity-50 disabled:cursor-not-allowed transition-colors"
                disabled={isLoading || !uploadData.file}
              >
                {isLoading ? (
                  <>
                    <div className="animate-spin rounded-full h-4 w-4 border-b-2 border-white mr-2"></div>
                    İşlənir...
                  </>
                ) : (
                  <>
                    <Upload className="w-4 h-4 mr-2" />
                    Yüklə və İşlə
                  </>
                )}
              </button>
            </div>
          </div>
        </div>
      )}
    </div>
  );
};

export default FileManagement;
