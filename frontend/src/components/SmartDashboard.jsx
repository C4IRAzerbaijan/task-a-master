// src/components/SmartDashboard.jsx
import React, { useState, useEffect, useRef } from 'react';
import { 
  Send, LogOut, MessageSquare, User, Bot, AlertCircle, 
  Settings, Edit2, Trash2, X, Check, Download, ExternalLink,
  FileText
} from 'lucide-react';
import { useNavigate } from 'react-router-dom';
import useAuthStore from '../stores/authStore';
import { documentService, chatService } from '../services/api';
import toast from 'react-hot-toast';
import ReactMarkdown from 'react-markdown';

const SmartDashboard = () => {
  const navigate = useNavigate();
  const { user, logout } = useAuthStore();

  // Ensure values rendered in JSX are always strings
  const safeString = (val) => {
    if (val == null) return '';
    if (typeof val === 'string') return val;
    if (typeof val === 'object') return val.message || val.error || JSON.stringify(val);
    return String(val);
  };

  const [messages, setMessages] = useState([]);
  const [question, setQuestion] = useState('');
  const [isLoading, setIsLoading] = useState(false);
  const [conversations, setConversations] = useState([]);
  const [currentConversation, setCurrentConversation] = useState(null);
  const [editingConvId, setEditingConvId] = useState(null);
  const [editTitle, setEditTitle] = useState('');
  const messagesEndRef = useRef(null);
  const [conversationContext, setConversationContext] = useState([]);

  useEffect(() => {
    loadConversations();
  }, []);

  useEffect(() => {
    scrollToBottom();
  }, [messages]);

  const scrollToBottom = () => {
    messagesEndRef.current?.scrollIntoView({ behavior: 'smooth' });
  };

  const loadConversations = async () => {
    try {
      const convs = await chatService.getConversations();
      setConversations(convs);
    } catch (error) {
      console.error('Failed to load conversations');
    }
  };

  // Download link parsing and handling
  const parseDownloadLinks = (text) => {
    const patterns = [
      // Pattern 1: [Link Text](URL)
      /\[([^\]]+)\]\((http[s]?:\/\/[^\)]+)\)/g,
      // Pattern 2: **Yükləmə linki:** URL
      /\*\*[^*]*yükləmə[^*]*linki[^*]*\*\*[:\s]*([http][^\s\n]+)/gi,
      // Pattern 3: 📥 **Yükləmə linki:** URL  
      /📥[^:]*:[:\s]*([http][^\s\n]+)/gi,
    ];
    
    const links = [];
    
    patterns.forEach(pattern => {
      let match;
      const regex = new RegExp(pattern);
      while ((match = regex.exec(text)) !== null) {
        if (match[2]) {
          // Markdown link format [text](url)
          links.push({
            text: match[1],
            url: match[2],
            fullMatch: match[0],
            type: 'markdown'
          });
        } else if (match[1]) {
          // Direct URL
          links.push({
            text: 'Yüklə',
            url: match[1],
            fullMatch: match[0],
            type: 'direct'
          });
        }
      }
    });
    
    return links;
  };

  const handleDownloadFromLink = async (url, filename = 'template') => {
    try {
      console.log(`Downloading from URL: ${url}`);
      
      // Use the API service download handler
      const response = await fetch(url, {
        method: 'GET',
        credentials: 'include',
      });
      
      if (!response.ok) {
        throw new Error(`Download failed: ${response.status} ${response.statusText}`);
      }
      
      const blob = await response.blob();
      
      if (!blob || blob.size === 0) {
        throw new Error('Boş fayl alındı');
      }
      
      console.log(`Downloaded blob: size=${blob.size}, type=${blob.type}`);
      
      // Try to get filename from response headers
      const contentDisposition = response.headers.get('Content-Disposition');
      if (contentDisposition && contentDisposition.includes('filename=')) {
        const match = contentDisposition.match(/filename[^;=\n]*=((['"]).*?\2|[^;\n]*)/);
        if (match && match[1]) {
          filename = match[1].replace(/['"]/g, '');
        }
      }
      
      // Create download
      const downloadUrl = window.URL.createObjectURL(blob);
      const link = document.createElement('a');
      link.href = downloadUrl;
      link.download = filename;
      link.style.display = 'none';
      
      document.body.appendChild(link);
      link.click();
      
      // Cleanup
      setTimeout(() => {
        window.URL.revokeObjectURL(downloadUrl);
        document.body.removeChild(link);
      }, 100);
      
      toast.success('Fayl yükləndi');
    } catch (error) {
      console.error('Download error:', error);
      toast.error('Yükləmə xətası: ' + error.message);
    }
  };

  const renderDownloadLinks = (links) => {
    if (links.length === 0) return null;

    return (
      <div className="mt-4 p-4 bg-blue-50 border border-blue-200 rounded-lg">
        <div className="flex items-center mb-2">
          <FileText className="w-5 h-5 text-blue-600 mr-2" />
          <span className="font-medium text-blue-900">
            {links.length === 1 ? 'Sənəd yükləmə' : `${links.length} sənəd mövcuddur`}
          </span>
        </div>
        
        <div className="space-y-2">
          {links.map((link, index) => (
            <button
              key={index}
              onClick={() => handleDownloadFromLink(link.url, link.text)}
              className="w-full flex items-center justify-between p-3 bg-white border border-blue-300 rounded-lg hover:bg-blue-50 hover:border-blue-400 transition-colors group"
            >
              <div className="flex items-center">
                <Download className="w-4 h-4 text-blue-600 mr-3" />
                <span className="text-sm font-medium text-blue-900">
                  {link.text}
                </span>
              </div>
              <ExternalLink className="w-4 h-4 text-blue-500 opacity-0 group-hover:opacity-100 transition-opacity" />
            </button>
          ))}
        </div>
        
        <div className="mt-3 flex items-center text-xs text-blue-700">
          <AlertCircle className="w-3 h-3 mr-1" />
          <span>Yüklənən fayllar brauzerin default qovluğuna saxlanacaq</span>
        </div>
      </div>
    );
  };

  const renderMessageWithDownloadLinks = (content) => {
    if (!content || typeof content !== 'string') {
      return <p>{safeString(content)}</p>;
    }
    const downloadLinks = parseDownloadLinks(content);
    
    // Remove download links from content for clean markdown rendering
    let cleanContent = content;
    downloadLinks.forEach(link => {
      cleanContent = cleanContent.replace(link.fullMatch, '');
    });
    
    return (
      <div>
        <ReactMarkdown className="prose prose-sm max-w-none">
          {cleanContent.trim()}
        </ReactMarkdown>
        {renderDownloadLinks(downloadLinks)}
      </div>
    );
  };

  const handleSendMessage = async (e) => {
    e.preventDefault();
    if (!question.trim() || isLoading) return;

    const userQuestion = question.trim();
    setQuestion('');
    setIsLoading(true);

    // Add user message to UI
    const userMessage = {
      type: 'user',
      content: userQuestion,
      timestamp: new Date().toISOString()
    };
    setMessages(prev => [...prev, userMessage]);
    
    // Add to context for continuity
    setConversationContext(prev => [...prev, { role: 'user', content: userQuestion }]);

    try {
      const response = await chatService.askQuestion(
        userQuestion,
        null, // Let backend determine document automatically
        currentConversation?.id
      );

      // Check response type
      if (response.needs_clarification) {
        // Need document selection
        const clarificationMessage = {
          type: 'system',
          content: safeString(response.message),
          documents: response.available_documents,
          timestamp: new Date().toISOString()
        };
        setMessages(prev => [...prev, clarificationMessage]);
      } else {
        // Got answer (general or document-based)
        const botMessage = {
          type: 'bot',
          content: safeString(response.answer),
          document: response.document_used,
          responseType: response.type,
          timestamp: new Date().toISOString()
        };
        setMessages(prev => [...prev, botMessage]);
        
        // Add to context
        setConversationContext(prev => [...prev, { role: 'assistant', content: response.answer }]);

        // Update conversation if needed
        if (response.conversation_id) {
          if (!currentConversation || currentConversation.id !== response.conversation_id) {
            setCurrentConversation({ id: response.conversation_id });
          }
          await loadConversations();
        }
      }
    } catch (error) {
      const errorMessage = {
        type: 'error',
        content: safeString(error.response?.data?.error) || 'Xəta baş verdi',
        timestamp: new Date().toISOString()
      };
      setMessages(prev => [...prev, errorMessage]);
    } finally {
      setIsLoading(false);
    }
  };

  const handleLoadConversation = async (conv) => {
    try {
      const data = await chatService.getConversation(conv.id);
      const loadedMessages = data.messages.flatMap(msg => [
        { type: 'user', content: safeString(msg.question), timestamp: msg.timestamp },
        { type: 'bot', content: safeString(msg.answer), timestamp: msg.timestamp }
      ]);
      setMessages(loadedMessages);
      setCurrentConversation(conv);
      
      // Build context from loaded messages
      const context = data.messages.flatMap(msg => [
        { role: 'user', content: msg.question },
        { role: 'assistant', content: msg.answer }
      ]);
      setConversationContext(context);
    } catch (error) {
      toast.error('Söhbət yüklənmədi');
    }
  };

  const handleNewConversation = () => {
    setMessages([]);
    setCurrentConversation(null);
    setConversationContext([]);
  };

  const handleRenameConversation = async (convId) => {
    if (!editTitle.trim()) {
      setEditingConvId(null);
      return;
    }

    try {
      await chatService.renameConversation(convId, editTitle);
      toast.success('Söhbət adı dəyişdirildi');
      await loadConversations();
      setEditingConvId(null);
      setEditTitle('');
    } catch (error) {
      toast.error('Ad dəyişdirilə bilmədi');
    }
  };

  const handleDeleteConversation = async (convId) => {
    if (!window.confirm('Bu söhbəti silmək istədiyinizə əminsiniz?')) return;

    try {
      await chatService.deleteConversation(convId);
      toast.success('Söhbət silindi');
      
      if (currentConversation?.id === convId) {
        handleNewConversation();
      }
      
      await loadConversations();
    } catch (error) {
      toast.error('Söhbət silinə bilmədi');
    }
  };

  const handleSelectDocument = async (docId) => {
    // Re-ask the question with specific document
    const lastUserMessage = messages.filter(m => m.type === 'user').pop();
    if (lastUserMessage) {
      setIsLoading(true);
      try {
        const response = await chatService.askQuestion(
          lastUserMessage.content,
          docId,
          currentConversation?.id
        );

        const botMessage = {
          type: 'bot',
          content: safeString(response.answer),
          document: response.document_used,
          responseType: response.type,
          timestamp: new Date().toISOString()
        };
        
        // Replace system message with bot answer
        setMessages(prev => {
          const filtered = prev.filter(m => m.type !== 'system');
          return [...filtered, botMessage];
        });

      } catch (error) {
        toast.error('Xəta baş verdi');
      } finally {
        setIsLoading(false);
      }
    }
  };

  const handleLogout = async () => {
    await logout();
    navigate('/login');
    toast.success('Sistemdən çıxış edildi');
  };

  return (
    <div className="h-screen bg-gray-100 flex flex-col">
      {/* Header */}
      <div className="bg-white shadow-sm border-b">
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
          <div className="flex justify-between items-center py-4">
            <div className="flex items-center space-x-4">
              <Bot className="w-8 h-8 text-blue-600" />
              <div>
                <h1 className="text-xl font-bold text-gray-900">RAG Smart Assistant</h1>
                <p className="text-sm text-gray-500">AI Sənəd Assistentiniz</p>
              </div>
            </div>
            
            <div className="flex items-center space-x-4">
              {/* Admin File Management Link */}
              {user?.role === 'admin' && (
                <button
                  onClick={() => navigate('/file-management')}
                  className="text-gray-600 hover:text-blue-600 flex items-center space-x-2"
                >
                  <Settings className="w-5 h-5" />
                  <span className="text-sm">Sənədləri İdarə Et</span>
                </button>
              )}
              
              {/* User Info */}
              <div className="flex items-center space-x-2 text-sm">
                <User className="w-4 h-4 text-gray-500" />
                <span className="text-gray-700">{user?.username}</span>
                <span className="text-gray-500">({user?.role})</span>
              </div>
              
              {/* Logout */}
              <button
                onClick={handleLogout}
                className="text-gray-500 hover:text-red-600 transition-colors"
                title="Çıxış"
              >
                <LogOut className="w-5 h-5" />
              </button>
            </div>
          </div>
        </div>
      </div>

      {/* Main Content */}
      <div className="flex-1 flex overflow-hidden">
        {/* Sidebar - Conversations */}
        <div className="w-80 bg-white border-r flex flex-col">
          <div className="p-4 border-b">
            <div className="flex items-center justify-between mb-3">
              <h3 className="font-semibold text-gray-900 flex items-center">
                <MessageSquare className="w-4 h-4 mr-2" />
                Söhbətlər
              </h3>
              <button
                onClick={handleNewConversation}
                className="text-sm text-blue-600 hover:text-blue-700"
              >
                Yeni
              </button>
            </div>
          </div>
          
          <div className="flex-1 overflow-y-auto p-4">
            <div className="space-y-1">
              {conversations.length === 0 ? (
                <p className="text-sm text-gray-500 text-center py-2">
                  Söhbət yoxdur
                </p>
              ) : (
                conversations.map(conv => (
                  <div
                    key={conv.id}
                    className={`group p-2 rounded cursor-pointer text-sm transition-colors ${
                      currentConversation?.id === conv.id 
                        ? 'bg-blue-50 text-blue-700' 
                        : 'hover:bg-gray-100 text-gray-700'
                    }`}
                  >
                    {editingConvId === conv.id ? (
                      <div className="flex items-center space-x-1">
                        <input
                          type="text"
                          value={editTitle}
                          onChange={(e) => setEditTitle(e.target.value)}
                          onKeyPress={(e) => e.key === 'Enter' && handleRenameConversation(conv.id)}
                          className="flex-1 px-2 py-1 text-sm border rounded"
                          autoFocus
                        />
                        <button
                          onClick={() => handleRenameConversation(conv.id)}
                          className="text-green-600 hover:text-green-700"
                        >
                          <Check className="w-4 h-4" />
                        </button>
                        <button
                          onClick={() => {
                            setEditingConvId(null);
                            setEditTitle('');
                          }}
                          className="text-gray-400 hover:text-gray-600"
                        >
                          <X className="w-4 h-4" />
                        </button>
                      </div>
                    ) : (
                      <div 
                        onClick={() => handleLoadConversation(conv)}
                        className="flex items-center justify-between"
                      >
                        <div className="flex-1 min-w-0">
                          <div className="truncate">{safeString(conv.title)}</div>
                          <div className="text-xs text-gray-500">
                            {conv.message_count} mesaj
                          </div>
                        </div>
                        <div className="opacity-0 group-hover:opacity-100 flex items-center space-x-1">
                          <button
                            onClick={(e) => {
                              e.stopPropagation();
                              setEditingConvId(conv.id);
                              setEditTitle(conv.title);
                            }}
                            className="text-gray-400 hover:text-gray-600"
                          >
                            <Edit2 className="w-3 h-3" />
                          </button>
                          <button
                            onClick={(e) => {
                              e.stopPropagation();
                              handleDeleteConversation(conv.id);
                            }}
                            className="text-red-400 hover:text-red-600"
                          >
                            <Trash2 className="w-3 h-3" />
                          </button>
                        </div>
                      </div>
                    )}
                  </div>
                ))
              )}
            </div>
          </div>
        </div>

        {/* Chat Area */}
        <div className="flex-1 flex flex-col">
          {/* Messages */}
          <div className="flex-1 overflow-y-auto p-6 space-y-4">
            {messages.length === 0 ? (
              <div className="h-full flex items-center justify-center">
                <div className="text-center max-w-md">
                  <Bot className="w-16 h-16 mx-auto mb-4 text-gray-300" />
                  <h3 className="text-lg font-semibold text-gray-900 mb-2">
                    Salam! Mən sizin AI assistentinizəm
                  </h3>
                  <p className="text-gray-500 mb-4">
                    İstənilən sualınızı verə bilərsiniz - ümumi məlumat və ya yüklənmiş sənədlər haqqında.
                  </p>
                  <div className="bg-blue-50 border border-blue-200 rounded-lg p-3 text-left">
                    <p className="text-sm font-medium text-blue-900 mb-2">Nə edə bilirəm:</p>
                    <ul className="text-sm text-blue-800 space-y-1">
                      <li>✅ Ümumi suallara cavab verməк</li>
                      <li>✅ Yüklənmiş sənədlərdən məlumat tapmaq</li>
                      <li>✅ Əlaqə məlumatlarını göstərməк</li>
                      <li>✅ Sənəd şablonları təqdim etməк</li>
                    </ul>
                  </div>
                </div>
              </div>
            ) : (
              <>
                {messages.map((msg, idx) => (
                  <div key={idx} className={`flex ${msg.type === 'user' ? 'justify-end' : 'justify-start'}`}>
                    <div className={`max-w-3xl ${msg.type === 'user' ? 'order-2' : 'order-1'}`}>
                      <div className={`flex items-start space-x-2 ${msg.type === 'user' ? 'flex-row-reverse space-x-reverse' : ''}`}>
                        <div className={`w-8 h-8 rounded-full flex items-center justify-center ${
                          msg.type === 'user' ? 'bg-blue-600' : msg.type === 'error' ? 'bg-red-500' : msg.type === 'system' ? 'bg-yellow-500' : 'bg-gray-600'
                        }`}>
                          {msg.type === 'user' ? (
                            <User className="w-4 h-4 text-white" />
                          ) : msg.type === 'error' ? (
                            <AlertCircle className="w-4 h-4 text-white" />
                          ) : (
                            <Bot className="w-4 h-4 text-white" />
                          )}
                        </div>
                        <div className={`rounded-lg px-4 py-3 ${
                          msg.type === 'user' 
                            ? 'bg-blue-600 text-white' 
                            : msg.type === 'error'
                            ? 'bg-red-50 text-red-900 border border-red-200'
                            : msg.type === 'system'
                            ? 'bg-yellow-50 text-yellow-900 border border-yellow-200'
                            : 'bg-white border shadow-sm'
                        }`}>
                          {msg.type === 'system' && msg.documents ? (
                            <div>
                              <p className="mb-3">{msg.content}</p>
                              <div className="space-y-2">
                                {msg.documents.map(doc => (
                                  <button
                                    key={doc.id}
                                    onClick={() => handleSelectDocument(doc.id)}
                                    className="block w-full text-left text-sm bg-yellow-100 hover:bg-yellow-200 px-3 py-2 rounded transition-colors"
                                  >
                                    📄 {doc.name}
                                  </button>
                                ))}
                              </div>
                            </div>
                          ) : msg.type === 'bot' ? (
                            renderMessageWithDownloadLinks(safeString(msg.content))
                          ) : (
                            <p>{safeString(msg.content)}</p>
                          )}
                        </div>
                      </div>
                    </div>
                  </div>
                ))}
                <div ref={messagesEndRef} />
              </>
            )}
          </div>

          {/* Input Form */}
          <div className="border-t bg-white p-4">
            <form onSubmit={handleSendMessage} className="flex space-x-4">
              <input
                type="text"
                value={question}
                onChange={(e) => setQuestion(e.target.value)}
                placeholder="Sualınızı yazın... (məs: telefon nömrələri, müqavilə şablonu, və s.)"
                className="flex-1 px-4 py-3 border border-gray-300 rounded-lg focus:outline-none focus:ring-2 focus:ring-blue-500"
                disabled={isLoading}
              />
              <button
                type="submit"
                disabled={isLoading || !question.trim()}
                className="bg-blue-600 hover:bg-blue-700 disabled:bg-gray-400 text-white px-6 py-3 rounded-lg flex items-center space-x-2 transition-colors"
              >
                <Send className="w-5 h-5" />
                <span>Göndər</span>
              </button>
            </form>
          </div>
        </div>
      </div>
    </div>
  );
};

export default SmartDashboard;
