# utils/database.py
"""Database management utilities"""
import sqlite3
from typing import Optional, Dict, List, Any
from contextlib import contextmanager
from werkzeug.security import generate_password_hash

class DatabaseManager:
    """Database management class with connection pooling"""
    
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.init_database()
    
    @contextmanager
    def get_connection(self):
        """Context manager for database connections"""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
        finally:
            conn.close()
    
    def init_database(self) -> None:
        """Initialize database tables and default data"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            
            # Create tables
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS users (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    username TEXT UNIQUE NOT NULL,
                    password_hash TEXT NOT NULL,
                    email TEXT,
                    role TEXT DEFAULT 'user',
                    is_active BOOLEAN DEFAULT TRUE,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS documents (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    filename TEXT NOT NULL,
                    original_name TEXT NOT NULL,
                    file_path TEXT NOT NULL,
                    file_size INTEGER,
                    file_type TEXT,
                    uploaded_by INTEGER NOT NULL,
                    is_processed BOOLEAN DEFAULT FALSE,
                    is_blob_storage BOOLEAN DEFAULT FALSE,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (uploaded_by) REFERENCES users (id)
                )
            ''')
            
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS conversations (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER NOT NULL,
                    document_id INTEGER,
                    title TEXT,
                    messages TEXT NOT NULL DEFAULT '[]',
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (user_id) REFERENCES users (id),
                    FOREIGN KEY (document_id) REFERENCES documents (id) ON DELETE CASCADE
                )
            ''')
            
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS refresh_tokens (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER NOT NULL,
                    token TEXT UNIQUE NOT NULL,
                    expires_at TIMESTAMP NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (user_id) REFERENCES users (id)
                )
            ''')
            
            # Create indexes
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_users_username ON users(username)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_documents_uploaded_by ON documents(uploaded_by)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_conversations_user_id ON conversations(user_id)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_refresh_tokens_token ON refresh_tokens(token)')
            
            # Create default admin if not exists
            cursor.execute("SELECT id FROM users WHERE username = ?", ('admin',))
            if not cursor.fetchone():
                admin_hash = generate_password_hash('admin123')
                cursor.execute(
                    "INSERT INTO users (username, password_hash, role, email) VALUES (?, ?, ?, ?)",
                    ('admin', admin_hash, 'admin', 'admin@example.com')
                )
            
            conn.commit()
    
    def execute_query(self, query: str, params: tuple = (), 
                     fetch_one: bool = False) -> Any:
        """Execute a database query"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(query, params)
            
            if query.strip().upper().startswith('SELECT'):
                return cursor.fetchone() if fetch_one else cursor.fetchall()
            else:
                conn.commit()
                return cursor.lastrowid
    
    def get_user_by_username(self, username: str) -> Optional[Dict]:
        """Get user by username"""
        result = self.execute_query(
            "SELECT * FROM users WHERE username = ?",
            (username,),
            fetch_one=True
        )
        return dict(result) if result else None
    
    def get_user_by_id(self, user_id: int) -> Optional[Dict]:
        """Get user by ID"""
        result = self.execute_query(
            "SELECT id, username, email, role, created_at FROM users WHERE id = ?",
            (user_id,),
            fetch_one=True
        )
        return dict(result) if result else None
    
    def create_user(self, username: str, password_hash: str, 
                   email: Optional[str] = None, role: str = 'user') -> int:
        """Create a new user"""
        return self.execute_query(
            "INSERT INTO users (username, password_hash, email, role) VALUES (?, ?, ?, ?)",
            (username, password_hash, email, role)
        )
    
    def get_documents(self, user_id: Optional[int] = None) -> List[Dict]:
        """Get all documents or documents by user"""
        if user_id:
            query = '''
                SELECT d.*, u.username as uploaded_by_name 
                FROM documents d 
                JOIN users u ON d.uploaded_by = u.id 
                WHERE d.uploaded_by = ? 
                ORDER BY d.created_at DESC
            '''
            params = (user_id,)
        else:
            query = '''
                SELECT d.*, u.username as uploaded_by_name 
                FROM documents d 
                JOIN users u ON d.uploaded_by = u.id 
                ORDER BY d.created_at DESC
            '''
            params = ()
        
        results = self.execute_query(query, params)
        return [dict(row) for row in results]
    
    def create_document(self, filename: str, original_name: str, 
                       file_path: str, file_size: int, file_type: str,
                       uploaded_by: int, is_blob_storage: bool = False) -> int:
        """Create a document record"""
        return self.execute_query(
            '''INSERT INTO documents 
               (filename, original_name, file_path, file_size, file_type, uploaded_by, is_blob_storage) 
               VALUES (?, ?, ?, ?, ?, ?, ?)''',
            (filename, original_name, file_path, file_size, file_type, uploaded_by, is_blob_storage)
        )
    
    def update_document_processed(self, doc_id: int, processed: bool = True) -> None:
        """Update document processed status"""
        self.execute_query(
            "UPDATE documents SET is_processed = ? WHERE id = ?",
            (processed, doc_id)
        )
    
    def delete_document(self, doc_id: int) -> Optional[Dict]:
        """Delete a document and return its info"""
        doc = self.execute_query(
            "SELECT * FROM documents WHERE id = ?",
            (doc_id,),
            fetch_one=True
        )
        
        if doc:
            self.execute_query("DELETE FROM documents WHERE id = ?", (doc_id,))
            return dict(doc)
        return None
    
    def get_conversations(self, user_id: int) -> List[Dict]:
        """Get user conversations"""
        results = self.execute_query(
            '''SELECT c.*, d.original_name as document_name 
               FROM conversations c 
               LEFT JOIN documents d ON c.document_id = d.id 
               WHERE c.user_id = ? 
               ORDER BY c.updated_at DESC''',
            (user_id,)
        )
        return [dict(row) for row in results]
    
    def create_conversation(self, user_id: int, document_id: Optional[int],
                          title: str, messages: str) -> int:
        """Create a new conversation"""
        return self.execute_query(
            '''INSERT INTO conversations (user_id, document_id, title, messages) 
               VALUES (?, ?, ?, ?)''',
            (user_id, document_id, title, messages)
        )
    
    def update_conversation(self, conv_id: int, messages: str) -> None:
        """Update conversation messages"""
        self.execute_query(
            '''UPDATE conversations 
               SET messages = ?, updated_at = CURRENT_TIMESTAMP 
               WHERE id = ?''',
            (messages, conv_id)
        )
    
    def get_conversation(self, conv_id: int, user_id: int) -> Optional[Dict]:
        """Get a specific conversation"""
        result = self.execute_query(
            "SELECT * FROM conversations WHERE id = ? AND user_id = ?",
            (conv_id, user_id),
            fetch_one=True
        )
        return dict(result) if result else None
    
    def delete_conversation(self, conv_id: int, user_id: int) -> bool:
        """Delete a conversation"""
        self.execute_query(
            "DELETE FROM conversations WHERE id = ? AND user_id = ?",
            (conv_id, user_id)
        )
        return True
    
    def save_refresh_token(self, user_id: int, token: str, expires_at: str) -> None:
        """Save refresh token"""
        self.execute_query(
            "INSERT INTO refresh_tokens (user_id, token, expires_at) VALUES (?, ?, ?)",
            (user_id, token, expires_at)
        )
    
    def get_refresh_token(self, token: str) -> Optional[Dict]:
        """Get refresh token data"""
        result = self.execute_query(
            "SELECT * FROM refresh_tokens WHERE token = ?",
            (token,),
            fetch_one=True
        )
        return dict(result) if result else None
    
    def delete_refresh_token(self, token: str) -> None:
        """Delete refresh token"""
        self.execute_query(
            "DELETE FROM refresh_tokens WHERE token = ?",
            (token,)
        )
    
    def cleanup_expired_tokens(self) -> None:
        """Clean up expired refresh tokens"""
        self.execute_query(
            "DELETE FROM refresh_tokens WHERE expires_at < CURRENT_TIMESTAMP"
        )
