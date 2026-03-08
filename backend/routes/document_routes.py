# routes/document_routes.py
"""Document management routes"""
import os
import uuid
import io
from flask import Blueprint, request, jsonify, send_file
from flask_jwt_extended import jwt_required, get_jwt_identity, get_jwt, set_access_cookies
from werkzeug.utils import secure_filename
from utils.auth import admin_required

docs_bp = Blueprint('documents', __name__, url_prefix='/api/documents')

def init_document_routes(db_manager, rag_service, config, blob_storage=None):
    """Initialize document routes"""
    
    @docs_bp.route('', methods=['GET'])
    @jwt_required(locations=['cookies', 'headers'])
    def list_documents():
        """List all documents"""
        try:
            claims = get_jwt()
            user_id = get_jwt_identity()
            
            # Admin sees all documents, users see their own
            if claims.get('role') == 'admin':
                documents = db_manager.get_documents()
            else:
                documents = db_manager.get_documents(user_id)
            
            return jsonify({
                'documents': [
                    {
                        'id': doc['id'],
                        'name': doc['original_name'],
                        'size': doc['file_size'],
                        'type': doc['file_type'],
                        'uploaded_by': doc['uploaded_by_name'],
                        'is_processed': doc['is_processed'],
                        'created_at': doc['created_at']
                    }
                    for doc in documents
                ]
            })
            
        except Exception as e:
            return jsonify({'error': f'Documents list xətası: {str(e)}'}), 500
    
    @docs_bp.route('', methods=['POST'])
    @jwt_required(locations=['cookies', 'headers'])
    @admin_required()
    def upload_document():
        """Upload new document (admin only) - uses Vercel Blob Storage on production"""
        try:
            if 'file' not in request.files:
                return jsonify({'error': 'Fayl seçilməyib'}), 400
            
            file = request.files['file']
            if not file.filename:
                return jsonify({'error': 'Fayl seçin'}), 400
            
            # Validate file
            from services.file_processor import FileProcessor
            processor = FileProcessor()
            is_valid, error_msg = processor.validate_file(file.filename, config.MAX_FILE_SIZE)
            
            if not is_valid:
                return jsonify({'error': error_msg}), 400
            
            # Check file size
            file.seek(0, os.SEEK_END)
            file_size = file.tell()
            file.seek(0)
            
            if file_size > config.MAX_FILE_SIZE:
                return jsonify({'error': f'Fayl çox böyükdür (max {config.MAX_FILE_SIZE // (1024*1024)}MB)'}), 400
            
            # Get file type
            filename = secure_filename(file.filename)
            file_type = processor.get_file_type(filename)
            
            # Use Blob Storage whenever it is configured, regardless of environment
            blob_url = None
            local_file_path = None
            
            if blob_storage and blob_storage.blob_enabled:
                # Use Blob Storage on Vercel
                unique_filename = f"{uuid.uuid4()}_{filename}"
                success, result = blob_storage.upload_file(file, unique_filename)
                
                if not success:
                    return jsonify({'error': f'Blob upload failed: {result}'}), 500
                
                blob_url = result
                file_path_for_db = blob_url  # Store blob URL in database
            else:
                # Use local filesystem in development
                unique_filename = f"{uuid.uuid4()}_{filename}"
                local_file_path = os.path.join(config.UPLOAD_FOLDER, unique_filename)
                
                # Ensure upload folder exists
                os.makedirs(config.UPLOAD_FOLDER, exist_ok=True)
                file.save(local_file_path)
                
                file_path_for_db = local_file_path
            
            # Save to database
            user_id = get_jwt_identity()
            doc_id = db_manager.create_document(
                filename=unique_filename,
                original_name=filename,
                file_path=file_path_for_db,  # Either blob URL or local path
                file_size=file_size,
                file_type=file_type,
                uploaded_by=user_id,
                is_blob_storage=bool(blob_url)  # Flag to indicate blob storage
            )
            
            # Process document with RAG
            # Pass the actual file content or path to RAG service
            if blob_url:
                # For blob storage, download to memory for processing
                file_content = blob_storage.download_file(blob_url)
                if file_content:
                    success = rag_service.process_document_from_bytes(file_content, doc_id, filename)
                else:
                    success = False
            else:
                # For local storage, use file path directly
                success = rag_service.process_document(file_path_for_db, doc_id)
            
            if success:
                db_manager.update_document_processed(doc_id, True)
                message = f'{file_type} faylı uğurla yükləndi və işləndi'
            else:
                message = f'{file_type} faylı yükləndi amma işlənmədi'
            
            return jsonify({
                'message': message,
                'document': {
                    'id': doc_id,
                    'name': filename,
                    'type': file_type,
                    'size': file_size,
                    'is_processed': success
                }
            }), 201
            
        except Exception as e:
            # Clean up local file if error
            if 'local_file_path' in locals() and local_file_path and os.path.exists(local_file_path):
                os.remove(local_file_path)
            return jsonify({'error': f'Upload xətası: {str(e)}'}), 500
    
    @docs_bp.route('/<int:doc_id>', methods=['GET'])
    @jwt_required(locations=['cookies', 'headers'])
    def get_document(doc_id):
        """Get document details"""
        try:
            documents = db_manager.get_documents()
            doc = next((d for d in documents if d['id'] == doc_id), None)
            
            if not doc:
                return jsonify({'error': 'Sənəd tapılmadı'}), 404
            
            # Check access
            user_id = get_jwt_identity()
            claims = get_jwt()
            if claims.get('role') != 'admin' and doc['uploaded_by'] != user_id:
                return jsonify({'error': 'Access denied'}), 403
            
            return jsonify({
                'document': {
                    'id': doc['id'],
                    'name': doc['original_name'],
                    'size': doc['file_size'],
                    'type': doc['file_type'],
                    'uploaded_by': doc['uploaded_by_name'],
                    'is_processed': doc['is_processed'],
                    'created_at': doc['created_at']
                }
            })
            
        except Exception as e:
            return jsonify({'error': f'Document info xətası: {str(e)}'}), 500
    
    @docs_bp.route('/<int:doc_id>', methods=['DELETE'])
    @jwt_required(locations=['cookies', 'headers'])
    @admin_required()
    def delete_document(doc_id):
        """Delete document (admin only)"""
        try:
            # Get document info
            doc = db_manager.delete_document(doc_id)
            
            if not doc:
                return jsonify({'error': 'Sənəd tapılmadı'}), 404
            
            # Delete file based on storage type
            is_blob = doc.get('is_blob_storage', False)
            file_path = doc['file_path']
            
            if is_blob and blob_storage:
                # Delete from Blob Storage
                blob_storage.delete_file(file_path)
            else:
                # Delete local file
                if os.path.exists(file_path):
                    os.remove(file_path)
            
            # Delete vector store
            rag_service.delete_document_vectors(doc_id)
            
            return jsonify({'message': 'Sənəd silindi'})
            
        except Exception as e:
            return jsonify({'error': f'Delete xətası: {str(e)}'}), 500
    
    @docs_bp.route('/<int:doc_id>/download', methods=['GET'])
    @jwt_required(locations=['cookies', 'headers'])
    def download_document(doc_id):
        """Download document"""
        try:
            documents = db_manager.get_documents()
            doc = next((d for d in documents if d['id'] == doc_id), None)
            
            if not doc:
                return jsonify({'error': 'Sənəd tapılmadı'}), 404
            
            # Check access
            user_id = get_jwt_identity()
            claims = get_jwt()
            if claims.get('role') != 'admin' and doc['uploaded_by'] != user_id:
                return jsonify({'error': 'Access denied'}), 403
            
            file_path = doc['file_path']
            is_blob = doc.get('is_blob_storage', False)
            
            if is_blob and blob_storage:
                # Download from Blob Storage
                file_content = blob_storage.download_file(file_path)
                if not file_content:
                    return jsonify({'error': 'Fayl tapılmadı'}), 404
                
                return send_file(
                    io.BytesIO(file_content),
                    as_attachment=True,
                    download_name=doc['original_name']
                )
            else:
                # Download from local filesystem
                if not os.path.exists(file_path):
                    return jsonify({'error': 'Fayl tapılmadı'}), 404
                
                return send_file(
                    file_path,
                    as_attachment=True,
                    download_name=doc['original_name']
                )
            
        except Exception as e:
            return jsonify({'error': f'Download xətası: {str(e)}'}), 500
    
    @docs_bp.route('/<int:doc_id>/reprocess', methods=['POST'])
    @jwt_required(locations=['cookies', 'headers'])
    @admin_required()
    def reprocess_document(doc_id):
        """Reprocess document (admin only)"""
        try:
            documents = db_manager.get_documents()
            doc = next((d for d in documents if d['id'] == doc_id), None)
            
            if not doc:
                return jsonify({'error': 'Sənəd tapılmadı'}), 404
            
            if not os.path.exists(doc['file_path']):
                return jsonify({'error': 'Fayl tapılmadı'}), 404
            
            # Reprocess with RAG
            success = rag_service.process_document(doc['file_path'], doc_id)
            
            if success:
                db_manager.update_document_processed(doc_id, True)
                return jsonify({'message': 'Sənəd yenidən işləndi'})
            else:
                return jsonify({'error': 'Sənəd işlənmədi'}), 500
            
        except Exception as e:
            return jsonify({'error': f'Reprocess xətası: {str(e)}'}), 500
    
    return docs_bp
