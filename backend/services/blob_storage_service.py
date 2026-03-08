# services/blob_storage_service.py
"""Vercel Blob Storage Service - Persistent document storage on Vercel"""
import os
import json
from typing import Optional, Tuple
import requests
from io import BytesIO

class BlobStorageService:
    """Service to handle file uploads to Vercel Blob Storage"""
    
    def __init__(self, config):
        self.config = config
        self.blob_enabled = os.getenv('VERCEL_BLOB_TOKEN', '') != ''
        self.blob_token = os.getenv('VERCEL_BLOB_TOKEN', '')
        self.blob_api_url = 'https://blob.vercel-storage.com'
        
        if self.blob_enabled:
            print("✓ Vercel Blob Storage enabled")
        else:
            print("⚠️ Vercel Blob Storage disabled - using fallback")
    
    def upload_file(self, file_obj, filename: str) -> Tuple[bool, str]:
        """
        Upload file to Vercel Blob Storage
        Returns: (success, blob_url_or_error_message)
        """
        if not self.blob_enabled:
            return False, "Blob storage not configured"
        
        try:
            # Read file content
            file_obj.seek(0)
            file_content = file_obj.read()
            
            # Prepare upload
            headers = {
                'Authorization': f'Bearer {self.blob_token}',
            }
            
            files = {
                'file': (filename, BytesIO(file_content), 'application/octet-stream'),
            }
            
            # Upload to Vercel Blob
            response = requests.post(
                f'{self.blob_api_url}/upload',
                headers=headers,
                files=files,
                data={'filename': filename}
            )
            
            if response.status_code in (200, 201):
                data = response.json()
                blob_url = data.get('url', '')
                return True, blob_url
            else:
                error = response.text
                print(f"Blob upload error: {response.status_code} - {error}")
                return False, f"Upload failed: {response.status_code}"
                
        except Exception as e:
            print(f"Blob service error: {str(e)}")
            return False, str(e)
    
    def delete_file(self, blob_url: str) -> bool:
        """Delete file from Vercel Blob Storage"""
        if not self.blob_enabled or not blob_url:
            return False
        
        try:
            headers = {
                'Authorization': f'Bearer {self.blob_token}',
            }
            
            response = requests.delete(
                blob_url,
                headers=headers
            )
            
            return response.status_code in (200, 204)
        except Exception as e:
            print(f"Blob delete error: {str(e)}")
            return False
    
    def download_file(self, blob_url: str) -> Optional[bytes]:
        """Download file content from Vercel Blob Storage"""
        if not blob_url:
            return None
        
        try:
            response = requests.get(blob_url, timeout=30)
            if response.status_code == 200:
                return response.content
            return None
        except Exception as e:
            print(f"Blob download error: {str(e)}")
            return None
    
    def get_file_stream(self, blob_url: str):
        """Get file as stream from Vercel Blob Storage"""
        if not blob_url:
            return None
        
        try:
            response = requests.get(blob_url, timeout=30, stream=True)
            if response.status_code == 200:
                return BytesIO(response.content)
            return None
        except Exception as e:
            print(f"Blob stream error: {str(e)}")
            return None
