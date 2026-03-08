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
        self.blob_token = os.getenv('BLOB_READ_WRITE_TOKEN', '')
        self.blob_enabled = self.blob_token != ''
        
        if self.blob_enabled:
            print(f"✓ Vercel Blob Storage enabled (token: {self.blob_token[:20]}...)")
        else:
            print("⚠️ Vercel Blob Storage disabled - token not found")
    
    def upload_file(self, file_obj, filename: str) -> Tuple[bool, str]:
        """
        Upload file to Vercel Blob Storage
        Returns: (success, blob_url_or_error_message)
        """
        if not self.blob_enabled:
            return False, "Blob storage not configured - VERCEL_BLOB_TOKEN missing"
        
        try:
            # Read file content
            file_obj.seek(0)
            file_content = file_obj.read()
            file_size = len(file_content)
            
            print(f"📤 Uploading {filename} ({file_size} bytes) to Vercel Blob...")
            
            # Vercel Blob API - use the proper endpoint
            # The token itself contains the URL, or we construct it properly
            headers = {
                'Authorization': f'Bearer {self.blob_token}',
                'Content-Type': 'application/octet-stream',
            }
            
            # Method 1: Direct upload to Vercel Blob API endpoint
            url = 'https://blob.vercel-storage.com/upload'
            
            # Send as raw bytes with filename in header
            headers['X-Vercel-Blob-Filename'] = filename
            
            response = requests.post(
                url,
                headers=headers,
                data=file_content,
                timeout=30
            )
            
            print(f"📊 Vercel response: {response.status_code}")
            
            if response.status_code in (200, 201):
                data = response.json()
                blob_url = data.get('url', '')
                if not blob_url:
                    # Try alternate response format
                    blob_url = data.get('pathname', '')
                print(f"✅ Upload successful: {blob_url[:80]}...")
                return True, blob_url
            else:
                error_text = response.text[:200]
                print(f"❌ Blob upload error: {response.status_code}")
                print(f"   Response: {error_text}")
                return False, f"Upload failed: {response.status_code} - {error_text}"
                
        except Exception as e:
            print(f"❌ Blob service error: {str(e)}")
            import traceback
            traceback.print_exc()
            return False, str(e)
    
    
    def delete_file(self, blob_url: str) -> bool:
        """Delete file from Vercel Blob Storage"""
        if not self.blob_enabled or not blob_url:
            return False
        
        try:
            # Vercel Blob delete endpoint
            headers = {
                'Authorization': f'Bearer {self.blob_token}',
            }
            
            # Extract pathname from blob URL if it's a full URL
            if blob_url.startswith('http'):
                # blob_url is like: https://xxxx.public.blob.vercel-storage.com/path
                # We need to send to delete endpoint with the pathname
                from urllib.parse import urlparse
                parsed = urlparse(blob_url)
                pathname = parsed.path
            else:
                pathname = blob_url
            
            delete_url = 'https://blob.vercel-storage.com/delete'
            
            response = requests.post(
                delete_url,
                headers=headers,
                json={'pathname': pathname},
                timeout=10
            )
            
            success = response.status_code in (200, 204)
            if success:
                print(f"✅ File deleted from Blob Storage")
            else:
                print(f"⚠️ Delete response: {response.status_code}")
            return success
        except Exception as e:
            print(f"⚠️ Blob delete error: {str(e)}")
            return False
    
    
    def download_file(self, blob_url: str) -> Optional[bytes]:
        """Download file content from Vercel Blob Storage"""
        if not blob_url:
            return None
        
        try:
            print(f"📥 Downloading from Blob: {blob_url[:60]}...")
            response = requests.get(blob_url, timeout=30)
            if response.status_code == 200:
                print(f"✅ Downloaded {len(response.content)} bytes")
                return response.content
            else:
                print(f"⚠️ Download failed: {response.status_code}")
                return None
        except Exception as e:
            print(f"⚠️ Blob download error: {str(e)}")
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
