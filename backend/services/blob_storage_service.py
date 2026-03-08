# services/blob_storage_service.py
"""Vercel Blob Storage Service - Persistent document storage on Vercel"""
import os
import json
import zipfile
import io as _io
from typing import Optional, Tuple, List
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
    
    # ------------------------------------------------------------------ #
    #  Core upload / download helpers                                      #
    # ------------------------------------------------------------------ #

    def _put(self, blob_path: str, data: bytes, add_random_suffix: bool = False) -> Optional[str]:
        """
        PUT raw bytes to blob_path.  Returns the blob URL on success, None on failure.
        blob_path should NOT start with '/'.
        """
        headers = {
            'Authorization': f'Bearer {self.blob_token}',
            'Content-Type': 'application/octet-stream',
            'x-vercel-blob-access': 'public',
            'x-add-random-suffix': '1' if add_random_suffix else '0',
        }
        response = requests.put(
            f'https://blob.vercel-storage.com/{blob_path}',
            headers=headers,
            data=data,
            timeout=60,
        )
        if response.status_code in (200, 201):
            return response.json().get('url', '')
        print(f"⚠️ Blob PUT failed ({response.status_code}): {response.text[:200]}")
        return None

    def _list_blobs(self, prefix: str) -> List[dict]:
        """Return list of blob metadata dicts whose pathname starts with prefix."""
        try:
            resp = requests.get(
                'https://blob.vercel-storage.com',
                headers={'Authorization': f'Bearer {self.blob_token}'},
                params={'prefix': prefix},
                timeout=15,
            )
            if resp.status_code == 200:
                return resp.json().get('blobs', [])
        except Exception as e:
            print(f"⚠️ Blob list error: {e}")
        return []

    def _delete_by_prefix(self, prefix: str) -> None:
        """Delete all blobs whose pathname starts with prefix."""
        blobs = self._list_blobs(prefix)
        if blobs:
            urls = [b['url'] for b in blobs]
            try:
                requests.delete(
                    'https://blob.vercel-storage.com',
                    headers={
                        'Authorization': f'Bearer {self.blob_token}',
                        'Content-Type': 'application/json',
                    },
                    json={'urls': urls},
                    timeout=15,
                )
            except Exception as e:
                print(f"⚠️ Blob delete error: {e}")

    # ------------------------------------------------------------------ #
    #  Document file upload / delete / download                           #
    # ------------------------------------------------------------------ #

    def upload_file(self, file_obj, filename: str) -> Tuple[bool, str]:
        """
        Upload a user document to Vercel Blob Storage.
        Returns: (success, blob_url_or_error_message)
        """
        if not self.blob_enabled:
            return False, "Blob storage not configured - BLOB_READ_WRITE_TOKEN missing"
        
        try:
            file_obj.seek(0)
            file_content = file_obj.read()
            print(f"📤 Uploading {filename} ({len(file_content)} bytes) to Vercel Blob...")

            url = self._put(f'documents/{filename}', file_content, add_random_suffix=True)
            if url:
                print(f"✅ Upload successful: {url[:80]}...")
                return True, url
            return False, "Upload failed: no URL returned"

        except Exception as e:
            print(f"❌ Blob service error: {e}")
            import traceback
            traceback.print_exc()
            return False, str(e)

    def delete_file(self, blob_url: str) -> bool:
        """Delete a file from Vercel Blob Storage by its URL."""
        if not self.blob_enabled or not blob_url:
            return False
        try:
            response = requests.delete(
                'https://blob.vercel-storage.com',
                headers={
                    'Authorization': f'Bearer {self.blob_token}',
                    'Content-Type': 'application/json',
                },
                json={'urls': [blob_url]},
                timeout=10,
            )
            success = response.status_code in (200, 204)
            if success:
                print("✅ File deleted from Blob Storage")
            else:
                print(f"⚠️ Delete response: {response.status_code}")
            return success
        except Exception as e:
            print(f"⚠️ Blob delete error: {e}")
            return False

    def download_file(self, blob_url: str) -> Optional[bytes]:
        """Download file content from a public Vercel Blob URL."""
        if not blob_url:
            return None
        try:
            print(f"📥 Downloading from Blob: {blob_url[:60]}...")
            response = requests.get(blob_url, timeout=30)
            if response.status_code == 200:
                print(f"✅ Downloaded {len(response.content)} bytes")
                return response.content
            print(f"⚠️ Download failed: {response.status_code}")
            return None
        except Exception as e:
            print(f"⚠️ Blob download error: {e}")
            return None

    def get_file_stream(self, blob_url: str):
        """Get file as a BytesIO stream from a public Vercel Blob URL."""
        if not blob_url:
            return None
        try:
            response = requests.get(blob_url, timeout=30, stream=True)
            if response.status_code == 200:
                return BytesIO(response.content)
            return None
        except Exception as e:
            print(f"Blob stream error: {e}")
            return None

    # ------------------------------------------------------------------ #
    #  Persistence sync – SQLite database                                 #
    # ------------------------------------------------------------------ #

    _DB_BLOB_PATH = '_system/rag_chatbot.db'

    def sync_db_to_blob(self, db_path: str) -> bool:
        """Upload the local SQLite database to Blob Storage (replaces existing)."""
        if not self.blob_enabled:
            return False
        if not os.path.exists(db_path):
            print(f"⚠️ DB sync skipped – file not found: {db_path}")
            return False
        try:
            # Delete the existing backup first so the URL stays predictable
            self._delete_by_prefix(self._DB_BLOB_PATH)

            with open(db_path, 'rb') as f:
                data = f.read()

            url = self._put(self._DB_BLOB_PATH, data, add_random_suffix=False)
            if url:
                print(f"🔄 DB synced to Blob ({len(data)} bytes)")
                return True
            return False
        except Exception as e:
            print(f"⚠️ DB sync to blob error: {e}")
            return False

    def sync_db_from_blob(self, db_path: str) -> bool:
        """Download the SQLite database from Blob Storage to local path."""
        if not self.blob_enabled:
            return False
        try:
            blobs = self._list_blobs(self._DB_BLOB_PATH)
            if not blobs:
                print("📋 No remote DB backup found – starting fresh")
                return False

            blob_url = blobs[0]['url']
            response = requests.get(blob_url, timeout=30)
            if response.status_code != 200:
                print(f"⚠️ DB download failed: {response.status_code}")
                return False

            os.makedirs(os.path.dirname(db_path) or '.', exist_ok=True)
            with open(db_path, 'wb') as f:
                f.write(response.content)
            print(f"🔄 DB restored from Blob ({len(response.content)} bytes)")
            return True
        except Exception as e:
            print(f"⚠️ DB sync from blob error: {e}")
            return False

    # ------------------------------------------------------------------ #
    #  Persistence sync – ChromaDB vector stores                         #
    # ------------------------------------------------------------------ #

    def _chroma_blob_path(self, doc_id: int) -> str:
        return f'_system/chroma_doc_{doc_id}.zip'

    def sync_chroma_to_blob(self, doc_id: int, chroma_dir: str) -> bool:
        """Zip and upload a document's ChromaDB directory to Blob Storage."""
        if not self.blob_enabled:
            return False
        if not os.path.exists(chroma_dir):
            print(f"⚠️ ChromaDB sync skipped – dir not found: {chroma_dir}")
            return False
        try:
            blob_path = self._chroma_blob_path(doc_id)
            self._delete_by_prefix(blob_path)

            parent_dir = os.path.dirname(chroma_dir)
            zip_buffer = _io.BytesIO()
            with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zf:
                for root, _dirs, files in os.walk(chroma_dir):
                    for fname in files:
                        fpath = os.path.join(root, fname)
                        arcname = os.path.relpath(fpath, parent_dir)
                        zf.write(fpath, arcname)

            zip_data = zip_buffer.getvalue()
            url = self._put(blob_path, zip_data, add_random_suffix=False)
            if url:
                print(f"🔄 ChromaDB doc_{doc_id} synced to Blob ({len(zip_data)} bytes)")
                return True
            return False
        except Exception as e:
            print(f"⚠️ ChromaDB sync to blob error: {e}")
            return False

    def sync_chroma_from_blob(self, doc_id: int, chroma_dir: str) -> bool:
        """Download and extract a document's ChromaDB directory from Blob Storage."""
        if not self.blob_enabled:
            return False
        try:
            blob_path = self._chroma_blob_path(doc_id)
            blobs = self._list_blobs(blob_path)
            if not blobs:
                print(f"📋 No remote ChromaDB backup for doc_{doc_id}")
                return False

            response = requests.get(blobs[0]['url'], timeout=60)
            if response.status_code != 200:
                print(f"⚠️ ChromaDB download failed: {response.status_code}")
                return False

            parent_dir = os.path.dirname(chroma_dir)
            os.makedirs(parent_dir, exist_ok=True)
            with zipfile.ZipFile(_io.BytesIO(response.content), 'r') as zf:
                zf.extractall(parent_dir)

            print(f"🔄 ChromaDB doc_{doc_id} restored from Blob")
            return True
        except Exception as e:
            print(f"⚠️ ChromaDB sync from blob error: {e}")
            return False
