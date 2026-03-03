# api/index.py - Vercel Serverless Function Entry Point
import sys
import os

# Add backend directory to Python path
backend_path = os.path.join(os.path.dirname(__file__), '..', 'backend')
sys.path.insert(0, os.path.abspath(backend_path))

# Set production environment
os.environ.setdefault('FLASK_ENV', 'production')

from simple_app import create_simple_app

# Create the Flask app (Vercel looks for the `app` variable)
app, db_manager, rag_service, chat_service = create_simple_app()
