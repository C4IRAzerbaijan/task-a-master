# api/index.py - Vercel Serverless Function Entry Point
import sys
import os
import traceback

# Add backend directory to Python path
backend_path = os.path.join(os.path.dirname(__file__), '..', 'backend')
sys.path.insert(0, os.path.abspath(backend_path))

# Set production environment
os.environ.setdefault('FLASK_ENV', 'production')
os.environ.setdefault('VERCEL', '1')

try:
    from simple_app import create_simple_app
    app, db_manager, rag_service, chat_service = create_simple_app()
except Exception as e:
    # If the main app fails to load, create a minimal Flask app that returns the error
    # This prevents Vercel from returning 404 and shows us the actual error
    from flask import Flask, jsonify
    app = Flask(__name__)

    startup_error = f"{type(e).__name__}: {e}\n{traceback.format_exc()}"
    print(f"STARTUP ERROR: {startup_error}")

    @app.route('/', defaults={'path': ''})
    @app.route('/<path:path>')
    def catch_all(path):
        return jsonify({
            'error': 'Backend failed to start',
            'detail': str(e),
            'type': type(e).__name__
        }), 500

handler = app
