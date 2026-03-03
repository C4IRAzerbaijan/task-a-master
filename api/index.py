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

startup_error_detail = None

try:
    from simple_app import create_simple_app
    app, db_manager, rag_service, chat_service = create_simple_app()
    print("APP STARTED SUCCESSFULLY")
except Exception as e:
    # If the main app fails to load, create a minimal Flask app that returns the error
    from flask import Flask, jsonify, request
    from flask_cors import CORS
    app = Flask(__name__)
    CORS(app, origins=["*"])

    startup_error_detail = f"{type(e).__name__}: {e}\n{traceback.format_exc()}"
    print(f"STARTUP ERROR: {startup_error_detail}")

    @app.route('/api/health', methods=['GET'])
    def health():
        return jsonify({
            'status': 'error',
            'startup_error': startup_error_detail
        }), 500

    @app.route('/', defaults={'path': ''}, methods=['GET', 'POST', 'PUT', 'DELETE', 'OPTIONS', 'PATCH'])
    @app.route('/<path:path>', methods=['GET', 'POST', 'PUT', 'DELETE', 'OPTIONS', 'PATCH'])
    def catch_all(path):
        return jsonify({
            'error': 'Backend failed to start',
            'detail': str(e),
            'type': type(e).__name__,
            'path': request.path
        }), 500

# Vercel Python runtime looks for 'app' variable (WSGI application)
# Do NOT rename this variable
