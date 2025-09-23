import os
import bcrypt
from flask import request, jsonify
from flask_jwt_extended import JWTManager, create_access_token, jwt_required, get_jwt_identity
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def init_auth(app):
    """Initialize JWT manager"""
    app.config['JWT_SECRET_KEY'] = os.getenv('JWT_SECRET')
    app.config['JWT_ACCESS_TOKEN_EXPIRES'] = False  # Tokens don't expire for simplicity
    jwt = JWTManager(app)
    return jwt

def authenticate_admin(username, password):
    """Authenticate admin user"""
    # Check username
    if username != os.getenv('ADMIN_USERNAME'):
        return False

    # Check password against hash
    stored_hash = os.getenv('ADMIN_PASSWORD_HASH')
    if not stored_hash:
        return False

    # bcrypt check
    return bcrypt.checkpw(password.encode('utf-8'), stored_hash.encode('utf-8'))

def login():
    """Login endpoint handler"""
    try:
        data = request.get_json()
        username = data.get('username')
        password = data.get('password')

        if not username or not password:
            return jsonify({'error': 'Username and password required'}), 400

        # Authenticate
        if authenticate_admin(username, password):
            # Create access token
            access_token = create_access_token(identity=username)
            return jsonify({
                'success': True,
                'message': 'Login successful',
                'access_token': access_token,
                'user': {
                    'username': username,
                    'email': os.getenv('ADMIN_EMAIL')
                }
            })
        else:
            return jsonify({'error': 'Invalid credentials'}), 401

    except Exception as e:
        return jsonify({'error': 'Login failed'}), 500

# Utility function to generate password hash
def generate_password_hash(password):
    """Generate bcrypt hash for password"""
    return bcrypt.hashpw(password.encode('utf-8'), bcrypt.gensalt()).decode('utf-8')

# Example usage to generate hash for .env file:
# print(generate_password_hash("your_password_here"))