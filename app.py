from flask import Flask, request, jsonify, send_file, make_response, Response
from flask_cors import CORS
from flask_jwt_extended import jwt_required
import os
import json
import threading
from werkzeug.utils import secure_filename
from werkzeug.datastructures import FileStorage
from services.pobs_service import verify_new_records, add_new_records, update_imei_data, verify_new_records_realtime, add_new_records_realtime, update_imei_data_realtime
from services.pcom_service import process_pcom_files, process_pcom_with_pobs, process_pcom_files_realtime, process_pcom_with_pobs_realtime
from services.tracking_service import generate_upload_gsped, update_tracking_data, generate_upload_gsped_realtime, update_tracking_data_realtime
from services.logger_service import operation_logger
from services.realtime_logger import realtime_logger
from middleware.auth import init_auth, login

app = Flask(__name__)
CORS(app, origins=[
    "https://easyrentwebapp.netlify.app",
    "https://*.netlify.app",
    "http://localhost:3000",
    "http://localhost:1420",
    "http://tauri.localhost",
    "http://127.0.0.1:3000"
], supports_credentials=True, methods=['GET', 'POST', 'PUT', 'DELETE', 'OPTIONS'],
   allow_headers=['Content-Type', 'Authorization', 'X-Requested-With', 'Accept-Ranges', 'Range'])
app.config['MAX_CONTENT_LENGTH'] = 50 * 1024 * 1024  # 50MB max file size

# Initialize JWT authentication
jwt = init_auth(app)

# Handle preflight requests
@app.before_request
def handle_preflight():
    if request.method == "OPTIONS":
        response = make_response()
        response.headers.add("Access-Control-Allow-Origin", "*")
        response.headers.add('Access-Control-Allow-Headers', "Content-Type,Authorization,Accept-Ranges,Range")
        response.headers.add('Access-Control-Allow-Methods', "GET,PUT,POST,DELETE,OPTIONS")
        response.headers.add('Access-Control-Allow-Credentials', "true")
        return response

# Ensure directories exist
os.makedirs('uploads', exist_ok=True)
os.makedirs('outputs', exist_ok=True)

def save_uploaded_file(file: FileStorage, folder: str) -> str:
    """Save uploaded file and return the path"""
    if file and file.filename:
        filename = secure_filename(file.filename)
        filepath = os.path.join(folder, filename)
        file.save(filepath)
        return filepath
    return None

# ============================================================================
# Authentication Routes
# ============================================================================

@app.route('/api/auth/login', methods=['POST'])
def auth_login():
    """Login endpoint"""
    return login()

# ============================================================================
# POBS Module Routes
# ============================================================================

@app.route('/api/pobs/verify-new', methods=['POST'])
@jwt_required()
def pobs_verify_new():
    """Verify new records between Noleggio and POBS files"""
    try:
        noleggio_file = request.files.get('noleggio')
        pobs_file = request.files.get('pobs')

        if not noleggio_file or not pobs_file:
            return jsonify({'error': 'Both Noleggio and POBS files are required'}), 400

        # Save files
        noleggio_path = save_uploaded_file(noleggio_file, 'uploads')
        pobs_path = save_uploaded_file(pobs_file, 'uploads')

        # Process files and return complete result with logs
        result = verify_new_records(noleggio_path, pobs_path)
        return jsonify(result)

    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/pobs/add-new', methods=['POST'])
@jwt_required()
def pobs_add_new():
    """Add new records to POBS file"""
    try:
        noleggio_file = request.files.get('noleggio')
        pobs_file = request.files.get('pobs')

        if not noleggio_file or not pobs_file:
            return jsonify({'error': 'Both files are required'}), 400

        # Save files
        noleggio_path = save_uploaded_file(noleggio_file, 'uploads')
        pobs_path = save_uploaded_file(pobs_file, 'uploads')

        # Process files and return complete result with logs
        result = add_new_records(noleggio_path, pobs_path, 'outputs')
        return jsonify(result)

    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/pobs/update-imei', methods=['POST'])
@jwt_required()
def pobs_update_imei():
    """Update IMEI data from masterfile with custom naming"""
    try:
        pobs_file = request.files.get('pobs')
        master_file = request.files.get('masterfile')
        template_file = request.files.get('template')
        custom_name = request.form.get('custom_name')

        if not all([pobs_file, master_file, template_file]):
            return jsonify({'error': 'All three files are required'}), 400

        # Save files
        pobs_path = save_uploaded_file(pobs_file, 'uploads')
        master_path = save_uploaded_file(master_file, 'uploads')
        template_path = save_uploaded_file(template_file, 'uploads')

        # Process files and return complete result with logs
        result = update_imei_data(pobs_path, master_path, template_path, 'outputs', custom_name)
        return jsonify(result)

    except Exception as e:
        return jsonify({'error': str(e)}), 500

# ============================================================================
# PCOM Module Routes
# ============================================================================

@app.route('/api/pcom/process', methods=['POST'])
@jwt_required()
def pcom_process():
    """Process PCOM files with optional POBS update"""
    try:
        noleggio_file = request.files.get('noleggio')
        soho_file = request.files.get('soho')
        modelli_file = request.files.get('modelli')
        pobs_file = request.files.get('pobs')  # Optional POBS file

        if not noleggio_file or not soho_file:
            return jsonify({'error': 'Noleggio and SOHO files are required'}), 400

        # Get options and custom names from request
        options_str = request.form.get('options', '{}')
        options = json.loads(options_str)
        custom_names_str = request.form.get('custom_names', '{}')
        custom_names = json.loads(custom_names_str)

        # Save files
        noleggio_path = save_uploaded_file(noleggio_file, 'uploads')
        soho_path = save_uploaded_file(soho_file, 'uploads')
        modelli_path = save_uploaded_file(modelli_file, 'uploads') if modelli_file else None
        pobs_path = save_uploaded_file(pobs_file, 'uploads') if pobs_file else None

        # Process files and return complete result
        if pobs_path:
            result = process_pcom_with_pobs(noleggio_path, soho_path, pobs_path, 'outputs', modelli_path, options, custom_names)
        else:
            result = process_pcom_files(noleggio_path, soho_path, 'outputs', modelli_path, options, custom_names.get('pcom') if custom_names else None)


        return jsonify(result)

    except Exception as e:
        return jsonify({'error': str(e)}), 500

# ============================================================================
# Tracking Module Routes
# ============================================================================

@app.route('/api/tracking/generate-gsped', methods=['POST'])
@jwt_required()
def tracking_generate_gsped():
    """Generate Upload Gsped file"""
    try:
        pobs_file = request.files.get('pobs')
        masterfile_file = request.files.get('masterfile')

        if not pobs_file or not masterfile_file:
            return jsonify({'error': 'Both POBS and Masterfile are required'}), 400

        # Save files
        pobs_path = save_uploaded_file(pobs_file, 'uploads')
        masterfile_path = save_uploaded_file(masterfile_file, 'uploads')

        # Process files and return complete result
        result = generate_upload_gsped(pobs_path, masterfile_path, 'outputs')
        return jsonify(result)

    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/tracking/update-tracking', methods=['POST'])
@jwt_required()
def tracking_update():
    """Update tracking data in POBS with masterfile integration"""
    try:
        pobs_file = request.files.get('pobs')
        trasporti_file = request.files.get('trasporti')
        masterfile_file = request.files.get('masterfile')
        custom_name = request.form.get('custom_name')

        if not pobs_file or not trasporti_file:
            return jsonify({'error': 'Both POBS and Trasporti files are required'}), 400

        # Save files
        pobs_path = save_uploaded_file(pobs_file, 'uploads')
        trasporti_path = save_uploaded_file(trasporti_file, 'uploads')
        masterfile_path = save_uploaded_file(masterfile_file, 'uploads') if masterfile_file else None

        # Process files and return complete result
        result = update_tracking_data(pobs_path, trasporti_path, masterfile_path, 'outputs', custom_name)
        return jsonify(result)

    except Exception as e:
        return jsonify({'error': str(e)}), 500

# ============================================================================
# File Download Routes
# ============================================================================

def generate_file_chunks(file_path, chunk_size=8192):
    """Generator function to stream file in chunks"""
    with open(file_path, 'rb') as f:
        while True:
            chunk = f.read(chunk_size)
            if not chunk:
                break
            yield chunk

@app.route('/api/download/<filename>')
@jwt_required()
def download_file(filename):
    """Download generated files with enhanced subdirectory search and streaming for large files"""
    try:
        # Define priority search order for subdirectories
        search_dirs = [
            'outputs',  # Direct outputs folder
            'outputs/PCOM',  # PCOM files
            'outputs/POBS',  # POBS files
            'outputs/IMEI HUB',  # IMEI HUB files
            'outputs/GSPED',  # GSPED files
            'outputs/TRACKING RADAR',  # Tracking radar files
            'outputs/POBS CON TRACKING',  # POBS with tracking
            'outputs/Backup',  # Backup files
            'outputs/backup_POBS'  # POBS backup files
        ]

        file_path = None

        # First try priority directories
        for search_dir in search_dirs:
            potential_path = os.path.join(search_dir, filename)
            if os.path.exists(potential_path):
                file_path = potential_path
                break

        # If not found, do a comprehensive search in all subdirectories
        if not file_path:
            for root, dirs, files in os.walk('outputs'):
                if filename in files:
                    file_path = os.path.join(root, filename)
                    break

        if not file_path:
            return jsonify({'error': f'File "{filename}" not found in any output directory'}), 404

        # Check file size for optimization
        file_size = os.path.getsize(file_path)

        # For very large files (>10MB), use chunked streaming
        if file_size > 10 * 1024 * 1024:  # 10MB
            def generate():
                with open(file_path, 'rb') as f:
                    while True:
                        chunk = f.read(8192)  # 8KB chunks
                        if not chunk:
                            break
                        yield chunk

            response = Response(
                generate(),
                headers={
                    'Content-Type': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
                    'Content-Disposition': f'attachment; filename="{filename}"',
                    'Content-Length': str(file_size),
                    'Cache-Control': 'no-cache',
                    'Connection': 'keep-alive'
                }
            )
            return response
        else:
            # For smaller files, use send_file
            return send_file(
                file_path,
                as_attachment=True,
                download_name=filename
            )

    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/download-direct/<filename>')
@jwt_required()
def download_file_direct(filename):
    """Alternative direct download with range request support for large files"""
    try:
        # Same file search logic
        search_dirs = [
            'outputs', 'outputs/PCOM', 'outputs/POBS', 'outputs/IMEI HUB',
            'outputs/GSPED', 'outputs/TRACKING RADAR', 'outputs/POBS CON TRACKING',
            'outputs/Backup', 'outputs/backup_POBS'
        ]

        file_path = None
        for search_dir in search_dirs:
            potential_path = os.path.join(search_dir, filename)
            if os.path.exists(potential_path):
                file_path = potential_path
                break

        if not file_path:
            for root, dirs, files in os.walk('outputs'):
                if filename in files:
                    file_path = os.path.join(root, filename)
                    break

        if not file_path:
            return jsonify({'error': f'File "{filename}" not found'}), 404

        # Get file size
        file_size = os.path.getsize(file_path)

        # Check if client supports range requests
        range_header = request.headers.get('Range')

        if range_header:
            # Parse range header
            byte_start = 0
            byte_end = file_size - 1

            if range_header.startswith('bytes='):
                range_match = range_header[6:].split('-')
                if range_match[0]:
                    byte_start = int(range_match[0])
                if range_match[1]:
                    byte_end = int(range_match[1])

            content_length = byte_end - byte_start + 1

            def generate_partial():
                with open(file_path, 'rb') as f:
                    f.seek(byte_start)
                    remaining = content_length
                    while remaining:
                        chunk_size = min(8192, remaining)
                        chunk = f.read(chunk_size)
                        if not chunk:
                            break
                        remaining -= len(chunk)
                        yield chunk

            response = Response(
                generate_partial(),
                206,  # Partial Content
                headers={
                    'Content-Type': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
                    'Content-Range': f'bytes {byte_start}-{byte_end}/{file_size}',
                    'Accept-Ranges': 'bytes',
                    'Content-Length': str(content_length),
                    'Content-Disposition': f'attachment; filename="{filename}"',
                }
            )
            return response
        else:
            # No range request, send entire file with chunked encoding
            def generate_full():
                with open(file_path, 'rb') as f:
                    while True:
                        chunk = f.read(8192)
                        if not chunk:
                            break
                        yield chunk

            response = Response(
                generate_full(),
                headers={
                    'Content-Type': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
                    'Content-Length': str(file_size),
                    'Content-Disposition': f'attachment; filename="{filename}"',
                    'Accept-Ranges': 'bytes',
                    'Cache-Control': 'no-cache'
                }
            )
            return response

    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/download-simple/<filename>')
def download_file_simple(filename):
    """Simple download without JWT for emergency fallback (token in query param)"""
    try:
        # Get token from query parameter
        token = request.args.get('token')
        if not token:
            return jsonify({'error': 'Token required'}), 401

        # Validate token manually
        from flask_jwt_extended import decode_token
        try:
            decode_token(token)
        except Exception:
            return jsonify({'error': 'Invalid token'}), 401

        # Same file search logic
        search_dirs = [
            'outputs', 'outputs/PCOM', 'outputs/POBS', 'outputs/IMEI HUB',
            'outputs/GSPED', 'outputs/TRACKING RADAR', 'outputs/POBS CON TRACKING',
            'outputs/Backup', 'outputs/backup_POBS'
        ]

        file_path = None
        for search_dir in search_dirs:
            potential_path = os.path.join(search_dir, filename)
            if os.path.exists(potential_path):
                file_path = potential_path
                break

        if not file_path:
            for root, dirs, files in os.walk('outputs'):
                if filename in files:
                    file_path = os.path.join(root, filename)
                    break

        if not file_path:
            return jsonify({'error': f'File "{filename}" not found'}), 404

        # Simple direct file send
        return send_file(file_path, as_attachment=True, download_name=filename)

    except Exception as e:
        return jsonify({'error': str(e)}), 500

# ============================================================================
# Historic Files Management
# ============================================================================

@app.route('/api/historic/files')
@jwt_required()
def get_historic_files():
    """Get all historic files organized by feature type"""
    try:
        # Define folder mappings
        folder_mappings = {
            'POBS': ['outputs/POBS', 'outputs/backup_POBS'],
            'PCOM': ['outputs/PCOM'],
            'IMEI_HUB': ['outputs/IMEI HUB'],
            'GSPED': ['outputs/GSPED'],
            'TRACKING_RADAR': ['outputs/TRACKING RADAR'],
            'POBS_TRACKING': ['outputs/POBS CON TRACKING'],
            'BACKUP': ['outputs/Backup']
        }

        result = {}

        for feature, folders in folder_mappings.items():
            files = []
            for folder in folders:
                if os.path.exists(folder):
                    for filename in os.listdir(folder):
                        file_path = os.path.join(folder, filename)
                        if os.path.isfile(file_path):
                            stat = os.stat(file_path)
                            files.append({
                                'name': filename,
                                'path': folder,
                                'size': stat.st_size,
                                'created': stat.st_ctime,
                                'modified': stat.st_mtime,
                                'extension': os.path.splitext(filename)[1],
                                'download_url': f'/api/download/{filename}',
                                'preview_url': f'/api/historic/preview/{filename}' if filename.lower().endswith(('.xlsx', '.xls', '.csv')) else None
                            })

            # Sort by modification time (newest first)
            files.sort(key=lambda x: x['modified'], reverse=True)
            result[feature] = files

        return jsonify({
            'success': True,
            'data': result
        })

    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/historic/preview/<filename>')
@jwt_required()
def preview_file(filename):
    """Preview file content (supports both small preview and expanded view)"""
    try:
        # Search for file in all output directories
        search_dirs = [
            'outputs/PCOM', 'outputs/POBS', 'outputs/IMEI HUB',
            'outputs/GSPED', 'outputs/TRACKING RADAR',
            'outputs/POBS CON TRACKING', 'outputs/Backup', 'outputs/backup_POBS'
        ]

        file_path = None
        for search_dir in search_dirs:
            potential_path = os.path.join(search_dir, filename)
            if os.path.exists(potential_path):
                file_path = potential_path
                break

        if not file_path:
            return jsonify({'error': f'File "{filename}" not found'}), 404

        # Check file extension
        ext = os.path.splitext(filename)[1].lower()

        # Get limit from query parameter (default 10 for small preview, more for expanded)
        limit = request.args.get('limit', '10', type=int)
        limit = min(limit, 1000)  # Cap at 1000 rows for performance

        if ext in ['.xlsx', '.xls']:
            import pandas as pd
            df = pd.read_excel(file_path, dtype=str)
            preview_data = df.head(limit).fillna('').to_dict('records')
            columns = list(df.columns)

            return jsonify({
                'success': True,
                'data': {
                    'columns': columns,
                    'rows': preview_data,
                    'total_rows': len(df),
                    'filename': filename,
                    'type': 'excel',
                    'preview_limit': limit
                }
            })

        elif ext == '.csv':
            import pandas as pd
            df = pd.read_csv(file_path, dtype=str)
            preview_data = df.head(limit).fillna('').to_dict('records')
            columns = list(df.columns)

            return jsonify({
                'success': True,
                'data': {
                    'columns': columns,
                    'rows': preview_data,
                    'total_rows': len(df),
                    'filename': filename,
                    'type': 'csv',
                    'preview_limit': limit
                }
            })

        else:
            return jsonify({'error': 'File type not supported for preview'}), 400

    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/historic/delete/<filename>', methods=['DELETE'])
@jwt_required()
def delete_historic_file(filename):
    """Delete a historic file"""
    try:
        # Search for file in all output directories
        search_dirs = [
            'outputs/PCOM', 'outputs/POBS', 'outputs/IMEI HUB',
            'outputs/GSPED', 'outputs/TRACKING RADAR',
            'outputs/POBS CON TRACKING', 'outputs/Backup', 'outputs/backup_POBS'
        ]

        file_path = None
        for search_dir in search_dirs:
            potential_path = os.path.join(search_dir, filename)
            if os.path.exists(potential_path):
                file_path = potential_path
                break

        if not file_path:
            return jsonify({'error': f'File "{filename}" not found'}), 404

        os.remove(file_path)
        return jsonify({
            'success': True,
            'message': f'File "{filename}" deleted successfully'
        })

    except Exception as e:
        return jsonify({'error': str(e)}), 500

# ============================================================================
# Health Check
# ============================================================================

# ============================================================================
# Logging System
# ============================================================================

@app.route('/api/logs')
@jwt_required()
def get_operation_logs():
    """Get recent operation logs"""
    try:
        operation_type = request.args.get('type', None)  # POBS, PCOM, TRACKING, etc.
        limit = int(request.args.get('limit', 20))

        logs = operation_logger.get_operation_logs(operation_type, limit)
        return jsonify({
            'success': True,
            'logs': logs,
            'total': len(logs)
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/logs/<log_filename>')
@jwt_required()
def get_log_file(log_filename):
    """Download a specific log file"""
    try:
        # Ensure the filename is secure
        log_filename = secure_filename(log_filename)
        log_path = os.path.join('outputs', 'LOGS', log_filename)

        if os.path.exists(log_path):
            return send_file(log_path, as_attachment=True)
        else:
            return jsonify({'error': 'Log file not found'}), 404
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/logs/cleanup', methods=['POST'])
@jwt_required()
def cleanup_logs():
    """Clean up old log files"""
    try:
        days_to_keep = int(request.json.get('days', 30))
        cleaned_count = operation_logger.cleanup_old_logs(days_to_keep)

        return jsonify({
            'success': True,
            'message': f'Cleaned up {cleaned_count} old log files',
            'files_removed': cleaned_count
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500

# ============================================================================
# Real-time Logging with Server-Sent Events (SSE)
# ============================================================================

@app.route('/api/logs/create-session', methods=['POST'])
@jwt_required()
def create_log_session():
    """Create a new real-time logging session"""
    try:
        session_id = realtime_logger.create_session()
        return jsonify({
            'success': True,
            'session_id': session_id
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/logs/stream/<session_id>')
@jwt_required()
def stream_logs(session_id):
    """Stream logs for a specific session via Server-Sent Events"""
    try:
        return realtime_logger.get_sse_response(session_id)
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/logs/result/<session_id>')
@jwt_required()
def get_session_result(session_id):
    """Get the final result for a completed session"""
    try:
        result = realtime_logger.get_result(session_id)
        if result is None:
            return jsonify({'error': 'No result found for this session or session not completed'}), 404
        return jsonify(result)
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/logs/sessions', methods=['GET'])
@jwt_required()
def get_active_sessions():
    """Get list of active sessions"""
    try:
        sessions = realtime_logger.get_active_sessions()
        return jsonify({
            'success': True,
            'sessions': sessions,
            'count': len(sessions)
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/logs/cleanup/<session_id>', methods=['DELETE'])
@jwt_required()
def cleanup_session(session_id):
    """Manually cleanup a specific session"""
    try:
        realtime_logger.cleanup_session(session_id)
        return jsonify({
            'success': True,
            'message': f'Session {session_id} cleaned up successfully'
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/logs/cleanup', methods=['DELETE'])
@jwt_required()
def cleanup_all_sessions():
    """Emergency cleanup of all sessions"""
    try:
        realtime_logger.cleanup_all_sessions()
        return jsonify({
            'success': True,
            'message': 'All sessions cleaned up successfully'
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500

# ============================================================================
# Health Check
# ============================================================================

@app.route('/api/health')
def health_check():
    """Health check endpoint"""
    return jsonify({'status': 'healthy', 'service': 'EasyRent Backend'})

@app.route('/api/debug/file-columns', methods=['POST'])
@jwt_required()
def debug_file_columns():
    """Debug endpoint to check file columns"""
    try:
        file = request.files.get('file')
        if not file:
            return jsonify({'error': 'No file provided'}), 400

        # Save file temporarily
        import tempfile
        with tempfile.NamedTemporaryFile(suffix='.xlsx', delete=False) as tmp:
            file.save(tmp.name)

            # Read file and get columns
            import pandas as pd
            df = pd.read_excel(tmp.name, dtype=str)

            # Clean up
            os.unlink(tmp.name)

            return jsonify({
                'success': True,
                'columns': list(df.columns),
                'shape': df.shape,
                'sample_data': df.head(3).to_dict('records') if len(df) > 0 else []
            })

    except Exception as e:
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)