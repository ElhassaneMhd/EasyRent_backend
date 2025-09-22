"""
Real-time logging service using Server-Sent Events (SSE)
Provides real-time log streaming for long-running operations
"""

import json
import threading
import time
from flask import Response
from typing import Dict, List, Callable
import uuid

class RealTimeLogger:
    def __init__(self):
        # Store active connections and their log queues
        self.connections: Dict[str, List[str]] = {}
        # Store final results for each session
        self.results: Dict[str, dict] = {}
        self.lock = threading.Lock()

    def create_session(self) -> str:
        """Create a new logging session and return session ID"""
        session_id = str(uuid.uuid4())
        with self.lock:
            self.connections[session_id] = []
        return session_id

    def log(self, session_id: str, message: str, level: str = "info"):
        """Add a log message to a specific session"""
        with self.lock:
            if session_id in self.connections:
                if level == "info":
                    self.connections[session_id].append(f"[INFO] {message}")
                elif level == "success":
                    self.connections[session_id].append(f"[OK] {message}")
                elif level == "warning":
                    self.connections[session_id].append(f"[WARNING] {message}")
                elif level == "error":
                    self.connections[session_id].append(f"[ERROR] {message}")
                elif level == "complete":
                    self.connections[session_id].append("__COMPLETE__")
                else:
                    self.connections[session_id].append(message)

    def log_info(self, session_id: str, message: str):
        """Log an info message"""
        self.log(session_id, message, "info")

    def log_ok(self, session_id: str, message: str):
        """Log a success message"""
        self.log(session_id, message, "success")

    def log_warning(self, session_id: str, message: str):
        """Log a warning message"""
        self.log(session_id, message, "warning")

    def log_error(self, session_id: str, message: str):
        """Log an error message"""
        self.log(session_id, message, "error")

    def store_result(self, session_id: str, result: dict):
        """Store the final result for a session"""
        with self.lock:
            self.results[session_id] = result

    def get_result(self, session_id: str) -> dict:
        """Get the final result for a session"""
        with self.lock:
            return self.results.get(session_id)

    def complete_session(self, session_id: str):
        """Mark a session as complete"""
        self.log(session_id, "", "complete")

    def stream_logs(self, session_id: str):
        """Generator function to stream logs via SSE"""
        last_index = 0

        while True:
            with self.lock:
                if session_id not in self.connections:
                    break

                logs = self.connections[session_id]
                new_logs = logs[last_index:]

                for log in new_logs:
                    if log == "__COMPLETE__":
                        # Session completed, clean up and close
                        del self.connections[session_id]
                        yield f"data: {json.dumps({'type': 'complete'})}\n\n"
                        return
                    else:
                        yield f"data: {json.dumps({'type': 'log', 'message': log})}\n\n"

                last_index = len(logs)

            time.sleep(0.1)  # Small delay to prevent busy waiting

    def get_sse_response(self, session_id: str) -> Response:
        """Get Flask Response object for SSE streaming"""
        return Response(
            self.stream_logs(session_id),
            mimetype='text/event-stream',
            headers={
                'Cache-Control': 'no-cache',
                'Connection': 'keep-alive',
                'Access-Control-Allow-Origin': '*',
                'Access-Control-Allow-Headers': 'Cache-Control'
            }
        )

    def cleanup_session(self, session_id: str):
        """Manually cleanup a session (in case of errors)"""
        with self.lock:
            if session_id in self.connections:
                del self.connections[session_id]
            if session_id in self.results:
                del self.results[session_id]

    def get_active_sessions(self):
        """Get list of active session IDs"""
        with self.lock:
            return list(self.connections.keys())

    def cleanup_all_sessions(self):
        """Clean up all active sessions (emergency cleanup)"""
        with self.lock:
            self.connections.clear()
            self.results.clear()

# Global instance
realtime_logger = RealTimeLogger()