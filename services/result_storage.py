"""
Result Storage Service - Independent of logging system
Handles operation results separately from real-time logs
"""

import threading
import uuid
from typing import Dict, Optional

class ResultStorage:
    def __init__(self):
        # Store results with operation IDs
        self.results: Dict[str, dict] = {}
        # Track pending operations
        self.pending_operations: set = set()
        self.lock = threading.Lock()

    def create_operation_id(self) -> str:
        """Create a new operation ID for result tracking"""
        operation_id = str(uuid.uuid4())
        with self.lock:
            self.pending_operations.add(operation_id)
        return operation_id

    def store_result(self, operation_id: str, result: dict):
        """Store a result for an operation"""
        with self.lock:
            self.results[operation_id] = result
            # Remove from pending when result is stored
            self.pending_operations.discard(operation_id)

    def get_result(self, operation_id: str) -> Optional[dict]:
        """Get the result for an operation"""
        with self.lock:
            return self.results.get(operation_id)

    def clear_result(self, operation_id: str):
        """Clear a specific result"""
        with self.lock:
            if operation_id in self.results:
                del self.results[operation_id]
            self.pending_operations.discard(operation_id)

    def clear_all_results(self):
        """Clear all stored results"""
        with self.lock:
            self.results.clear()
            self.pending_operations.clear()

    def get_operation_status(self, operation_id: str) -> str:
        """Get operation status: 'pending', 'completed', 'not_found'"""
        with self.lock:
            if operation_id in self.results:
                return 'completed'
            elif operation_id in self.pending_operations:
                return 'pending'
            else:
                return 'not_found'

# Global instance
result_storage = ResultStorage()