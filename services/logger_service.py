"""
Centralized Logging Service
Provides structured logging for all EasyRent operations
"""

import os
import json
from datetime import datetime
from typing import Dict, List, Any, Optional

class OperationLogger:
    """Centralized logger for all EasyRent operations"""

    def __init__(self, output_dir: str = "outputs"):
        self.output_dir = output_dir
        self.logs_dir = os.path.join(output_dir, "LOGS")
        os.makedirs(self.logs_dir, exist_ok=True)

    def log_operation(self,
                     operation_type: str,
                     operation_name: str,
                     status: str,
                     details: Dict[str, Any],
                     files_created: List[str] = None,
                     errors: List[str] = None) -> str:
        """
        Log an operation with structured data

        Args:
            operation_type: Type of operation (POBS, PCOM, TRACKING, etc.)
            operation_name: Specific operation name
            status: SUCCESS, ERROR, WARNING
            details: Dictionary with operation details
            files_created: List of files created during operation
            errors: List of error messages if any

        Returns:
            Path to the log file created
        """
        timestamp = datetime.now()
        log_filename = f"{operation_type}_{operation_name}_{timestamp.strftime('%Y%m%d_%H%M%S')}.log"
        log_path = os.path.join(self.logs_dir, log_filename)

        # Prepare log data
        log_data = {
            "timestamp": timestamp.isoformat(),
            "operation_type": operation_type,
            "operation_name": operation_name,
            "status": status,
            "details": details or {},
            "files_created": files_created or [],
            "errors": errors or [],
            "execution_time": timestamp.strftime('%Y-%m-%d %H:%M:%S')
        }

        # Write human-readable log
        with open(log_path, "w", encoding="utf-8") as f:
            f.write("=" * 80 + "\n")
            f.write(f"EASYRENT OPERATION LOG\n")
            f.write("=" * 80 + "\n")
            f.write(f"Operation Type: {operation_type}\n")
            f.write(f"Operation Name: {operation_name}\n")
            f.write(f"Status: {status}\n")
            f.write(f"Execution Time: {timestamp.strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write("=" * 80 + "\n\n")

            if details:
                f.write("OPERATION DETAILS:\n")
                f.write("-" * 40 + "\n")
                for key, value in details.items():
                    f.write(f"{key}: {value}\n")
                f.write("\n")

            if files_created:
                f.write("FILES CREATED:\n")
                f.write("-" * 40 + "\n")
                for file in files_created:
                    f.write(f"- {file}\n")
                f.write("\n")

            if errors:
                f.write("ERRORS/WARNINGS:\n")
                f.write("-" * 40 + "\n")
                for error in errors:
                    f.write(f"- {error}\n")
                f.write("\n")

            f.write("=" * 80 + "\n")
            f.write("End of Log\n")
            f.write("=" * 80 + "\n")

        # Also create a JSON version for programmatic access
        json_path = log_path.replace('.log', '.json')
        with open(json_path, "w", encoding="utf-8") as f:
            json.dump(log_data, f, indent=2, ensure_ascii=False)

        return log_filename

    def append_to_operation_log(self, operation_type: str, message: str):
        """Append a message to the latest operation log of a specific type"""
        # Find the latest log file for this operation type
        latest_log = None
        latest_time = None

        for filename in os.listdir(self.logs_dir):
            if filename.startswith(f"{operation_type}_") and filename.endswith('.log'):
                file_path = os.path.join(self.logs_dir, filename)
                file_time = os.path.getmtime(file_path)
                if latest_time is None or file_time > latest_time:
                    latest_time = file_time
                    latest_log = file_path

        if latest_log:
            with open(latest_log, "a", encoding="utf-8") as f:
                f.write(f"[{datetime.now().strftime('%H:%M:%S')}] {message}\n")

    def get_operation_logs(self, operation_type: Optional[str] = None, limit: int = 10) -> List[Dict]:
        """Get recent operation logs"""
        logs = []

        for filename in sorted(os.listdir(self.logs_dir), reverse=True):
            if filename.endswith('.json'):
                if operation_type and not filename.startswith(f"{operation_type}_"):
                    continue

                try:
                    with open(os.path.join(self.logs_dir, filename), 'r', encoding='utf-8') as f:
                        log_data = json.load(f)
                        log_data['log_file'] = filename.replace('.json', '.log')
                        logs.append(log_data)

                        if len(logs) >= limit:
                            break
                except:
                    continue

        return logs

    def cleanup_old_logs(self, days_to_keep: int = 30):
        """Clean up log files older than specified days"""
        import time
        cutoff_time = time.time() - (days_to_keep * 24 * 60 * 60)

        cleaned_count = 0
        for filename in os.listdir(self.logs_dir):
            file_path = os.path.join(self.logs_dir, filename)
            if os.path.getmtime(file_path) < cutoff_time:
                try:
                    os.remove(file_path)
                    cleaned_count += 1
                except:
                    pass

        return cleaned_count

# Global logger instance
operation_logger = OperationLogger()

def log_pobs_operation(operation_name: str, status: str, details: Dict, files_created: List[str] = None, errors: List[str] = None) -> str:
    """Helper function for POBS operations"""
    return operation_logger.log_operation("POBS", operation_name, status, details, files_created, errors)

def log_pcom_operation(operation_name: str, status: str, details: Dict, files_created: List[str] = None, errors: List[str] = None) -> str:
    """Helper function for PCOM operations"""
    return operation_logger.log_operation("PCOM", operation_name, status, details, files_created, errors)

def log_tracking_operation(operation_name: str, status: str, details: Dict, files_created: List[str] = None, errors: List[str] = None) -> str:
    """Helper function for TRACKING operations"""
    return operation_logger.log_operation("TRACKING", operation_name, status, details, files_created, errors)

def log_imei_operation(operation_name: str, status: str, details: Dict, files_created: List[str] = None, errors: List[str] = None) -> str:
    """Helper function for IMEI operations"""
    return operation_logger.log_operation("IMEI", operation_name, status, details, files_created, errors)