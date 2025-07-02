"""
Logging configuration for the DB Foreign Key Analyzer.
"""
import logging
import sys
from typing import Optional

def setup_logging(level: str = "INFO", log_file: Optional[str] = None) -> None:
    """
    Setup logging configuration for the application.
    
    Args:
        level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        log_file: Optional log file path
    """
    # Convert string level to logging constant
    numeric_level = getattr(logging, level.upper(), logging.INFO)
    
    # Create formatter
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    # Setup root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(numeric_level)
    
    # Clear existing handlers
    root_logger.handlers.clear()
    
    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(numeric_level)
    console_handler.setFormatter(formatter)
    root_logger.addHandler(console_handler)
    
    # File handler (optional)
    if log_file:
        file_handler = logging.FileHandler(log_file)
        file_handler.setLevel(numeric_level)
        file_handler.setFormatter(formatter)
        root_logger.addHandler(file_handler)
    
    # Suppress noisy third-party loggers
    logging.getLogger('urllib3').setLevel(logging.WARNING)
    logging.getLogger('requests').setLevel(logging.WARNING)
    logging.getLogger('sqlalchemy.engine').setLevel(logging.WARNING)


class StreamlitLogHandler(logging.Handler):
    """Custom log handler for Streamlit applications."""
    
    def __init__(self):
        super().__init__()
        self.logs = []
        self.max_logs = 100
    
    def emit(self, record):
        """Emit a log record."""
        try:
            msg = self.format(record)
            self.logs.append({
                'timestamp': record.created,
                'level': record.levelname,
                'message': msg,
                'logger': record.name
            })
            
            # Keep only the last max_logs entries
            if len(self.logs) > self.max_logs:
                self.logs = self.logs[-self.max_logs:]
                
        except Exception:
            self.handleError(record)
    
    def get_logs(self, level: Optional[str] = None):
        """Get logs, optionally filtered by level."""
        if level:
            return [log for log in self.logs if log['level'] == level]
        return self.logs
    
    def clear_logs(self):
        """Clear all stored logs."""
        self.logs.clear()


# Global instance for Streamlit
streamlit_handler = StreamlitLogHandler()
