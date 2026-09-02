"""
Structured logging utilities for the AI Gateway scheduler.
"""

import json
import logging
import sys
from datetime import datetime
from typing import Any


class StructuredLogger:
    """JSON-formatted structured logger for better observability."""

    def __init__(self, name: str):
        self.name = name
        self.logger = logging.getLogger(name)
        self.logger.setLevel(logging.INFO)

        # Console handler with JSON formatting
        if not self.logger.handlers:
            handler = logging.StreamHandler(sys.stdout)
            handler.setFormatter(logging.Formatter("%(message)s"))
            self.logger.addHandler(handler)

    def _log(self, level: str, message: str, **kwargs):
        """Log a structured message."""
        log_entry = {
            "timestamp": datetime.utcnow().isoformat(),
            "logger": self.name,
            "level": level,
            "message": message,
            **kwargs,
        }
        self.logger.info(json.dumps(log_entry))

    def info(self, message: str, **kwargs):
        """Log info level message."""
        self._log("INFO", message, **kwargs)

    def error(self, message: str, **kwargs):
        """Log error level message."""
        self._log("ERROR", message, **kwargs)

    def warning(self, message: str, **kwargs):
        """Log warning level message."""
        self._log("WARNING", message, **kwargs)

    def debug(self, message: str, **kwargs):
        """Log debug level message."""
        self._log("DEBUG", message, **kwargs)


def get_logger(name: str) -> StructuredLogger:
    """Get or create a structured logger."""
    return StructuredLogger(name)
