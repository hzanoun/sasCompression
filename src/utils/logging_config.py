import logging
from logging.handlers import RotatingFileHandler
import os

def setup_logging(log_level=None):
    """Configure logging with rotation and customizable level."""
    # Get log level from environment or use default
    log_level = log_level or os.getenv('LOG_LEVEL', 'INFO')
    log_file = os.getenv('LOG_FILE', 'compression_script.log')
    max_bytes = int(os.getenv('LOG_MAX_SIZE', 10485760))  # 10MB
    backup_count = int(os.getenv('LOG_BACKUP_COUNT', 5))

    # Create formatter
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    # Set up rotating file handler
    file_handler = RotatingFileHandler(
        log_file,
        maxBytes=max_bytes,
        backupCount=backup_count
    )
    file_handler.setFormatter(formatter)

    # Set up console handler
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)

    # Configure root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(getattr(logging, log_level.upper()))
    root_logger.addHandler(file_handler)
    root_logger.addHandler(console_handler)

    return root_logger