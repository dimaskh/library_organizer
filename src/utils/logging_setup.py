import logging
from typing import Optional


def get_logger(name: str, level: Optional[str] = None) -> logging.Logger:
    """Get configured logger instance"""
    logger = logging.getLogger(name)

    if not logger.handlers:  # Only configure if not already configured
        formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )

        # Console handler
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)

        # File handler
        file_handler = logging.FileHandler("library_organizer.log")
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

        # Set level
        logger.setLevel(level or logging.INFO)

    return logger
