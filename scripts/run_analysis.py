#!/usr/bin/env python3

import argparse
import logging
from pathlib import Path
from src.utils.config import Config
from src.core.organizer import BookLibraryOrganizer

# Configure logging
def setup_logging(config):
    """Setup logging configuration and clear previous log file."""
    log_path = Path(config["logging"]["file"])
    
    # Clear existing log file
    if log_path.exists():
        try:
            log_path.write_text('')
        except Exception as e:
            print(f"Warning: Could not clear log file: {e}")
    
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[
            logging.FileHandler(log_path),
            logging.StreamHandler()
        ]
    )
    logging.info("Started new analysis run")

def main():
    parser = argparse.ArgumentParser(description="Library Organizer")
    parser.add_argument("--config", type=Path, default=Path("config.yaml"))
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    # Load configuration
    config = Config(args.config)
    
    # Setup logging
    setup_logging(config)

    # Initialize and run organizer
    organizer = BookLibraryOrganizer(config)
    organizer.scan_library()

    if not args.dry_run:
        organizer.reorganize_library()

if __name__ == "__main__":
    main()
