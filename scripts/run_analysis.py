#!/usr/bin/env python3

import argparse
from pathlib import Path
from src.utils.config import Config
from src.core.organizer import BookLibraryOrganizer


def main():
    parser = argparse.ArgumentParser(description="Library Organizer")
    parser.add_argument("--config", type=Path, default=Path("config.yaml"))
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    # Load configuration
    config = Config(args.config)

    # Initialize and run organizer
    organizer = BookLibraryOrganizer(config)
    organizer.scan_library()

    if not args.dry_run:
        organizer.reorganize_library()


if __name__ == "__main__":
    main()
