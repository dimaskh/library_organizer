# Library Organizer

A Python-based tool for organizing and analyzing PDF book collections. This tool helps manage technical books by extracting metadata, analyzing content, and organizing files based on topics and categories.

## Features

- Metadata extraction from PDF files
- Smart filename parsing
- Topic detection and categorization
- Duplicate file detection
- Reading time estimation
- Book difficulty assessment
- Library statistics and analysis
- Configurable organization structure

## Installation

1. Clone the repository:
```bash
git clone https://github.com/yourusername/library-organizer.git
cd library-organizer
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

## Configuration

The tool is configured through `config.yaml`. Key configuration options include:

- Logging settings
- Input/output directories
- File patterns
- Processing options
- Analysis settings
- Organization preferences

See `config.yaml` for detailed configuration options.

## Usage

1. Basic usage:
```bash
python library_organizer.py
```

2. With custom config:
```bash
python library_organizer.py --config path/to/config.yaml
```

## Output

The tool generates:
- Organized book collection (if enabled)
- Analysis report in JSON format
- Detailed log file

## Project Structure

```
library-organizer/
├── library_organizer.py  # Main script
├── config.yaml           # Configuration file
├── requirements.txt      # Python dependencies
├── README.md            # This file
└── library_analysis.json # Analysis output
```

## License

MIT License - see LICENSE file for details.
