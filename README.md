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

## Project Structure

```
library-organizer/
├── src/
│   ├── core/
│   │   ├── analyzer.py    # Book analysis functionality
│   │   ├── organizer.py   # Main organization logic
│   │   └── processor.py   # PDF processing functionality
│   ├── models/
│   │   └── book.py        # Book data model
│   └── utils/
│       ├── config.py      # Configuration handling
│       ├── exceptions.py  # Custom exceptions
│       └── logging_setup.py # Logging configuration
├── scripts/
│   └── run_analysis.py    # Entry point script
├── config/
│   └── default_config.yaml # Default configuration
├── tests/                 # Test files
├── requirements.txt       # Python dependencies
└── README.md             # Documentation
```

## Installation

1. Clone the repository:

```bash
git clone https://github.com/yourusername/library-organizer.git
cd library-organizer
```

2. Create and activate a virtual environment:

```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

3. Install dependencies:

```bash
pip install -r requirements.txt
```

## Configuration

Create a `config.yaml` file in the root directory:

```yaml
directories:
  input: "/path/to/your/books"
  output: "/path/to/organized/books"

topics:
  programming:
    - python
    - java
    - javascript
  # Add more topics...

reading_speeds:
  easy: 30
  moderate: 25
  hard: 20
  extreme: 15

advanced_topics:
  - algorithms
  - system design
  - machine learning
  # Add more...

intermediate_topics:
  - web development
  - testing
  - databases
  # Add more...

renowned_authors:
  - martin fowler
  - robert martin
  - donald knuth
  # Add more...
```

## Usage

You can use the following make commands to run the program:

```bash
# Show available commands
make help

# Install dependencies
make install      # Production dependencies
make dev-install  # Development dependencies

# Run the program
make run         # Normal run
make run-dry     # Dry run (no changes)

# Development tasks
make test        # Run tests
make lint        # Check code style
make format      # Format code
make clean       # Clean cache files
```

## Output

The tool generates:

- Organized book collection (if not in dry-run mode)
- Analysis report in JSON format (`library_analysis.json`)
- Log file (`library_organizer.log`)

## Development

1. Install development dependencies:

```bash
pip install -r requirements-dev.txt
```

2. Run tests:

```bash
pytest tests/
```

3. Run linting:

```bash
black .
flake8 .
```

## License

MIT License - see LICENSE file for details.
