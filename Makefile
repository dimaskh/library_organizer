export PYTHONPATH := .

.PHONY: help install dev-install run run-dry clean test lint format

# Default target when just running 'make'
help:
	@echo "Available commands:"
	@echo "  make install      - Install production dependencies"
	@echo "  make dev-install  - Install development dependencies"
	@echo "  make run         - Run the library organizer"
	@echo "  make run-dry     - Run in dry-run mode (no changes)"
	@echo "  make clean       - Remove Python cache files"
	@echo "  make test        - Run tests"
	@echo "  make lint        - Run linters"
	@echo "  make format      - Format code with black"

# Create and activate virtual environment
venv:
	python -m venv .venv

# Install production dependencies
install: venv
	.venv/bin/python -m pip install -r requirements.txt

# Install development dependencies
dev-install: venv
	.venv/bin/python -m pip install -r requirements.txt
	.venv/bin/python -m pip install -r requirements-dev.txt

# Run the library organizer
run:
	.venv/bin/python scripts/run_analysis.py

# Run in dry-run mode
run-dry:
	.venv/bin/python scripts/run_analysis.py --dry-run

# Clean Python cache files
clean:
	find . -type d -name "__pycache__" -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete
	find . -type f -name "*.pyo" -delete
	find . -type f -name "*.pyd" -delete
	find . -type f -name ".coverage" -delete
	find . -type d -name "*.egg-info" -exec rm -rf {} +
	find . -type d -name "*.egg" -exec rm -rf {} +
	find . -type d -name ".pytest_cache" -exec rm -rf {} +
	find . -type d -name ".tox" -exec rm -rf {} +
	find . -type d -name "build" -exec rm -rf {} +
	find . -type d -name "dist" -exec rm -rf {} +

# Run tests
test:
	.venv/bin/pytest tests/ -v

# Run linters
lint:
	.venv/bin/flake8 src/ tests/
	.venv/bin/black --check src/ tests/

# Format code
format:
	.venv/bin/black src/ tests/