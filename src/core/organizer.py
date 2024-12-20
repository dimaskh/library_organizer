from pathlib import Path
from typing import Dict, List
from .analyzer import BookAnalyzer
from .processor import PDFProcessor
from ..models.book import Book
from ..utils.logging_setup import get_logger


class BookLibraryOrganizer:
    def __init__(self, config: Dict):
        self.config = config
        self.logger = get_logger(__name__)
        self.analyzer = BookAnalyzer(config)
        self.processor = PDFProcessor(config)
        self.books: List[Book] = []

    def scan_library(self) -> None:
        """Main method to scan and process library"""
        pdf_files = self.processor.get_pdf_files()
        self.books = self.processor.process_files(pdf_files)
        self.analyzer.analyze_and_save(self.books)
