from pathlib import Path
from typing import Dict, List
import warnings
from PyPDF2 import PdfReader
from ..models.book import Book
from ..utils.logging_setup import get_logger
from tqdm import tqdm


class PDFProcessor:
    def __init__(self, config: Dict):
        self.config = config
        self.logger = get_logger(__name__)
        self._configure_warnings()

    def _configure_warnings(self) -> None:
        """Configure PyPDF2 warnings"""
        width_warn = ".*unknown widths.*"
        multi_warn = ".*Multiple definitions.*"
        warnings.filterwarnings(
            "ignore", category=UserWarning, message=width_warn
        )
        warnings.filterwarnings(
            "ignore", category=UserWarning, message=multi_warn
        )
        warnings.filterwarnings(
            "ignore", category=UserWarning, message=".*startxref.*"
        )
        warnings.filterwarnings(
            "ignore", category=DeprecationWarning, module="PyPDF2"
        )

    def get_pdf_files(self) -> List[Path]:
        """Get all PDF files from configured directory"""
        root_dir = Path(self.config["directories"]["input"])
        return list(root_dir.rglob("*.pdf"))

    def process_files(self, files: List[Path]) -> List[Book]:
        """Process PDF files and return Book objects"""
        books = []
        errors = 0
        with tqdm(files, desc="Processing PDFs", unit="file") as pbar:
            for file in pbar:
                try:
                    metadata = self._extract_metadata(file)
                    books.append(self._create_book(file, metadata))
                except Exception as e:
                    errors += 1
                    # Update progress bar description with error count
                    pbar.set_description(f"Processing PDFs (Errors: {errors})")
                    # Log only the error type and file name for cleaner output
                    self.logger.error(
                        f"{type(e).__name__} processing {file.name}"
                    )

        if errors:
            self.logger.warning(f"Completed with {errors} errors")
        return books

    def _extract_metadata(self, file: Path) -> Dict:
        """Extract metadata from PDF file"""
        try:
            reader = PdfReader(str(file))
            info = reader.metadata or {}  # Handle None metadata

            # Extract title from metadata or filename
            title = info.get("/Title")
            if not title:
                # Clean up filename by removing extension
                # and replacing underscores
                title = file.stem.replace("_", " ")

            # Get page count safely
            try:
                page_count = len(reader.pages)
            except:
                page_count = 0

            return {
                "title": title,
                "author": info.get("/Author"),
                "year": None,  # We'll parse this from filename later
                "page_count": page_count,
                "size_bytes": file.stat().st_size,
            }
        except Exception as e:
            self.logger.error(
                f"Failed to extract metadata from {file}: {str(e)}"
            )
            # Return basic metadata when PDF processing fails
            return {
                "title": file.stem.replace("_", " "),
                "author": None,
                "year": None,
                "page_count": 0,
                "size_bytes": file.stat().st_size,
            }

    def _create_book(self, file: Path, metadata: Dict) -> Book:
        """Create Book object from file and metadata"""
        return Book(
            path=file,
            title=metadata["title"],
            author=metadata["author"],
            year=metadata["year"],
            page_count=metadata["page_count"],
            size_bytes=metadata["size_bytes"],
            topics=self._detect_topics(file),
            difficulty=None,  # Will be determined later
            rating=None,  # Will be determined later
        )

    def _detect_topics(self, file: Path) -> List[str]:
        """Detect topics based on file path"""
        # Get the parent folder name as the topic
        topic = file.parent.name
        return [topic] if topic != "input" else []
