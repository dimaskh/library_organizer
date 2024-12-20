#!/usr/bin/env python3

import os
import json
import hashlib
import shutil
import re
import warnings
import multiprocessing as mp
from functools import partial
from pathlib import Path
from typing import Dict, List, Set, Tuple, Optional
from tqdm import tqdm
from PyPDF2 import PdfReader
import math
import logging
import yaml
import argparse

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler("library_organizer.log"), logging.StreamHandler()],
)
logger = logging.getLogger(__name__)

# Suppress specific PyPDF2 warnings
warnings.filterwarnings("ignore", category=UserWarning, message=".*unknown widths.*")
warnings.filterwarnings(
    "ignore", category=UserWarning, message=".*Multiple definitions.*"
)
warnings.filterwarnings("ignore", category=UserWarning, message=".*startxref.*")
warnings.filterwarnings("ignore", category=UserWarning, message=".*Xref.*")
warnings.filterwarnings(
    "ignore", category=UserWarning, message=".*impossible to decode.*"
)
warnings.filterwarnings("ignore", category=DeprecationWarning, module="PyPDF2")


class BookLibraryOrganizer:
    def __init__(self, root_dir: str, log_level: str = "INFO"):
        """
        Initialize the library organizer.

        Args:
            root_dir: Root directory containing PDF files
            log_level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        """
        self.root_dir = Path(root_dir)
        self.book_data = {}
        self.duplicates = []
        self.total_size_saved = 0

        # Reading speed configuration (pages per day)
        self.reading_speeds = {
            "easy": 30,  # Can read faster as concepts are familiar
            "moderate": 25,  # Standard reading pace
            "hard": 20,  # Need more time to grasp concepts
            "extreme": 15,  # Requires deep focus and note-taking
        }

        # High-value technical topics for career growth
        self.high_value_topics = {
            "must_read": [
                r"system design|distributed systems|scalability|architecture",
                r"algorithms|data structures|problem solving|competitive",
                r"design patterns|clean code|refactoring|software engineering",
                r"machine learning|artificial intelligence|deep learning",
                r"security|cryptography|networking|protocols",
            ],
            "highly_valuable": [
                r"cloud computing|kubernetes|docker|microservices",
                r"database|sql|nosql|data modeling|optimization",
                r"testing|tdd|bdd|quality assurance|performance",
                r"agile|devops|ci/cd|site reliability|monitoring",
                r"web development|api design|rest|graphql",
            ],
            "career_growth": [
                r"leadership|management|team building",
                r"project management|scrum|kanban",
                r"communication|soft skills|collaboration",
                r"entrepreneurship|startup|innovation",
                r"productivity|time management|organization",
            ],
        }

        # Difficulty indicators with more precise categorization
        self.difficulty_indicators = {
            "easy": [
                r"beginner|basic|introduction|primer|fundamental",
                r"getting started|learn|simple|quick start",
                r"practical guide|hands-on|tutorial|101",
                r"essentials|fundamentals|basics",
            ],
            "moderate": [
                r"intermediate|professional|practical|handbook",
                r"guide|development|programming|implementation",
                r"cookbook|patterns|best practices",
                r"real-world|production|in action",
            ],
            "hard": [
                r"advanced|mastering|complete|comprehensive",
                r"architecture|design|principles|internals",
                r"performance|optimization|scalability",
                r"enterprise|professional|expert",
            ],
            "extreme": [
                r"theoretical|theory|academic|research|mathematical",
                r"formal methods|algorithms|computation|analysis",
                r"distributed|concurrent|parallel|quantum",
                r"compiler|kernel|low-level|operating system",
            ],
        }

        # Topics and subtopics
        self.topics = {
            "Programming Languages": {
                "patterns": [
                    r"python|java|c\+\+|javascript|ruby|go|rust|scala|kotlin|swift",
                    r"programming.*(language|tutorial|guide)",
                ],
                "subtopics": {
                    "Python": r"python",
                    "Java": r"java\b|java\s|java$",
                    "C/C++": r"c\+\+|\bc\b|c programming",
                    "JavaScript": r"javascript|js|node|react|angular|vue",
                    "Go": r"\bgo\b|golang",
                    "Other": r".*",
                },
            },
            "Software Engineering": {
                "patterns": [
                    r"software|engineering|architecture|design patterns|clean code",
                    r"refactoring|testing|agile|scrum|devops",
                ],
                "subtopics": {
                    "Design Patterns": r"pattern|design|architecture",
                    "Best Practices": r"clean code|refactoring|best practice|principle",
                    "Testing": r"test|tdd|bdd|quality",
                    "Agile & DevOps": r"agile|scrum|devops|continuous|deployment",
                },
            },
            "Computer Science": {
                "patterns": [
                    r"algorithm|data structure|complexity|computation|theory",
                    r"computer science|discrete|mathematics|database",
                ],
                "subtopics": {
                    "Algorithms": r"algorithm|complexity",
                    "Data Structures": r"data structure|collection",
                    "Theory": r"theory|computation|discrete|mathematics",
                    "Databases": r"database|sql|nosql|data modeling|optimization",
                },
            },
            "Artificial Intelligence": {
                "patterns": [
                    r"machine learning|deep learning|ai|artificial intelligence",
                    r"neural network|data science|nlp|computer vision",
                ],
                "subtopics": {
                    "Machine Learning": r"machine learning|ml|statistical learning",
                    "Deep Learning": r"deep learning|neural network|cnn|rnn",
                    "NLP": r"nlp|natural language|text processing|language model",
                    "Computer Vision": r"computer vision|image processing|opencv",
                },
            },
            "Web Development": {
                "patterns": [
                    r"web|html|css|javascript|frontend|backend",
                    r"http|rest|api|server|client",
                ],
                "subtopics": {
                    "Frontend": r"frontend|html|css|javascript|ui|ux",
                    "Backend": r"backend|server|api|rest|graphql",
                    "Full Stack": r"full.?stack|web development|mean|mern",
                },
            },
            "System & Infrastructure": {
                "patterns": [
                    r"linux|unix|windows|operating system|network",
                    r"cloud|kubernetes|docker|container|aws|azure",
                ],
                "subtopics": {
                    "Operating Systems": r"linux|unix|windows|os|operating system",
                    "Networking": r"network|tcp|ip|protocol|security",
                    "Cloud": r"cloud|aws|azure|gcp|serverless",
                    "Containers": r"container|docker|kubernetes|k8s",
                },
            },
            "Leadership & Self-Development": {
                "patterns": [
                    r"leadership|management|agile|team|productivity",
                    r"career|skill|improvement|success|habit",
                ],
                "subtopics": {
                    "Leadership": r"leadership|management|team|leading",
                    "Career Development": r"career|professional|skill|growth",
                    "Productivity": r"productivity|habit|success|improvement",
                },
            },
        }

        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(getattr(logging, log_level.upper()))

    def calculate_file_hash(self, filepath: Path) -> str:
        """Calculate SHA-256 hash of a file."""
        sha256_hash = hashlib.sha256()
        with open(filepath, "rb") as f:
            for byte_block in iter(lambda: f.read(4096), b""):
                sha256_hash.update(byte_block)
        return sha256_hash.hexdigest()

    def extract_book_info(self, filename: str) -> Tuple[str, str, str]:
        """Extract book name, author, and year from filename."""
        # Try to match different filename patterns
        patterns = [
            r"(.+?)\s*-\s*(.+?)\s*\[(\d{4})\]",  # Name - Author [Year]
            r"(.+?)\s*by\s*(.+?)\s*\[(\d{4})\]",  # Name by Author [Year]
            r"(.+?)\s*\((.+?)\)\s*\[(\d{4})\]",  # Name (Author) [Year]
        ]

        for pattern in patterns:
            match = re.search(pattern, filename)
            if match:
                name, author, year = match.groups()
                return name.strip(), author.strip(), year.strip()

        # If no pattern matches, return filename as name
        return filename.replace(".pdf", ""), "", ""

    def extract_author(self, file_path: Path) -> str:
        """Extract author name from filename."""
        filename = file_path.stem

        # Common patterns for author extraction
        patterns = [
            r"[-_]([^-_]+?)(?:\s*\[[0-9]{4}\]|\s*\([0-9]{4}\)|\s*$)",  # Author before year
            r"^([^-_]+?)(?:\s*-|_)",  # Author at start
            r"by\s+([^-_\[\]]+)",  # "by Author"
            r"([A-Z][^-_\[\]]+?)\s*[-_]",  # Capitalized name before delimiter
        ]

        for pattern in patterns:
            match = re.search(pattern, filename)
            if match:
                author = match.group(1).strip()
                # Clean up common artifacts
                author = re.sub(r"\s+", " ", author)  # normalize spaces
                author = re.sub(r"^\W+|\W+$", "", author)  # trim non-word chars
                if len(author) > 2:  # Avoid single letters or empty strings
                    return author

        return ""

    def extract_year(self, file_path: Path) -> str:
        """Extract publication year from filename."""
        filename = file_path.stem

        # Look for year in brackets or parentheses
        year_match = re.search(r"[\[\(](\d{4})[\]\)]", filename)
        if year_match:
            return year_match.group(1)

        # Look for 4-digit number that could be a year
        year_match = re.search(r"\b(19|20)\d{2}\b", filename)
        if year_match:
            return year_match.group(0)

        return ""

    def process_pdf(self, file_path: Path) -> Dict:
        """Process a PDF file and extract its information."""
        try:
            with open(file_path, "rb") as file:
                # Use a timeout to prevent hanging on corrupted PDFs
                pdf = PdfReader(file, strict=False)
                
                # Basic info that doesn't require content processing
                info = {
                    "page_count": len(pdf.pages),
                    "metadata": {}
                }
                
                # Extract metadata safely
                if pdf.metadata:
                    for key, value in pdf.metadata.items():
                        if isinstance(value, str):
                            info["metadata"][key] = value
                
                # Try to get text from first page only for topic detection
                try:
                    if pdf.pages and len(pdf.pages) > 0:
                        text = pdf.pages[0].extract_text()
                        info["first_page_text"] = text[:1000] if text else ""
                except Exception as e:
                    self.logger.warning(f"Could not extract text from first page of {file_path}: {str(e)}")
                    info["first_page_text"] = ""
                
                return info
                
        except Exception as e:
            self.logger.error(f"Error processing PDF {file_path}: {str(e)}")
            # Return minimal info on error
            return {
                "page_count": 0,
                "metadata": {},
                "first_page_text": ""
            }

    def extract_pdf_metadata(self, file_path: Path) -> Dict:
        """Extract metadata from PDF file."""
        try:
            with open(file_path, "rb") as file:
                try:
                    # Configure PyPDF2 to be more tolerant of errors
                    pdf = PdfReader(
                        file,
                        strict=False,  # Be more lenient with PDF spec violations
                        ignore_eof=True,  # Try to read even with EOF errors
                    )

                    try:
                        info = pdf.metadata
                    except Exception as e:
                        self.logger.debug(
                            f"Error reading metadata from {file_path}: {str(e)}"
                        )
                        info = {}

                    if not info:
                        self.logger.debug(f"No metadata found in {file_path}")
                        # Try to extract text from first page for potential metadata
                        try:
                            first_page_text = pdf.pages[0].extract_text()
                            metadata = self._extract_metadata_from_text(first_page_text)
                            if metadata:
                                self.logger.debug(
                                    f"Extracted metadata from first page text: {metadata}"
                                )
                                return metadata
                        except Exception as e:
                            self.logger.debug(
                                f"Could not extract text from first page: {str(e)}"
                            )
                        return {}

                    metadata = {}

                    # Process metadata with error handling for each field
                    for key, value in info.items():
                        try:
                            clean_key = key.strip("/")
                            if value and isinstance(value, (str, bytes)):
                                clean_value = self._clean_metadata_value(str(value))
                                if clean_value:
                                    metadata[clean_key.lower()] = clean_value
                        except Exception as e:
                            self.logger.debug(
                                f"Error processing metadata field {key}: {str(e)}"
                            )

                    # Extract and validate author
                    author = metadata.get("author") or metadata.get("creator")
                    if author and self._is_valid_author(author):
                        metadata["author"] = author

                    # Extract and validate year
                    year = self._extract_year_from_metadata(metadata)
                    if year:
                        metadata["year"] = year

                    # Clean up title
                    if "title" in metadata:
                        metadata["title"] = self._clean_title(metadata["title"])

                    return metadata

                except Exception as e:
                    error_type = type(e).__name__
                    if "PdfReadError" in error_type:
                        self.logger.debug(f"PDF read error in {file_path}: {str(e)}")
                    else:
                        self.logger.debug(f"Error processing PDF {file_path}: {str(e)}")
                    return {}

        except Exception as e:
            self.logger.error(f"Error accessing file {file_path}: {str(e)}")
            return {}

    def _clean_metadata_value(self, value: str) -> str:
        """Clean up metadata value."""
        if not value:
            return ""

        # Convert to string and clean up
        value = str(value).strip()

        # Remove common PDF artifacts
        value = re.sub(r"[\x00-\x1F\x7F-\xFF]", "", value)  # Remove non-printable chars
        value = re.sub(r"\\[0-9]+", "", value)  # Remove escape sequences
        value = re.sub(r"[^\w\s\-\.,:]", " ", value)  # Replace special chars with space
        value = " ".join(value.split())  # Normalize whitespace

        return value

    def _clean_title(self, title: str) -> str:
        """Clean up book title."""
        if not title:
            return ""

        title = self._clean_metadata_value(title)

        # Remove common title artifacts
        artifacts = [
            r"PDF",
            r"Ebook",
            r"Book",
            r"Download",
            r"Free",
            r"Copy",
            r"Version",
            r"\([^)]*\)",
            r"\[[^\]]*\]",
            r"www\.[^\s]+",
            r"https?://[^\s]+",
        ]

        for artifact in artifacts:
            title = re.sub(artifact, "", title, flags=re.IGNORECASE)

        return " ".join(title.split())

    def _extract_year_from_metadata(self, metadata: Dict) -> Optional[str]:
        """Extract and validate year from metadata."""
        year = None

        # Try different date fields
        date_fields = ["creationdate", "moddate", "date"]
        for field in date_fields:
            if field in metadata:
                # Try D:YYYY format
                match = re.search(r"D:(\d{4})", metadata[field])
                if match:
                    year = match.group(1)
                    break

                # Try plain YYYY format
                match = re.search(r"(\d{4})", metadata[field])
                if match:
                    year = match.group(1)
                    break

        # Validate year
        if year and year.isdigit():
            year_int = int(year)
            if 1900 <= year_int <= 2024:
                return year

        return None

    def _extract_metadata_from_text(self, text: str) -> Dict:
        """Extract metadata from text content."""
        metadata = {}

        # Try to find title (usually at the start)
        lines = text.split("\n")
        if lines:
            potential_title = lines[0].strip()
            if len(potential_title) > 3 and len(potential_title) < 200:
                metadata["title"] = self._clean_title(potential_title)

        # Try to find author (common patterns)
        author_patterns = [
            r"(?:Author|By|Written by)[:]\s*([A-Z][A-Za-z\s\.-]+)",
            r"([A-Z][A-Za-z\s\.-]+)(?:\s+\d{4})",
        ]

        for pattern in author_patterns:
            match = re.search(pattern, text)
            if match:
                potential_author = match.group(1)
                if self._is_valid_author(potential_author):
                    metadata["author"] = potential_author
                    break

        # Try to find year
        year_patterns = [
            r"(?:Published[:\s]+|Copyright[:\s]+)(\d{4})",
            r"(\d{4})",
        ]

        for pattern in year_patterns:
            match = re.search(pattern, text)
            if match:
                year = match.group(1)
                if 1900 <= int(year) <= 2024:
                    metadata["year"] = year
                    break

        return metadata

    def _is_valid_author(self, author: str) -> bool:
        """
        Check if the author string looks valid.

        Args:
            author: Author string to validate

        Returns:
            bool: True if author looks valid, False otherwise
        """
        if not author:
            return False

        author = str(author).lower()

        # List of invalid author strings
        invalid_authors = {
            "unknown",
            "administrator",
            "admin",
            "user",
            "guest",
            "tex",
            "latex",
            "adobe",
            "microsoft",
            "writer",
            "framemaker",
            "indesign",
            "pdf",
            "acrobat",
            "scanner",
            "scansnap",
            "copyright",
            "radical eye software",
            "www.",
            "http",
            ".com",
            ".org",
            ".net",
        }

        # Check for invalid authors
        if any(invalid in author.lower() for invalid in invalid_authors):
            return False

        # Check for minimum length and maximum length
        if len(author) < 2 or len(author) > 100:
            return False

        # Check for too many digits (likely a version number or date)
        if sum(c.isdigit() for c in author) > 4:
            return False

        # Check for too many special characters
        special_chars = sum(not c.isalnum() and not c.isspace() for c in author)
        if special_chars > len(author) * 0.3:  # More than 30% special characters
            return False

        return True

    def _find_duplicates(self, all_books: List[Dict]) -> List[Dict]:
        """Find duplicate books based on content and metadata."""
        duplicates = []
        seen_books = {}  # Dict to track unique books

        for book in all_books:
            # Create a unique signature for the book
            size = book["size"]
            page_count = book.get("page_count", 0)
            
            # Get all possible names for comparison
            names = {
                self._normalize_book_name(book["name"]),  # Current name
                self._normalize_book_name(Path(book["path"]).stem),  # Filename
            }
            
            # Add author-title combination if available
            if book.get("author") and book.get("name"):
                names.add(self._normalize_book_name(f"{book['author']} {book['name']}"))
            
            # Create signatures using each name variant
            signatures = {(size, page_count, name) for name in names}
            
            # Check if any signature matches existing books
            matched_sig = None
            matched_book = None
            for sig in signatures:
                if sig in seen_books:
                    matched_sig = sig
                    matched_book = seen_books[sig]
                    break
            
            if matched_book:
                # Compare paths to determine which one to keep
                existing_path = matched_book["path"]
                current_path = book["path"]
                
                # Prefer the file with better metadata
                existing_meta_score = self._metadata_completeness_score(matched_book)
                current_meta_score = self._metadata_completeness_score(book)
                
                if current_meta_score > existing_meta_score:
                    # Current book has better metadata, mark the existing one as duplicate
                    duplicates.append({
                        "original_path": current_path,
                        "duplicate_path": existing_path
                    })
                    # Update all signatures to point to the better version
                    for sig in signatures:
                        seen_books[sig] = book
                else:
                    # Keep the existing book, mark current as duplicate
                    duplicates.append({
                        "original_path": existing_path,
                        "duplicate_path": current_path
                    })
            else:
                # New unique book, store all its signatures
                for sig in signatures:
                    seen_books[sig] = book

        return duplicates

    def _normalize_book_name(self, name: str) -> str:
        """Normalize book name for comparison."""
        if not name:
            return ""
            
        # Remove common variations and clean the name
        name = name.lower()
        
        # Remove common prefixes
        prefixes = ['hands on', 'hands-on', 'the', 'a', 'an']
        for prefix in prefixes:
            if name.startswith(prefix + ' '):
                name = name[len(prefix)+1:]
        
        # Remove content in brackets and parentheses
        name = re.sub(r'[\[\(].*?[\]\)]', '', name)
        
        # Remove special characters but keep hyphens for compound words
        name = re.sub(r'[^\w\s-]', '', name)
        
        # Replace multiple spaces and hyphens with single ones
        name = re.sub(r'[-\s]+', ' ', name)
        
        # Remove common suffixes
        suffixes = ['.pdf', 'book', 'ebook', 'edition', 'ed']
        for suffix in suffixes:
            if name.endswith(' ' + suffix):
                name = name[:-len(suffix)-1]
        
        return name.strip()

    def _metadata_completeness_score(self, book: Dict) -> int:
        """Calculate a score representing metadata completeness."""
        score = 0
        
        # Author presence and quality
        author = book.get("author", "")
        if author:
            score += 2
            if len(author.split()) > 1:  # Full name is better
                score += 1
            if not any(x in author.lower() for x in ['unknown', 'various', 'anonymous']):
                score += 1
        
        # Year presence and validity
        year = book.get("year", "")
        if year and year.isdigit():
            year_int = int(year)
            if 1900 <= year_int <= 2024:
                score += 2
        
        # Title quality
        title = book.get("name", "")
        if title:
            score += 1
            if not title.endswith('.pdf'):
                score += 1
            if len(title.split()) > 2:  # More detailed title
                score += 1
        
        # Additional metadata
        if book.get("subject"):
            score += 1
        if book.get("keywords"):
            score += 1
        
        return score

    def _process_single_file(self, file_path: Path) -> Tuple[Path, Dict]:
        """Process a single file and extract its metadata."""
        try:
            # Extract metadata from PDF
            pdf_info = self.process_pdf(file_path)
            
            # Get filename-based metadata as fallback
            filename_info = self._parse_filename(file_path.stem)
            
            # Extract metadata with priority
            metadata = pdf_info.get("metadata", {})
            
            # Combine metadata with priority:
            # 1. PDF metadata
            # 2. Filename-based metadata
            # 3. Fallback to filename
            file_info = {
                "path": str(file_path.relative_to(self.root_dir)),
                "name": (self._clean_metadata_value(metadata.get("/Title", "")) or 
                        filename_info["title"] or 
                        self._clean_title(file_path.stem)),
                "author": (self._clean_metadata_value(metadata.get("/Author", "")) or 
                         filename_info["author"] or 
                         ""),
                "year": (metadata.get("/CreationDate", "")[:4] or 
                        filename_info["year"] or 
                        ""),
                "size": file_path.stat().st_size,
                "page_count": pdf_info.get("page_count", 0),
            }
            
            # Clean up the title
            if file_info["name"]:
                file_info["name"] = self._clean_title(file_info["name"])
            
            # Clean up the author
            if file_info["author"]:
                file_info["author"] = self._clean_metadata_value(file_info["author"])
                if not self._is_valid_author(file_info["author"]):
                    file_info["author"] = ""
            
            # Extract topics
            file_info["topics"] = self._extract_topics({
                "title": file_info["name"],
                "text": pdf_info.get("first_page_text", "")
            })
            
            # Calculate rating and difficulty
            file_info["rating"] = self.calculate_rating(file_info)
            file_info["difficulty"] = self.estimate_difficulty(file_info)
            
            # Calculate reading time
            file_info["reading_time_days"] = self.estimate_reading_time(
                file_info,
                file_info["difficulty"]
            )
            
            return file_path, file_info
            
        except Exception as e:
            self.logger.error(f"Error processing {file_path}: {str(e)}")
            return None

    def _parse_filename(self, file_path: str) -> Dict[str, str]:
        """
        Parse the filename to extract author and title information.
        Expected format: 'Title - Author [Year].pdf' or 'Author - Title [Year].pdf'
        
        Args:
            file_path: Path to the PDF file
            
        Returns:
            Dict with 'author', 'title', and 'year' keys
        """
        filename = os.path.basename(file_path)
        # Remove the .pdf extension if present
        filename = re.sub(r'\.pdf$', '', filename)
        
        # Initialize return dictionary
        result = {
            "author": "",
            "title": "",
            "year": ""
        }
        
        # Extract year if present
        year_match = re.search(r'\[(\d{4})\]$', filename)
        if year_match:
            result["year"] = year_match.group(1)
            # Remove year from filename for further processing
            filename = re.sub(r'\s*\[\d{4}\]$', '', filename)
        
        # Split by ' - ' to separate author and title
        parts = filename.split(' - ')
        
        if len(parts) == 2:
            # Check if first part looks like initials + surname
            if re.match(r'^[A-Z]\.\s*[A-Z]?\.?\s+[A-Z][a-z]+', parts[0]):
                result["author"] = parts[0].strip()
                result["title"] = parts[1].strip()
            else:
                result["author"] = parts[1].strip()
                result["title"] = parts[0].strip()
        else:
            # If we can't parse properly, use filename as title
            result["title"] = filename.strip()
        
        return result

    def analyze_library(self) -> Dict:
        """Analyze the library and generate statistics."""
        all_books = []
        
        # Get all PDF files
        pdf_files = self._get_pdf_files()
        self.logger.info(f"Found {len(pdf_files)} PDF files")
        
        # Process files with error handling
        with mp.Pool(mp.cpu_count()) as pool:
            try:
                # Process files in parallel with progress bar
                process_func = partial(self._process_single_file)
                with tqdm(
                    total=len(pdf_files),
                    desc="Processing files",
                    disable=self.logger.level >= logging.WARNING,
                ) as pbar:
                    for result in pool.imap_unordered(process_func, pdf_files):
                        if result:  # If file was processed successfully
                            file_path, file_info = result
                            all_books.append(file_info)
                            pbar.update(1)
            except KeyboardInterrupt:
                pool.terminate()
                pool.join()
                raise
            except Exception as e:
                self.logger.error(f"Error during parallel processing: {str(e)}")
                pool.terminate()
                pool.join()
                raise
        
        # Find and remove duplicates before analysis
        duplicates = self._find_duplicates(all_books)
        duplicate_paths = {dup["duplicate_path"] for dup in duplicates}
        all_books = [book for book in all_books if book["path"] not in duplicate_paths]
        
        # Calculate statistics
        total_size = sum(book["size"] for book in all_books)
        total_pages = sum(book.get("page_count", 0) for book in all_books)
        
        # Generate analysis
        analysis = {
            "summary": {
                "total_books": len(all_books),
                "total_size_bytes": total_size,
                "total_size_human": self._format_size(total_size),
                "total_pages": total_pages,
                "unique_authors": len({book["author"] for book in all_books if book["author"]}),
                "years_range": self._get_years_range(all_books),
            },
            "all_books": sorted(all_books, key=lambda x: x["path"]),
            "duplicates": duplicates
        }
        
        return analysis

    def _get_years_range(self, books: List[Dict]) -> str:
        """
        Get the range of years from the book collection.
        
        Args:
            books: List of book dictionaries
            
        Returns:
            String representing the year range (e.g., "2000-2023" or "2015" for single year)
        """
        years = []
        for book in books:
            year = book.get("year", "")
            if year and year.isdigit() and 1900 <= int(year) <= 2024:
                years.append(int(year))
                
        if not years:
            return "Unknown"
            
        min_year = min(years)
        max_year = max(years)
        
        if min_year == max_year:
            return str(min_year)
        else:
            return f"{min_year}-{max_year}"

    def _format_size(self, size_bytes: int) -> str:
        """Format file size in human-readable format."""
        for unit in ["B", "KB", "MB", "GB"]:
            if size_bytes < 1024:
                return f"{size_bytes:.2f} {unit}"
            size_bytes /= 1024
        return f"{size_bytes:.2f} TB"

    def estimate_difficulty(self, book_info: Dict) -> str:
        """Estimate book difficulty based on various factors."""
        complexity_score = 0

        # Check title and content indicators
        title_lower = book_info.get("name", "").lower()
        keywords = book_info.get("keywords", "").lower()
        subject = book_info.get("subject", "").lower()

        # Advanced topics increase complexity
        advanced_topics = {
            "advanced": 3,
            "expert": 3,
            "professional": 2,
            "architecture": 2,
            "design patterns": 2,
            "algorithms": 2,
            "data structures": 2,
            "distributed systems": 2,
            "machine learning": 2,
            "optimization": 1,
            "performance": 1,
            "security": 1,
        }

        # Basic topics decrease complexity
        basic_topics = {
            "introduction": -2,
            "beginner": -2,
            "basic": -2,
            "starter": -2,
            "fundamentals": -1,
            "essential": -1,
            "practical": -1,
            "guide": -1,
        }

        # Check topics
        for topic, score in advanced_topics.items():
            if topic in title_lower or topic in keywords or topic in subject:
                complexity_score += score

        for topic, score in basic_topics.items():
            if topic in title_lower or topic in keywords or topic in subject:
                complexity_score += score

        # Consider page count
        pages = book_info.get("page_count", 0)
        if pages > 800:
            complexity_score += 2
        elif pages > 600:
            complexity_score += 1
        elif pages < 200:
            complexity_score -= 1
        elif pages < 100:
            complexity_score -= 2

        # Consider technical depth (based on rating)
        rating = book_info.get("rating", 5.0)
        if rating >= 9.0:
            complexity_score += 2
        elif rating >= 8.0:
            complexity_score += 1
        elif rating <= 4.0:
            complexity_score -= 1

        # Map score to difficulty levels with better distribution
        if complexity_score <= -2:
            return "easy"
        elif complexity_score <= 0:
            return "moderate"
        elif complexity_score <= 2:
            return "hard"
        else:
            return "extreme"

    def calculate_rating(self, book_info: Dict) -> float:
        """Calculate book rating on a scale of 1-10."""
        base_rating = 5.0  # Start from middle
        rating = base_rating

        # High-value topics that boost rating significantly
        high_value_topics = {
            "algorithms": 2.5,
            "data structures": 2.5,
            "system design": 2.5,
            "distributed systems": 2.5,
            "architecture": 2.0,
            "design patterns": 2.0,
            "security": 2.0,
            "performance": 2.0,
            "machine learning": 2.0,
            "artificial intelligence": 2.0,
            "optimization": 1.5,
            "cloud computing": 1.5,
            "best practices": 1.5,
            "clean code": 1.5,
            "testing": 1.0,
            "devops": 1.0,
            "networking": 1.0,
            "databases": 1.0,
        }

        # Author reputation significantly impacts rating
        renowned_authors = {
            "donald knuth": 3.0,
            "martin fowler": 2.5,
            "robert martin": 2.5,
            "kent beck": 2.5,
            "brian kernighan": 2.5,
            "dennis ritchie": 2.5,
            "andrew tanenbaum": 2.5,
            "martin kleppmann": 2.5,
            "thomas cormen": 2.5,
            "erich gamma": 2.0,
            "steve mcconnell": 2.0,
            "robert sedgewick": 2.0,
            "james gosling": 2.0,
            "brendan eich": 1.5,
            "douglas crockford": 1.5,
            "alex xu": 1.5,
        }

        # Negative factors that decrease rating
        negative_factors = {
            "outdated": -2.5,
            "deprecated": -2.0,
            "basic": -1.5,
            "introduction": -1.0,
            "beginner": -1.0,
            "starter": -1.0,
        }

        title_lower = book_info.get("name", "").lower()
        keywords = book_info.get("keywords", "").lower()
        subject = book_info.get("subject", "").lower()
        author_lower = book_info.get("author", "").lower()

        # Apply topic-based adjustments
        topic_matches = 0
        for topic, weight in high_value_topics.items():
            if topic in title_lower or topic in keywords or topic in subject:
                rating += weight
                topic_matches += 1
                self.logger.debug(f"Rating adjusted by {weight} for topic: {topic}")

        # Normalize topic boost if too many matches
        if topic_matches > 3:
            rating = rating * 3 / topic_matches

        # Apply author reputation bonus
        for author, bonus in renowned_authors.items():
            if author in author_lower:
                rating += bonus
                self.logger.debug(f"Rating adjusted by {bonus} for author: {author}")
                break

        # Apply negative factors
        for factor, penalty in negative_factors.items():
            if factor in title_lower or factor in keywords or factor in subject:
                rating += penalty
                self.logger.debug(f"Rating adjusted by {penalty} for factor: {factor}")

        # Adjust based on page count (favor comprehensive books)
        pages = book_info.get("page_count", 0)
        if pages > 600:
            rating += 1.0
        elif pages > 400:
            rating += 0.5
        elif pages < 200:
            rating -= 1.0

        # Adjust based on year (favor recent books)
        year = book_info.get("year", "")
        if year and year.isdigit():
            year_int = int(year)
            if year_int >= 2020:
                rating += 1.0
            elif year_int >= 2015:
                rating += 0.5
            elif year_int < 2010:
                rating -= 1.0
            elif year_int < 2005:
                rating -= 2.0

        # Add some controlled randomization for better distribution
        import random

        rating += random.uniform(-0.3, 0.3)

        # Ensure rating stays within 1-10 range
        return round(max(1.0, min(10.0, rating)), 1)

    def scan_library(self):
        """Scan the library and collect information about all PDF files."""
        pdf_files = list(self.root_dir.rglob("*.pdf"))
        self.logger.info(f"Found {len(pdf_files)} PDF files")

        # Create a process pool
        num_cores = max(1, mp.cpu_count() - 1)  # Leave one core free
        pool = mp.Pool(num_cores)

        # Process files in parallel with progress bar
        process_func = partial(self._process_single_file)
        with tqdm(
            total=len(pdf_files),
            desc="Processing files",
            disable=self.logger.level >= logging.WARNING,
        ) as pbar:
            for result in pool.imap_unordered(process_func, pdf_files):
                if result:  # If file was processed successfully
                    file_path, file_info = result
                    self.book_data[str(file_path)] = file_info
                pbar.update()

        pool.close()
        pool.join()

        self.logger.info(f"Successfully processed {len(self.book_data)} files")

    def estimate_reading_time(self, book_info: Dict, difficulty: str) -> int:
        """
        Estimate reading time based on page count and difficulty.
        Returns estimated days to read the book.
        """
        page_count = book_info.get("page_count", 0)

        if not page_count:
            return None

        # Get pages per day based on difficulty
        pages_per_day = self.reading_speeds[difficulty]

        # Calculate reading days
        reading_days = math.ceil(page_count / pages_per_day)

        return reading_days

    def determine_book_topics(self, book_info: Dict) -> List[Tuple[str, str]]:
        """Determine the topics and subtopics for a book based on its title and author."""
        book_topics = []
        title_lower = book_info["name"].lower()

        for topic, topic_info in self.topics.items():
            # Check if book matches any topic patterns
            for pattern in topic_info["patterns"]:
                if re.search(pattern, title_lower, re.IGNORECASE):
                    # Find matching subtopic
                    subtopic = "Other"
                    for sub, sub_pattern in topic_info["subtopics"].items():
                        if re.search(sub_pattern, title_lower, re.IGNORECASE):
                            subtopic = sub
                            break
                    book_topics.append((topic, subtopic))
                    break

        return book_topics or [("Uncategorized", "General")]

    def suggest_topics(self) -> Dict:
        """Suggest topic organization based on book titles and current structure."""
        organized_topics = {
            topic: {sub: [] for sub in info["subtopics"].keys()}
            for topic, info in self.topics.items()
        }
        organized_topics["Uncategorized"] = {"General": []}

        for book_path, book_info in self.book_data.items():
            book_topics = self.determine_book_topics(book_info)
            book_info["rating"] = self.calculate_rating(book_info)
            book_info["topics"] = book_topics

            for topic, subtopic in book_topics:
                if topic in organized_topics and subtopic in organized_topics[topic]:
                    organized_topics[topic][subtopic].append(
                        {
                            "path": book_path,
                            "name": book_info["name"],
                            "author": book_info["author"],
                            "year": book_info["year"],
                            "rating": book_info["rating"],
                        }
                    )

        return organized_topics

    def generate_new_filename(self, book_info: Dict) -> str:
        """Generate standardized filename for a book."""
        name = book_info["name"].strip()
        author = book_info["author"].strip()
        year = book_info["year"].strip()

        # Clean up special characters
        name = re.sub(r'[<>:"/\\|?*]', "", name)
        author = re.sub(r'[<>:"/\\|?*]', "", author)

        if author and year:
            return f"{name} - {author} [{year}].pdf"
        elif author:
            return f"{name} - {author}.pdf"
        elif year:
            return f"{name} [{year}].pdf"
        else:
            return f"{name}.pdf"

    def reorganize_library(self, dry_run: bool = True):
        """Reorganize the library according to suggested structure."""
        if dry_run:
            self.logger.info("Running in DRY RUN mode - no changes will be made")
        else:
            self.logger.info("Running in LIVE mode - changes will be applied")

        # Process duplicates
        if self.duplicates:
            self.logger.info("Processing duplicates...")
            for dup in self.duplicates:
                if not dry_run:
                    dup_file = self.root_dir / dup["duplicate_path"]
                    dup_file.unlink()  # Remove duplicate
                self.logger.info(
                    f"{'Would remove' if dry_run else 'Removed'} duplicate: {dup['duplicate_path']}"
                )

        # Rename files to standard format
        self.logger.info("Processing file names...")
        for book_path, book_info in self.book_data.items():
            new_name = self.generate_new_filename(book_info)
            old_path = self.root_dir / book_path
            new_path = old_path.parent / new_name

            if old_path != new_path:
                if not dry_run:
                    old_path.rename(new_path)
                self.logger.info(
                    f"{'Would rename' if dry_run else 'Renamed'}: {book_path} -> {new_name}"
                )

    def _get_pdf_files(self) -> List[Path]:
        """Get all PDF files in the library."""
        return list(self.root_dir.rglob("*.pdf"))

    def _extract_topics(self, text: Dict) -> List[str]:
        """
        Extract topics from text based on predefined patterns.
        
        Args:
            text: Dictionary containing 'title' and 'text' keys
            
        Returns:
            List of identified topics
        """
        topics = []
        
        try:
            # Convert text to lowercase for case-insensitive matching
            title = text.get("title", "").lower() if isinstance(text.get("title"), str) else ""
            content = text.get("text", "").lower() if isinstance(text.get("text"), str) else ""
            
            # Check each category of topics
            for category, patterns in self.high_value_topics.items():
                for pattern in patterns:
                    if re.search(pattern, title, re.IGNORECASE) or re.search(pattern, content, re.IGNORECASE):
                        topics.append(category)
                        break  # Only add category once
                        
            return list(set(topics))  # Remove duplicates
            
        except Exception as e:
            self.logger.error(f"Error extracting topics: {str(e)}")
            return []


def load_config(config_path: str = "config.yaml") -> dict:
    """Load configuration from YAML file."""
    try:
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)
        return config
    except Exception as e:
        print(f"Error loading config file: {str(e)}")
        return {}

def setup_logging(config: dict) -> None:
    """Setup logging configuration."""
    log_config = config.get('logging', {})
    logging.basicConfig(
        level=getattr(logging, log_config.get('level', 'INFO')),
        format=log_config.get('format', '%(asctime)s - %(levelname)s - %(message)s'),
        filename=log_config.get('file'),
        filemode='a'
    )

def main():
    """Main entry point."""
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Organize and analyze PDF book library')
    parser.add_argument('--config', default='config.yaml', help='Path to config file')
    args = parser.parse_args()

    # Load configuration
    config = load_config(args.config)
    
    # Setup logging
    setup_logging(config)
    
    # Initialize organizer with config
    organizer = BookLibraryOrganizer(
        root_dir=config['directories']['input'],
        log_level=config['logging']['level']
    )
    
    # Run analysis
    analysis = organizer.analyze_library()
    
    # Save analysis if enabled
    if config['analysis']['save_json']:
        analysis_path = Path(config['directories']['analysis']) / config['analysis']['json_file']
        with open(analysis_path, 'w') as f:
            json.dump(analysis, f, indent=2)
    
    # Reorganize if not in dry run mode
    if not config['organization']['dry_run']:
        organizer.reorganize_library()

if __name__ == "__main__":
    main()
