import re
import json
import multiprocessing as mp
import logging
from functools import partial
from pathlib import Path
from typing import Dict, List, Tuple
from tqdm import tqdm
from PyPDF2 import PdfReader
from ..models.book import Book

logger = logging.getLogger(__name__)


class BookAnalyzer:
    def __init__(self, config: Dict):
        self.config = config
        self.analysis_dir = Path(config["directories"]["analysis"])
        self.analysis_dir.mkdir(exist_ok=True)

        # Port difficulty indicators from legacy
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

        # Port topics structure from legacy
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
            # ... (add other topics from legacy)
        }

    def _extract_metadata_from_filename(
        self, filepath: Path
    ) -> Tuple[str, str, int]:
        """Extract author, title and year from filename pattern."""
        filename = filepath.stem  # Get filename without extension

        # Common filename patterns:
        # 1. "A. Author - Book Title [YYYY]"
        # 2. "Author A. - Book Title [YYYY]"
        # 3. "Book Title - Author A. [YYYY]"

        patterns = [
            r"^(?P<author>[\w\s\.,]+?)\s*-\s*(?P<title>.*?)\s*\[(?P<year>\d{4})\]",  # A. Author - Title [Year]
            r"^(?P<title>.*?)\s*-\s*(?P<author>[\w\s\.,]+?)\s*\[(?P<year>\d{4})\]",  # Title - A. Author [Year]
            r"^(?P<author>[\w\s\.,]+?)\s*-\s*(?P<title>.*?)(?:\s*\[(?P<year>\d{4})\])?$",  # A. Author - Title [optional Year]
        ]

        for pattern in patterns:
            match = re.match(pattern, filename)
            if match:
                data = match.groupdict()
                return (
                    data.get("author", "").strip(),
                    data.get("title", "").strip(),
                    int(data["year"]) if data.get("year") else None,
                )

        return None, filename, None

    def _extract_metadata_from_pdf(
        self, filepath: Path
    ) -> Tuple[str, str, int]:
        """Extract metadata from PDF file."""
        try:
            with open(filepath, "rb") as file:
                pdf = PdfReader(file)
                info = pdf.metadata

                if info:
                    author = info.get("/Author", "")
                    title = info.get("/Title", "")
                    # Try to extract year from /CreationDate or /ModDate
                    year = None
                    for date_field in ["/CreationDate", "/ModDate"]:
                        if date_field in info:
                            date_str = info[date_field]
                            # Format is typically 'D:YYYYMMDDHHmmSS'
                            year_match = re.search(r"D:(\d{4})", date_str)
                            if year_match:
                                year = int(year_match.group(1))
                                break

                    return author, title, year
        except Exception as e:
            logger.debug(
                f"Failed to extract PDF metadata from {filepath}: {str(e)}"
            )

        return None, None, None

    def determine_difficulty(self, book: Book) -> str:
        """Determine book difficulty using multiple factors."""
        score = 0.0
        
        # Content complexity indicators
        complexity_indicators = {
            "easy": [
                r"beginner|basic|introduction|primer",
                r"getting.?started|learn|simple",
                r"fundamentals|basics|essential",
            ],
            "moderate": [
                r"intermediate|practical|handbook",
                r"guide|development|implementation",
                r"cookbook|patterns|practices",
            ],
            "hard": [
                r"advanced|mastering|complete",
                r"architecture|design|principles",
                r"performance|optimization",
            ],
            "extreme": [
                r"theoretical|theory|academic",
                r"formal.?methods|computation",
                r"distributed|concurrent|parallel",
            ]
        }
        
        # Check title and content for complexity indicators
        title_lower = book.title.lower()
        for level, patterns in complexity_indicators.items():
            for pattern in patterns:
                if re.search(pattern, title_lower):
                    score += {
                        "easy": -2,
                        "moderate": 0,
                        "hard": 2,
                        "extreme": 4
                    }[level]
                    break
        
        # Topic-based complexity
        topic_complexity = {
            "Computer Science": {
                "Theory": 3,
                "Algorithms": 2,
                "Data Structures": 2,
                "Compilers": 3,
                "Operating Systems": 2
            },
            "Artificial Intelligence": {
                "Deep Learning": 3,
                "Machine Learning": 2,
                "Statistics": 2
            }
            # ... more topic complexities
        }
        
        # Adjust score based on topics
        for topic, subtopic in book.topics:
            if topic in topic_complexity and subtopic in topic_complexity[topic]:
                score += topic_complexity[topic][subtopic]
        
        # Page count impact
        if book.page_count > 600:
            score += 2
        elif book.page_count > 400:
            score += 1
        elif book.page_count < 200:
            score -= 1
        
        # Map final score to difficulty levels
        if score <= -2:
            return "easy"
        elif score <= 1:
            return "moderate"
        elif score <= 3:
            return "hard"
        else:
            return "extreme"

    def determine_topics(self, book: Book) -> List[Tuple[str, str]]:
        """Port of legacy determine_book_topics."""
        book_topics = []
        title_lower = book.title.lower()

        for topic, topic_info in self.topics.items():
            for pattern in topic_info["patterns"]:
                if re.search(pattern, title_lower, re.IGNORECASE):
                    subtopic = "Other"
                    for sub, sub_pattern in topic_info["subtopics"].items():
                        if re.search(sub_pattern, title_lower, re.IGNORECASE):
                            subtopic = sub
                            break
                    book_topics.append((topic, subtopic))
                    break

        return book_topics or [("Uncategorized", "General")]

    def analyze_books(self, books: List[Book]) -> Dict:
        """Analyze book collection using parallel processing."""
        with mp.Pool(mp.cpu_count()) as pool:
            process_func = partial(self._process_single_book)
            with tqdm(total=len(books), desc="Analyzing books") as pbar:
                processed_books = []
                for result in pool.imap_unordered(process_func, books):
                    if result:
                        processed_books.append(result)
                        pbar.update(1)

        # Generate analysis with enhanced summary
        analysis = {
            "summary": {
                # Basic stats
                "total_books": len(processed_books),
                "total_size_bytes": sum(book.size_bytes for book in processed_books),
                "total_pages": sum(book.page_count for book in processed_books),
                "average_pages": round(sum(book.page_count for book in processed_books) / len(processed_books)) if processed_books else 0,
                "unique_authors": len({book.author for book in processed_books if book.author}),
                "years_range": self._get_years_range(processed_books),
                
                # Ratings distribution
                "ratings": {
                    "average": round(sum(book.rating for book in processed_books) / len(processed_books), 1),
                    "distribution": self._get_distribution([book.rating for book in processed_books]),
                    "by_value": {
                        "excellent (9-10)": len([b for b in processed_books if b.rating >= 9]),
                        "very_good (7-8)": len([b for b in processed_books if 7 <= b.rating <= 8]),
                        "good (5-6)": len([b for b in processed_books if 5 <= b.rating <= 6]),
                        "average (3-4)": len([b for b in processed_books if 3 <= b.rating <= 4]),
                        "poor (1-2)": len([b for b in processed_books if b.rating <= 2])
                    }
                },
                
                # Difficulty distribution
                "difficulties": {
                    "distribution": self._get_distribution([book.difficulty for book in processed_books]),
                    "by_level": {
                        "easy": len([b for b in processed_books if b.difficulty == "easy"]),
                        "moderate": len([b for b in processed_books if b.difficulty == "moderate"]),
                        "hard": len([b for b in processed_books if b.difficulty == "hard"]),
                        "extreme": len([b for b in processed_books if b.difficulty == "extreme"])
                    }
                },
                
                # Topics summary
                "topics": self._summarize_topics(processed_books)
            },
            "books": [self._book_to_dict(book) for book in processed_books]
        }

        return analysis

    def _get_distribution(self, values: List[any]) -> Dict:
        """Calculate distribution of values."""
        if not values:
            return {}
        
        from collections import Counter
        counts = Counter(values)
        total = len(values)
        
        return {
            str(key): {
                "count": count,
                "percentage": round(count / total * 100, 1)
            }
            for key, count in counts.items()
        }

    def _summarize_topics(self, books: List[Book]) -> Dict:
        """Create summary of topics and subtopics."""
        topic_summary = {}
        
        # Count books per topic and subtopic
        for book in books:
            for topic, subtopic in book.topics:
                if topic not in topic_summary:
                    topic_summary[topic] = {
                        "total_books": 0,
                        "subtopics": {},
                        "average_rating": 0,
                        "books_by_difficulty": {
                            "easy": 0,
                            "moderate": 0,
                            "hard": 0,
                            "extreme": 0
                        }
                    }
                
                topic_data = topic_summary[topic]
                topic_data["total_books"] += 1
                topic_data["books_by_difficulty"][book.difficulty] += 1
                
                if subtopic not in topic_data["subtopics"]:
                    topic_data["subtopics"][subtopic] = {
                        "total_books": 0,
                        "average_rating": 0,
                        "books_by_difficulty": {
                            "easy": 0,
                            "moderate": 0,
                            "hard": 0,
                            "extreme": 0
                        }
                    }
                
                subtopic_data = topic_data["subtopics"][subtopic]
                subtopic_data["total_books"] += 1
                subtopic_data["books_by_difficulty"][book.difficulty] += 1
        
        # Calculate averages
        for topic, topic_data in topic_summary.items():
            topic_books = [b for b in books if topic in [t[0] for t in b.topics]]
            topic_data["average_rating"] = round(
                sum(b.rating for b in topic_books) / len(topic_books), 1
            ) if topic_books else 0
            
            for subtopic, subtopic_data in topic_data["subtopics"].items():
                subtopic_books = [b for b in books if (topic, subtopic) in b.topics]
                subtopic_data["average_rating"] = round(
                    sum(b.rating for b in subtopic_books) / len(subtopic_books), 1
                ) if subtopic_books else 0
        
        return topic_summary

    def _process_single_book(self, book: Book) -> Book:
        """Process a single book with all analysis steps."""
        try:
            # Try to get metadata from PDF first
            pdf_author, pdf_title, pdf_year = self._extract_metadata_from_pdf(
                book.path
            )

            # If PDF metadata is incomplete, try filename
            file_author, file_title, file_year = (
                self._extract_metadata_from_filename(book.path)
            )

            # Use the best available data
            book.author = pdf_author or file_author or book.author
            book.title = pdf_title or file_title or book.title
            book.year = pdf_year or file_year or book.year

            # Clean up author field
            if book.author:
                # Remove common prefixes/suffixes
                book.author = re.sub(
                    r"^by\s+", "", book.author, flags=re.IGNORECASE
                )
                book.author = re.sub(
                    r"\s*\(Author\)$", "", book.author, flags=re.IGNORECASE
                )

                # Handle multiple authors
                if ";" in book.author:
                    book.author = book.author.replace(";", ",")

                # Clean up extra whitespace
                book.author = " ".join(book.author.split())

            # Process the rest
            book.topics = self.determine_topics(book)
            book.difficulty = self.determine_difficulty(book)
            book.rating = self._rate_book(book)

            return book

        except Exception as e:
            logger.error(f"Error processing book {book.title}: {str(e)}")
            return None

    def _book_to_dict(self, book: Book) -> Dict:
        """Convert Book object to dictionary for JSON output."""
        return {
            "title": book.title,
            "author": book.author,
            "year": book.year,
            "page_count": book.page_count,
            "size_bytes": book.size_bytes,
            "topics": book.topics,
            "difficulty": book.difficulty,
            "rating": book.rating,
            "path": str(book.path),
        }

    def _get_years_range(self, books: List[Book]) -> Dict:
        """Get the range of years in the collection."""
        years = [book.year for book in books if book.year]
        if not years:
            return {"min": None, "max": None}
        return {"min": min(years), "max": max(years)}

    def _group_by_topic(self, books: List[Book]) -> Dict:
        """Group books by their topics and subtopics."""
        topic_groups = {}

        for book in books:
            for topic, subtopic in book.topics:
                if topic not in topic_groups:
                    topic_groups[topic] = {}

                if subtopic not in topic_groups[topic]:
                    topic_groups[topic][subtopic] = []

                topic_groups[topic][subtopic].append(
                    {
                        "title": book.title,
                        "author": book.author,
                        "year": book.year,
                        "rating": book.rating,
                        "difficulty": book.difficulty,
                        "path": str(book.path),
                    }
                )

        return topic_groups

    def _rate_book(self, book: Book) -> int:
        """Calculate book rating based on topics and difficulty."""
        base_rating = 5  # Default rating

        # Value multipliers based on topic categories
        topic_value = {
            "must_read": 2.0,
            "highly_valuable": 1.5,
            "career_growth": 1.2,
            "general": 1.0,
        }

        # Extract topics from title and content
        book_topics = self._extract_topics(book)

        # Calculate rating based on topics
        if book_topics:
            # Get highest value multiplier from found topics
            max_multiplier = max(
                topic_value.get(topic, 1.0) for topic in book_topics
            )
            base_rating *= max_multiplier

        # Adjust rating based on difficulty
        difficulty_adjustments = {
            "easy": -0.5,  # Might be too basic
            "moderate": 0,  # No adjustment
            "hard": 0.5,  # More valuable for career growth
            "extreme": 1.0,  # Highly valuable for deep understanding
        }

        base_rating += difficulty_adjustments.get(book.difficulty, 0)

        # Normalize rating to 1-10 scale
        final_rating = min(max(round(base_rating), 1), 10)

        return final_rating

    def _extract_topics(self, book: Book) -> List[str]:
        """Extract high-value topics from book title and content."""
        topics = []

        # High-value technical topics from legacy
        high_value_topics = {
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

        title_lower = book.title.lower()

        # Check each category of topics
        for category, patterns in high_value_topics.items():
            for pattern in patterns:
                if re.search(pattern, title_lower, re.IGNORECASE):
                    topics.append(category)
                    break  # Only add category once

        return list(set(topics))  # Remove duplicates

    def save_analysis(self, analysis: Dict) -> None:
        """Save analysis results to JSON file."""
        self.analysis_dir.mkdir(parents=True, exist_ok=True)
        output_file = (
            self.analysis_dir / Path(self.config["analysis"]["json_file"]).name
        )
        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(analysis, f, indent=2, ensure_ascii=False)

    def analyze_and_save(self, books: List[Book]) -> None:
        """Analyze books and save results to JSON file."""
        analysis = self.analyze_books(books)
        self.save_analysis(analysis)
