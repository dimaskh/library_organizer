import re
import json
import multiprocessing as mp
import logging
from functools import partial
from pathlib import Path
from typing import Dict, List, Tuple, Set
from tqdm import tqdm
from PyPDF2 import PdfReader
from ..models.book import Book
from collections import defaultdict

logger = logging.getLogger(__name__)


class BookAnalyzer:
    def __init__(self, config: Dict):
        self.config = config
        self.analysis_dir = Path(config["directories"]["analysis"])
        self.analysis_dir.mkdir(exist_ok=True)

        # Load topic patterns first as they're used by other methods
        self.topic_patterns = self._load_topic_patterns()
        
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

    def _clean_filename(self, filename: str) -> str:
        """Clean up filename for better parsing."""
        # Remove common path artifacts and normalize path separators
        filename = str(filename).replace('\\', '/').split('/')[-1]
        
        # Remove file:/// and URL encoded characters
        filename = re.sub(r'^file:///.*?/', '', filename)
        filename = re.sub(r'%20', ' ', filename)
        
        # Remove common file prefixes
        filename = re.sub(r'^(?:book_|ebook_|doc_)', '', filename, flags=re.I)
        
        # Remove common file extensions
        filename = re.sub(r'\.(?:pdf|epub|mobi|dvi|djvu|html?|txt)$', '', filename, flags=re.I)
        
        # Replace underscores and multiple spaces
        filename = filename.replace('_', ' ')
        filename = re.sub(r'\s+', ' ', filename)
        
        return filename.strip()

    def _extract_metadata_from_filename(self, filepath: Path) -> Tuple[str, str, int]:
        """Extract and clean author, title and year from filename pattern."""
        filename = self._clean_filename(str(filepath))
        
        # Common filename patterns (moved flags to pattern)
        patterns = [
            r"(?i)^(?P<author>[\w\s\.,]+?)\s*-\s*(?P<title>.*?)\s*\[(?P<year>\d{4})\]",
            r"(?i)^(?P<title>.*?)\s*-\s*(?P<author>[\w\s\.,]+?)\s*\[(?P<year>\d{4})\]",
            r"(?i)^(?P<author>[\w\s\.,]+?)\s*-\s*(?P<title>.*?)(?:\s*\[(?P<year>\d{4})\])?$",
            r"(?i)^(?P<title>.*?)(?:\s*\[(?P<year>\d{4})\])?$"
        ]
        
        for pattern in patterns:
            match = re.match(pattern, filename)
            if match:
                data = match.groupdict()
                title = self._clean_title(data.get("title", filename))
                return (
                    data.get("author", "").strip(),
                    title,
                    int(data["year"]) if data.get("year") else None
                )
        
        # If no pattern matches, try to extract a sensible title
        return None, self._clean_title(filename), None

    def _clean_title(self, title: str) -> str:
        """Clean and format book title."""
        if not title:
            return ""
            
        # Remove path components and clean up
        title = self._clean_filename(title)
        
        # Remove common prefixes (moved flags to pattern)
        prefixes_to_remove = [
            r'(?i)^(?:a|the|an)\s+',
            r'(?i)^(?:abook|ebook|book|document|manual|guide)[_\s.-]*'
        ]
        
        for prefix in prefixes_to_remove:
            title = re.sub(prefix, '', title)
        
        # Clean up edition/version information
        title = re.sub(r'(?i)\s*[\[(](?:(?:\d+(?:st|nd|rd|th)?\s*)?edition|ver?\.?\s*\d+[\.\d]*|v\d+)[\])]', '', title)
        
        # Capitalize words properly
        words = title.split()
        if not words:
            return ""
        
        # List of words that should not be capitalized
        small_words = {'a', 'an', 'and', 'as', 'at', 'but', 'by', 'for', 'if', 'in', 
                      'of', 'on', 'or', 'the', 'to', 'via', 'with'}
        
        # Capitalize first and last word always, and all other words except small words
        result = []
        for i, word in enumerate(words):
            if i == 0 or i == len(words) - 1 or word.lower() not in small_words:
                result.append(word.capitalize())
            else:
                result.append(word.lower())
        
        title = ' '.join(result)
        
        # Fix common abbreviations
        abbreviations = {
            r'(?i)\bAi\b': 'AI',
            r'(?i)\bMl\b': 'ML',
            r'(?i)\bNlp\b': 'NLP',
            r'(?i)\bApi\b': 'API',
            r'(?i)\bSql\b': 'SQL',
            r'(?i)\bNosql\b': 'NoSQL',
            r'(?i)\bJavascript\b': 'JavaScript',
            r'(?i)\bTypescript\b': 'TypeScript',
            r'(?i)\bPhp\b': 'PHP',
            r'(?i)\bCss\b': 'CSS',
            r'(?i)\bHtml\b': 'HTML'
        }
        
        for pattern, replacement in abbreviations.items():
            title = re.sub(pattern, replacement, title)
        
        return title.strip()

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
        """Determine single most relevant topic and subtopic for a book."""
        topic_scores = defaultdict(lambda: defaultdict(float))
        
        # Use loaded patterns instead of hardcoded ones
        for topic, subtopics in self.topic_patterns.items():
            for subtopic, patterns in subtopics.items():
                if any(re.search(pattern, str(book.path).lower(), re.I) for pattern in patterns):
                    topic_scores[topic][subtopic] += 2.0

        # 2. Analyze title
        title_str = book.title.lower()
        for topic, subtopics in self.topic_patterns.items():
            for subtopic, patterns in subtopics.items():
                if any(re.search(pattern, title_str, re.I) for pattern in patterns):
                    topic_scores[topic][subtopic] += 3.0

        # 3. Deep content analysis
        if hasattr(book, 'content') and book.content:
            # Analyze first few chapters (first 20% of content)
            content_sample = book.content[:int(len(book.content) * 0.2)]
            
            # Extract and analyze TOC
            toc_match = re.search(r'(?:contents|table of contents).*?(?:chapter|section)', 
                                content_sample, re.I | re.S)
            if toc_match:
                toc_text = toc_match.group(0)
                for topic, subtopics in self.topic_patterns.items():
                    for subtopic, patterns in subtopics.items():
                        matches = sum(1 for pattern in patterns 
                                   if re.search(pattern, toc_text, re.I))
                        topic_scores[topic][subtopic] += matches * 0.5

            # Analyze content patterns
            for topic, subtopics in self.topic_patterns.items():
                for subtopic, patterns in subtopics.items():
                    matches = sum(len(re.findall(pattern, content_sample, re.I)) 
                                for pattern in patterns)
                    topic_scores[topic][subtopic] += matches * 0.1

        # 4. Select best topic-subtopic pair
        best_topic = None
        best_subtopic = None
        best_score = 0

        for topic, subtopics in topic_scores.items():
            for subtopic, score in subtopics.items():
                if score > best_score:
                    best_score = score
                    best_topic = topic
                    best_subtopic = subtopic

        # 5. If no good match found (score too low), try additional analysis
        if best_score < 1.0:
            return self._determine_fallback_topic(book)

        return [(best_topic, best_subtopic)]

    def _determine_fallback_topic(self, book: Book) -> List[Tuple[str, str]]:
        """Determine topic when no clear match is found."""
        # Additional specific patterns for edge cases
        specific_patterns = {
            ("Computer Science", "Algorithms"): [
                r"problem solving",
                r"computational thinking",
                r"algorithmic thinking",
                r"competitive programming"
            ],
            ("Computer Science", "Data Structures"): [
                r"data organization",
                r"data management",
                r"memory organization"
            ],
            ("Computer Science", "Computer Architecture"): [
                r"computer organization",
                r"digital design",
                r"computer system"
            ]
            # ... add more specific patterns
        }

        # Try specific patterns
        for (topic, subtopic), patterns in specific_patterns.items():
            if any(re.search(pattern, book.title, re.I) for pattern in patterns):
                return [(topic, subtopic)]

        # If still no match, use directory structure as last resort
        path_parts = str(book.path).lower().split('/')
        for part in path_parts:
            if "algorithm" in part:
                return [("Computer Science", "Algorithms")]
            if "data" in part and "struct" in part:
                return [("Computer Science", "Data Structures")]
            if "arch" in part or "system" in part:
                return [("Computer Science", "Computer Architecture")]
            # ... add more specific mappings

        # Absolute last resort - analyze title words
        title_words = set(re.findall(r'\w+', book.title.lower()))
        if any(word in title_words for word in ["algorithm", "computational"]):
            return [("Computer Science", "Algorithms")]
        if any(word in title_words for word in ["data", "structure"]):
            return [("Computer Science", "Data Structures")]
        
        # If everything fails, return based on directory structure
        return [("Computer Science", "Theory")]  # Better than "General"

    def _extract_toc_keywords(self, book: Book) -> Set[str]:
        """Extract keywords from table of contents."""
        keywords = set()
        
        if hasattr(book, 'content'):
            # Common TOC patterns
            toc_patterns = [
                r"(?:Table of )?Contents?[:|\n]+((?:(?:\d+\.)*\d+\s+[^\n]+\n)+)",
                r"(?:Chapter|Section)\s+\d+[.:]\s*([^\n]+)",
                r"^\d+\.\d*\s+([^\n]+)",  # Numbered sections
            ]
            
            for pattern in toc_patterns:
                matches = re.finditer(pattern, book.content, re.MULTILINE | re.IGNORECASE)
                for match in matches:
                    # Clean and extract meaningful terms
                    terms = re.findall(r'\b\w+(?:\s+\w+){0,3}\b', match.group(1))
                    keywords.update(terms)
        
        return keywords

    def _tokenize_content(self, content: str) -> List[str]:
        """Tokenize and clean content."""
        # Remove code blocks
        content = re.sub(r'```.*?```', '', content, flags=re.DOTALL)
        # Remove punctuation and convert to lowercase
        content = re.sub(r'[^\w\s]', ' ', content.lower())
        # Split into words and remove common words
        words = content.split()
        stop_words = set(['the', 'and', 'or', 'in', 'to', 'a', 'of', 'for', 'is'])
        return [w for w in words if w not in stop_words and len(w) > 2]

    def _calculate_word_frequencies(self, words: List[str]) -> Dict[str, int]:
        """Calculate word frequencies with technical term bias."""
        freq = {}
        for word in words:
            freq[word] = freq.get(word, 0) + 1
        
        # Apply technical term bias
        technical_patterns = [
            r'\w+(?:ing|tion|ment|ity)\b',  # Technical suffixes
            r'[A-Z][a-z]+(?:[A-Z][a-z]+)+',  # CamelCase
            r'\w+(?:\_\w+)+',  # snake_case
            r'\b[A-Z]+\b',     # UPPERCASE terms
        ]
        
        for word in list(freq.keys()):
            for pattern in technical_patterns:
                if re.match(pattern, word):
                    freq[word] *= 1.5  # Boost technical terms
                    break
        
        return freq

    def _identify_technical_terms(self, word_freq: Dict[str, int]) -> Dict[str, float]:
        """Identify technical terms and their importance."""
        technical_terms = {}
        
        # Technical term indicators
        indicators = {
            'algorithm': 2.0,
            'framework': 1.8,
            'protocol': 1.8,
            'database': 1.7,
            'pattern': 1.6,
            'architecture': 1.7,
            'interface': 1.5,
            'implementation': 1.6,
            'optimization': 1.8,
            'security': 1.7,
        }
        
        for word, freq in word_freq.items():
            # Check for compound technical terms
            compounds = self._find_compound_terms(word, word_freq)
            for compound in compounds:
                for indicator, multiplier in indicators.items():
                    if indicator in compound.lower():
                        technical_terms[compound] = freq * multiplier
                        break
        
        return technical_terms

    def _extract_code_blocks(self, content: str) -> List[str]:
        """Extract code blocks from content."""
        code_blocks = []
        
        # Common code block patterns
        patterns = [
            r'```(?:\w+)?\n(.*?)\n```',  # Markdown code blocks
            r'(?s)<code>(.*?)</code>',    # HTML code tags
            r'(?m)^\s{4}.*$',             # Indented code blocks
        ]
        
        for pattern in patterns:
            matches = re.finditer(pattern, content, re.DOTALL)
            code_blocks.extend(match.group(1) for match in matches)
        
        return code_blocks

    def _analyze_code_languages(self, code_blocks: List[str]) -> Dict[str, float]:
        """Analyze programming languages in code blocks."""
        lang_scores = {}
        
        # Language indicators
        lang_patterns = {
            'python': [r'def\s+\w+\s*\(', r'import\s+\w+', r'class\s+\w+:'],
            'java': [r'public\s+class', r'void\s+\w+\s*\(', r'System\.out'],
            'javascript': [r'function\s+\w+\s*\(', r'const\s+\w+\s*=', r'let\s+\w+'],
            'cpp': [r'#include\s*<\w+>', r'std::', r'void\s+\w+\s*\('],
            'go': [r'func\s+\w+\s*\(', r'package\s+main', r'import\s+"'],
            'rust': [r'fn\s+\w+\s*\(', r'let\s+mut', r'use\s+std::'],
        }
        
        for block in code_blocks:
            for lang, patterns in lang_patterns.items():
                score = 0
                for pattern in patterns:
                    if re.search(pattern, block):
                        score += 1
                if score > 0:
                    lang_scores[lang] = lang_scores.get(lang, 0) + score
        
        return lang_scores

    def _analyze_filename(self, book: Book) -> List[Tuple[str, str]]:
        """Analyze filename and path for topic hints."""
        topics = []
        path_parts = Path(book.path).parts
        
        # 1. Directory Structure Analysis
        dir_topics = self._analyze_directory_structure(path_parts)
        if dir_topics:
            topics.extend(dir_topics)
        
        # 2. Filename Analysis
        filename = Path(book.path).stem.lower()
        filename = re.sub(r'\[\d{4}\]', '', filename)
        filename = re.sub(r'[-_\.]', ' ', filename)
        
        terms = re.findall(r'\b\w+(?:\s+\w+){0,3}\b', filename)
        
        # Use topic_patterns instead of self.topics
        for term in terms:
            for topic, subtopics in self.topic_patterns.items():
                for subtopic, patterns in subtopics.items():
                    if any(re.search(pattern, term, re.IGNORECASE) for pattern in patterns):
                        topics.append((topic, subtopic))
        
        return list(set(topics))

    def _analyze_directory_structure(self, path_parts: Tuple[str]) -> List[Tuple[str, str]]:
        """Analyze directory structure for topic hints."""
        topics = []
        path_parts = [p.lower() for p in path_parts]
        
        for part in path_parts:
            part = re.sub(r'[-_\.]', ' ', part)
            
            # Use topic_patterns instead of self.topics
            for topic, subtopics in self.topic_patterns.items():
                for subtopic, patterns in subtopics.items():
                    if any(re.search(pattern, part, re.IGNORECASE) for pattern in patterns):
                        topics.append((topic, subtopic))
        
        return list(set(topics))

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

        # Generate analysis with simplified difficulties
        analysis = {
            "summary": {
                # Basic stats
                "total_books": len(processed_books),
                "average_pages": round(sum(book.page_count for book in processed_books) / len(processed_books)) if processed_books else 0,
                "unique_authors": len({book.author for book in processed_books if book.author}),
                "years_range": {
                    "min": min(book.year for book in processed_books if book.year),
                    "max": max(book.year for book in processed_books if book.year)
                },
                
                # Ratings categories
                "ratings": {
                    "excellent (9-10)": len([b for b in processed_books if b.rating >= 9]),
                    "very_good (7-8)": len([b for b in processed_books if 7 <= b.rating <= 8]),
                    "good (5-6)": len([b for b in processed_books if 5 <= b.rating <= 6]),
                    "average (3-4)": len([b for b in processed_books if 3 <= b.rating <= 4]),
                    "poor (1-2)": len([b for b in processed_books if b.rating <= 2])
                },
                
                # Simplified difficulties
                "difficulties": {
                    "easy": len([b for b in processed_books if b.difficulty == "easy"]),
                    "moderate": len([b for b in processed_books if b.difficulty == "moderate"]),
                    "hard": len([b for b in processed_books if b.difficulty == "hard"]),
                    "extreme": len([b for b in processed_books if b.difficulty == "extreme"])
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
        """Generate topic statistics with book counts."""
        topic_counts = defaultdict(lambda: {"total": 0, "subtopics": defaultdict(int)})
        
        for book in books:
            for topic, subtopic in book.topics:
                topic_counts[topic]["total"] += 1
                topic_counts[topic]["subtopics"][subtopic] += 1
        
        # Convert to regular dict for JSON serialization
        return {
            topic: {
                "total_books": stats["total"],
                "subtopics": dict(stats["subtopics"])
            }
            for topic, stats in topic_counts.items()
        }

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

    def _rate_book(self, book: Book) -> float:
        """Calculate book rating with decimal precision (1.0-10.0)."""
        base_rating = 6.0  # Start at "good" level
        score = 0.0
        
        # Quality indicators with adjusted weights
        indicators = {
            "recency": self._calculate_recency_score(book),
            "comprehensiveness": self._calculate_comprehensiveness_score(book),
            "authority": self._calculate_authority_score(book),
            "technical_depth": self._calculate_technical_depth_score(book),
            "practical_value": self._calculate_practical_value_score(book)
        }
        
        # Adjusted weights (total = 1.0)
        weights = {
            "recency": 0.15,          # Less weight on recency
            "comprehensiveness": 0.25, # More weight on completeness
            "authority": 0.20,         # Publisher/author reputation
            "technical_depth": 0.25,   # More weight on depth
            "practical_value": 0.15    # Practical examples and exercises
        }
        
        # Calculate weighted score
        for indicator, weight in weights.items():
            score += indicators[indicator] * weight
            
        # Adjust base rating by score with reduced range
        final_rating = base_rating + (score * 2.0)  # Scale to +/- 2 points
        
        # Normalize to 1.0-10.0 scale with one decimal point
        return round(max(1.0, min(10.0, final_rating)), 1)

    def _calculate_recency_score(self, book: Book) -> float:
        """Calculate score based on book recency."""
        if not book.year:
            return 0.0
            
        current_year = 2024
        age = current_year - book.year
        
        if age <= 1:
            return 1.0    # Very recent
        elif age <= 3:
            return 0.8    # Recent
        elif age <= 5:
            return 0.6    # Still relevant
        elif age <= 7:
            return 0.4    # Slightly dated
        elif age <= 10:
            return 0.2    # Dated
        else:
            return 0.0    # Old but might have fundamental value

    def _calculate_comprehensiveness_score(self, book: Book) -> float:
        """Calculate score based on book comprehensiveness."""
        score = 0.0
        
        # Page count evaluation with adjusted thresholds
        if book.page_count:
            if book.page_count > 600:
                score += 1.0    # Very comprehensive
            elif book.page_count > 400:
                score += 0.8    # Comprehensive
            elif book.page_count > 300:
                score += 0.6    # Adequate
            elif book.page_count > 200:
                score += 0.4    # Basic coverage
            elif book.page_count > 100:
                score += 0.2    # Brief
            else:
                score += 0.0    # Very brief
        
        return score

    def _calculate_authority_score(self, book: Book) -> float:
        """Calculate score based on book and author authority."""
        score = 0.0
        
        # Updated publisher scores
        reputable_publishers = {
            'oreilly': 0.9,      # Increased
            'addison wesley': 0.9,# Increased
            'manning': 0.8,       # Increased
            'apress': 0.7,
            'packt': 0.6,
            'springer': 0.8,
            'pearson': 0.7,
            'microsoft press': 0.8,
            'wiley': 0.7,
            'mcgraw hill': 0.7,
            'academic press': 0.8,
            'cambridge': 0.9,
            'mit press': 0.9
        }
        
        if hasattr(book, 'publisher'):
            publisher_lower = book.publisher.lower()
            for pub, value in reputable_publishers.items():
                if pub in publisher_lower:
                    score += value
                    break
        
        # Academic indicators remain the same
        if hasattr(book, 'content') and book.content:
            academic_indicators = [
                r'\breference\b',
                r'\btheorem\b',
                r'\bproof\b',
                r'\blemma\b',
                r'\bcitation[s]?\b',
                r'\breferences\b',
                r'\bbibliography\b'
            ]
            matches = sum(1 for pattern in academic_indicators 
                        if re.search(pattern, book.content, re.IGNORECASE))
            score += min(0.5, matches * 0.1)
        
        return min(1.0, score)

    def _calculate_technical_depth_score(self, book: Book) -> float:
        """Calculate score based on technical depth."""
        score = 0.0
        
        # Difficulty-based scoring
        difficulty_scores = {
            "beginner": -0.3,
            "intermediate": 0.0,
            "advanced": 0.4,
            "expert": 0.7
        }
        score += difficulty_scores.get(book.difficulty, 0.0)
        
        # Check for technical indicators in content
        if hasattr(book, 'content') and book.content:
            technical_indicators = [
                (r'\balgorithm\b', 0.2),
                (r'\bcomplexity\b', 0.2),
                (r'\bimplementation\b', 0.1),
                (r'\barchitecture\b', 0.2),
                (r'code\s+example', 0.1),
                (r'\bdesign\s+pattern', 0.2),
                (r'\bperformance\b', 0.1),
                (r'\boptimization\b', 0.2)
            ]
            
            for pattern, value in technical_indicators:
                if re.search(pattern, book.content, re.IGNORECASE):
                    score += value
        
        return min(1.0, score)  # Cap at 1.0

    def _calculate_practical_value_score(self, book: Book) -> float:
        """Calculate score based on practical value."""
        score = 0.0
        
        if hasattr(book, 'content') and book.content:
            practical_indicators = [
                (r'(?:^|\n)(?:def|class|function)', 0.2),  # Code blocks
                (r'(?:^|\n)(?:var|let|const)', 0.2),  # Variables
                (r'example[s]?\s+\d+', 0.1),  # Numbered examples
                (r'exercise[s]?\s+\d+', 0.2),  # Exercises
                (r'practice[s]?\b', 0.1),
                (r'tutorial[s]?\b', 0.1),
                (r'workshop[s]?\b', 0.1),
                (r'hands[-\s]on', 0.2)
            ]
            
            for pattern, value in practical_indicators:
                if re.search(pattern, book.content, re.IGNORECASE):
                    score += value
        
        return min(1.0, score)  # Cap at 1.0

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

    def _load_topic_patterns(self) -> dict:
        """Load topic patterns from JSON file."""
        patterns_path = Path(__file__).parent / "patterns" / "topic_patterns.json"
        try:
            with open(patterns_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"Failed to load topic patterns: {e}")
            return {}
