from typing import Dict
from ..models.book import Book
from pathlib import Path
import json
from typing import List


class BookAnalyzer:
    def __init__(self, config: Dict):
        self.config = config
        self.topics = config["topics"]
        self.reading_speeds = config["reading_speeds"]
        self.analysis_dir = Path(config["directories"]["analysis"])
        self.analysis_dir.mkdir(exist_ok=True)

    def calculate_rating(self, book: Book) -> float:
        """Calculate book rating based on various factors"""
        base_rating = 5.0

        # Topic-based rating
        for topic in book.topics:
            if topic in self.config["high_value_topics"]:
                base_rating += 1.0

        # Author reputation
        if book.author in self.config["renowned_authors"]:
            base_rating += 1.5

        # Page count factor
        if book.page_count > 500:
            base_rating += 0.5

        # Year factor
        if book.year and book.year >= 2020:
            base_rating += 0.5

        return min(10.0, max(1.0, base_rating))

    def estimate_difficulty(self, book: Book) -> str:
        """Estimate book difficulty"""
        score = 0

        # Topic-based difficulty
        for topic in book.topics:
            if topic in self.config["advanced_topics"]:
                score += 2
            elif topic in self.config["intermediate_topics"]:
                score += 1

        # Page count factor
        if book.page_count > 600:
            score += 1

        # Map score to difficulty
        if score <= 1:
            return "easy"
        elif score <= 2:
            return "moderate"
        elif score <= 3:
            return "hard"
        return "extreme"

    def analyze_and_save(self, books: List[Book]) -> None:
        """Analyze books and save results"""
        analysis = self._analyze_books(books)
        self._save_analysis(analysis)

    def _analyze_books(self, books: List[Book]) -> Dict:
        """Analyze book collection"""
        return {
            "total_books": len(books),
            "by_topic": self._group_by_topic(books),
            "total_size": sum(book.size_bytes for book in books),
            "total_pages": sum(book.page_count for book in books),
        }

    def _group_by_topic(self, books: List[Book]) -> Dict:
        """Group books by topic"""
        topics = {}
        for book in books:
            for topic in book.topics:
                if topic not in topics:
                    topics[topic] = []
                topics[topic].append(book.title)
        return topics

    def _save_analysis(self, analysis: Dict) -> None:
        """Save analysis to JSON file"""
        output_file = Path(self.config["analysis"]["json_file"])
        with open(output_file, "w") as f:
            json.dump(analysis, f, indent=2)
