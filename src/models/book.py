from dataclasses import dataclass
from pathlib import Path
from typing import Optional, List


@dataclass
class Book:
    path: Path
    title: str
    author: Optional[str]
    year: Optional[int]
    page_count: int
    size_bytes: int
    topics: List[str]
    difficulty: Optional[str] = None
    rating: Optional[float] = None

    @property
    def filename(self) -> str:
        return self.path.name
