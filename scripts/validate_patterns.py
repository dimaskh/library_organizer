#!/usr/bin/env python3
import json
import re
from pathlib import Path
from typing import Set

def get_expected_topics() -> Set[str]:
    """Get the set of expected topics from library analysis."""
    analysis_path = Path(__file__).parent.parent / "analysis" / "library_analysis.json"
    with open(analysis_path, 'r', encoding='utf-8') as f:
        analysis = json.load(f)
    return set(analysis["summary"]["topics"].keys())

def validate_patterns(patterns: dict, expected_topics: Set[str]) -> bool:
    """Validate topic patterns for correctness."""
    is_valid = True
    
    # Validate structure
    for topic, subtopics in patterns.items():
        # Check if topic exists in library analysis
        if topic not in expected_topics:
            print(f"Warning: Topic '{topic}' not found in library analysis")
        
        if not isinstance(topic, str):
            print(f"Error: Topic must be string, got {type(topic)}")
            is_valid = False
        
        if not isinstance(subtopics, dict):
            print(f"Error: Subtopics must be dict, got {type(subtopics)}")
            is_valid = False
            continue
            
        for subtopic, pattern_list in subtopics.items():
            if not isinstance(subtopic, str):
                print(f"Error: Subtopic must be string, got {type(subtopic)}")
                is_valid = False
            
            if not isinstance(pattern_list, list):
                print(f"Error: Pattern list must be list, got {type(pattern_list)}")
                is_valid = False
                continue
            
            # Validate each regex pattern
            for pattern in pattern_list:
                if not isinstance(pattern, str):
                    print(f"Error: Pattern must be string, got {type(pattern)}")
                    is_valid = False
                    continue
                    
                try:
                    re.compile(pattern)
                except re.error as e:
                    print(f"Error: Invalid regex pattern '{pattern}': {e}")
                    is_valid = False
                
                # Check for common regex mistakes
                if pattern.startswith('*'):
                    print(f"Warning: Pattern '{pattern}' starts with *, which might be a mistake")
                if '\\' in pattern and not r'\\' in pattern:
                    print(f"Warning: Pattern '{pattern}' contains single backslash, did you mean double backslash?")
    
    return is_valid

def main():
    patterns_path = Path(__file__).parent.parent / "src" / "core" / "patterns" / "topic_patterns.json"
    
    try:
        with open(patterns_path, 'r', encoding='utf-8') as f:
            patterns = json.load(f)
    except Exception as e:
        print(f"Error loading patterns file: {e}")
        exit(1)
    
    expected_topics = get_expected_topics()
    
    if validate_patterns(patterns, expected_topics):
        print("All patterns are valid!")
    else:
        print("Pattern validation failed!")
        exit(1)

if __name__ == "__main__":
    main() 