class LibraryOrganizerError(Exception):
    """Base exception for library organizer"""

    pass


class ConfigurationError(LibraryOrganizerError):
    """Configuration related errors"""

    pass


class PDFProcessingError(LibraryOrganizerError):
    """PDF processing related errors"""

    pass
