class DomainError(Exception):
    """Base class for exceptions in this module."""
    pass

class ArticleNotFoundError(DomainError):
    """Exception raised when an article is not found."""
    def __init__(self, article_id):
        self.article_id = article_id
        self.message = f"Article with ID {article_id} not found."
        super().__init__(self.message)
        
class DumpFileError(DomainError):
    """Exception raised for errors related to dump files."""
    def __init__(self, file_name):
        self.file_name = file_name
        self.message = f"Error processing dump file: {file_name}."
        super().__init__(self.message)

class DatabaseError(DomainError):
    """Exception raised for database-related errors."""
    def __init__(self, details):
        self.details = details
        self.message = f"Database error: {details}."
        super().__init__(self.message)