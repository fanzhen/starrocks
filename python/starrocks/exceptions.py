"""Custom exceptions for the StarRocks DataFrame API."""


class StarRocksError(Exception):
    """Base exception for all StarRocks DataFrame errors."""
    pass


class ConnectionError(StarRocksError):
    """Raised when a connection to StarRocks cannot be established."""
    pass


class QueryError(StarRocksError):
    """Raised when a SQL query fails on StarRocks.

    Attributes:
        sql: The SQL statement that failed.
        original: The original database exception.
    """

    def __init__(self, message: str, sql: str | None = None, original: Exception | None = None) -> None:
        self.sql = sql
        self.original = original
        if sql:
            message = f"{message}\n\nGenerated SQL:\n{sql}"
        if original:
            message = f"{message}\n\nOriginal error: {original}"
        super().__init__(message)


class CompilationError(StarRocksError):
    """Raised when the logical plan cannot be compiled to SQL."""
    pass
