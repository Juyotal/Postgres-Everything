from __future__ import annotations


class PostgresEverythingError(Exception):
    """Base error for all postgres_everything exceptions."""


class ConfigurationError(PostgresEverythingError):
    """Raised when a module is misconfigured or missing a required dependency."""


class MigrationError(PostgresEverythingError):
    """Raised when a schema migration fails to apply."""


class TaskHandlerError(PostgresEverythingError):
    """Raised when a task handler is not registered or raises during execution."""


class EmbeddingProviderError(PostgresEverythingError):
    """Raised when an embedding provider fails to generate embeddings."""


class DocumentNotFoundError(PostgresEverythingError):
    """Raised when a requested document does not exist."""
