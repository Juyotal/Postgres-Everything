from __future__ import annotations

from postgres_everything.cache import Cache
from postgres_everything.client import PostgresEverything
from postgres_everything.documents import DocumentStore
from postgres_everything.embeddings.base import EmbeddingProvider
from postgres_everything.exceptions import (
    ConfigurationError,
    DocumentNotFoundError,
    EmbeddingProviderError,
    MigrationError,
    PostgresEverythingError,
    TaskHandlerError,
)
from postgres_everything.pubsub import PubSub
from postgres_everything.queue import TaskQueue
from postgres_everything.search import SearchEngine
from postgres_everything.vectors import VectorStore

__version__ = "0.1.0"

__all__ = [
    "PostgresEverything",
    "DocumentStore",
    "TaskQueue",
    "SearchEngine",
    "VectorStore",
    "Cache",
    "PubSub",
    "EmbeddingProvider",
    "PostgresEverythingError",
    "ConfigurationError",
    "MigrationError",
    "TaskHandlerError",
    "EmbeddingProviderError",
    "DocumentNotFoundError",
]
