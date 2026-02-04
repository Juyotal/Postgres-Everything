from __future__ import annotations

from abc import ABC, abstractmethod


class EmbeddingProvider(ABC):
    """Abstract base class for embedding providers.

    Concrete implementations wrap a model (local or API-based) and expose a
    uniform interface so the rest of the library stays provider-agnostic.
    """

    @property
    @abstractmethod
    def dimensions(self) -> int:
        """Return the dimensionality of the embedding vectors produced."""

    @abstractmethod
    def embed(self, text: str) -> list[float]:
        """Embed a single piece of text.

        Args:
            text: Input string to embed.

        Returns:
            A list of floats of length ``self.dimensions``.
        """

    @abstractmethod
    def embed_many(self, texts: list[str]) -> list[list[float]]:
        """Embed multiple texts in a single call (batched for efficiency).

        Args:
            texts: List of strings to embed.

        Returns:
            A list of float vectors, one per input text, in the same order.
        """
