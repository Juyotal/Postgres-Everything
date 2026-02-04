from __future__ import annotations

import os

from postgres_everything.embeddings.base import EmbeddingProvider
from postgres_everything.exceptions import EmbeddingProviderError

_DEFAULT_DIMS = {
    "text-embedding-3-small": 1536,
    "text-embedding-3-large": 3072,
    "text-embedding-ada-002": 1536,
}


class OpenAIEmbeddings(EmbeddingProvider):
    """Embedding provider that calls the OpenAI Embeddings API.

    Supports Matryoshka-style dimension truncation (``dimensions`` kwarg) for
    the ``text-embedding-3-*`` model family.

    The ``openai`` package is imported lazily so the library stays importable
    without it installed.

    Args:
        model: OpenAI embedding model name.
        api_key: OpenAI API key.  Defaults to the ``OPENAI_API_KEY`` env var.
        dimensions: Override the output vector size (Matryoshka truncation).
    """

    def __init__(
        self,
        model: str = "text-embedding-3-small",
        api_key: str | None = None,
        dimensions: int | None = None,
    ) -> None:
        try:
            import openai as _openai  # noqa: F401
        except ImportError as exc:
            raise EmbeddingProviderError(
                "openai package is not installed. "
                "Run: pip install 'postgres-everything[openai]'"
            ) from exc

        import openai

        self._model = model
        self._custom_dims = dimensions
        self._client = openai.OpenAI(
            api_key=api_key or os.environ.get("OPENAI_API_KEY")
        )

    @property
    def dimensions(self) -> int:
        """Return the configured output dimensionality."""
        if self._custom_dims is not None:
            return self._custom_dims
        return _DEFAULT_DIMS.get(self._model, 1536)

    def embed(self, text: str) -> list[float]:
        """Embed a single text string via the OpenAI API.

        Args:
            text: Input to embed.

        Returns:
            Embedding vector as a list of floats.

        Raises:
            EmbeddingProviderError: On API failure.
        """
        return self.embed_many([text])[0]

    def embed_many(self, texts: list[str]) -> list[list[float]]:
        """Embed multiple texts in a single API call.

        Args:
            texts: Strings to embed.

        Returns:
            List of embedding vectors in the same order as ``texts``.

        Raises:
            EmbeddingProviderError: On API failure.
        """
        try:
            kwargs: dict = {"model": self._model, "input": texts}
            if self._custom_dims is not None:
                kwargs["dimensions"] = self._custom_dims
            response = self._client.embeddings.create(**kwargs)
            return [item.embedding for item in response.data]
        except Exception as exc:
            raise EmbeddingProviderError(f"OpenAI embeddings request failed: {exc}") from exc
