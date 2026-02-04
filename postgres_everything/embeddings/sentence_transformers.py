from __future__ import annotations

from postgres_everything.embeddings.base import EmbeddingProvider
from postgres_everything.exceptions import EmbeddingProviderError


class SentenceTransformerEmbeddings(EmbeddingProvider):
    """Embedding provider that runs a model locally via sentence-transformers.

    Fully offline — no API key or network calls required.  The default model
    (``all-MiniLM-L6-v2``) produces 384-dimensional vectors and is fast
    enough for development and moderate-scale production.

    The ``sentence_transformers`` package is imported lazily so the library
    stays importable without it installed.

    Args:
        model: HuggingFace model name or local path accepted by
            ``SentenceTransformer``.
    """

    def __init__(self, model: str = "all-MiniLM-L6-v2") -> None:
        try:
            import sentence_transformers as _st  # noqa: F401
        except ImportError as exc:
            raise EmbeddingProviderError(
                "sentence-transformers package is not installed. "
                "Run: pip install 'postgres-everything[local]'"
            ) from exc

        from sentence_transformers import SentenceTransformer

        self._model_name = model
        self._model = SentenceTransformer(model)

    @property
    def dimensions(self) -> int:
        """Return the embedding dimensionality of the loaded model."""
        return int(self._model.get_sentence_embedding_dimension())

    def embed(self, text: str) -> list[float]:
        """Embed a single text locally.

        Args:
            text: Input string.

        Returns:
            Embedding vector as a list of floats.

        Raises:
            EmbeddingProviderError: If encoding fails.
        """
        return self.embed_many([text])[0]

    def embed_many(self, texts: list[str]) -> list[list[float]]:
        """Embed multiple texts in a single forward pass.

        Args:
            texts: Strings to embed.

        Returns:
            List of embedding vectors.

        Raises:
            EmbeddingProviderError: If encoding fails.
        """
        try:
            vectors = self._model.encode(texts, convert_to_numpy=True)
            return [v.tolist() for v in vectors]
        except Exception as exc:
            raise EmbeddingProviderError(
                f"sentence-transformers encoding failed: {exc}"
            ) from exc
