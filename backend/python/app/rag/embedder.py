
from sentence_transformers import SentenceTransformer

from app.rag.pipeline import PipelineConfig


class Embedder:
    def __init__(
        self,
        config: PipelineConfig
    ):
        self.config = config

        self.model = SentenceTransformer(
            self.config.embedding_model_path
        )

    def embed_texts(self, texts: list[str]) -> list[list[float]]:
        if not texts:
            return []

        embeddings = self.model.encode(
            texts,
            normalize_embeddings=self.config.normalize_embeddings,
            convert_to_numpy=True,
        )

        return embeddings.tolist()

    def embed_query(self, text: str) -> list[float]:
        vectors = self.embed_texts([text])
        return vectors[0]
