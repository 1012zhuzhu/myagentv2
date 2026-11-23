import hashlib
import json
from dataclasses import asdict, dataclass


@dataclass(frozen=True)
class PipelineConfig:
    embedding_model: str
    embedding_model_path: str
    chunk_size: int = 80
    overlap: int = 20
    normalize_embeddings: bool = True
    schema_version: int = 1

    def __post_init__(self):
        if not self.embedding_model.strip():
            raise ValueError("embedding_model cannot be empty")

        if not self.embedding_model_path.strip():
            raise ValueError("embedding_model_path cannot be empty")

        if self.chunk_size <= 0:
            raise ValueError("chunk_size must be greater than 0")

        if self.overlap < 0 or self.overlap >= self.chunk_size:
            raise ValueError("overlap must be >= 0 and < chunk_size")

    def to_dict(self) -> dict:
        return asdict(self)

    def to_json(self) -> str:
        config = self.to_dict()

        # 本地加载路径不影响 Pipeline 身份
        config.pop("embedding_model_path")

        return json.dumps(
            config,
            sort_keys=True,
            ensure_ascii=False,
            separators=(",", ":"),
        )

    def calculate_hash(self) -> str:
        return hashlib.sha256(
            self.to_json().encode("utf-8")
        ).hexdigest()