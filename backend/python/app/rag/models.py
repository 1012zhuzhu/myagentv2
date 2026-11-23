from dataclasses import dataclass


@dataclass(frozen=True)
class VersionDocument:
    document_id: int
    revision_id: int
    content_hash: str
    pipeline_hash: str
    revision_number: str
    content: str

@dataclass
class VectorRecord:
    id: str
    version_id: int
    document_id: int
    revision_id: int
    chunk_index: int
    content_hash: str
    pipeline_hash: str
    text: str
    vector: list[float]

@dataclass
class IndexBuildResult:
    version_id: int
    document_count: int
    chunk_count: int
    vector_count: int

def build_vector_id(
    version_id: int,
    document_id: int,
    revision_id: int,
    chunk_index: int,
) -> str:
    if min(version_id, document_id, revision_id,chunk_index) < 0:
        raise ValueError("vector ID cannot be negative")

    return(
        f"v{version_id}_"
        f"d{document_id}_"
        f"r{revision_id}_"
        f"c{chunk_index}_"
    )