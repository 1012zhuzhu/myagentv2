from dataclasses import dataclass


@dataclass
class TextChunk:
    index: int
    content: str

def split_text(
        text: str,
        chunk_size: int = 80,
        overlap: int = 20,
    ) -> list[TextChunk]: 
    if chunk_size <= 0:
        raise ValueError("chunk_size must be greater than 0")

    if overlap < 0 or overlap >= chunk_size:
        raise ValueError("overlap must be >= 0 and < chunk_size")

    text = text.strip()

    if not text:
        return []

    chunks = []
    start = 0
    index = 0
    step = chunk_size - overlap

    while start < len(text):
        end = min(start + chunk_size, len(text))
        content = text[start:end].strip()

        if content:
            chunks.append(
                TextChunk(
                    index,
                    content,
                )
            )
            index += 1

        start += step

    return chunks