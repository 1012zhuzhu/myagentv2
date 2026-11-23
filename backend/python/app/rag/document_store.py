
from app.infrastructure.database import get_connection
from app.rag.models import VersionDocument


def get_version_documents(
    db_path: str,
    version_id: int
) -> list[VersionDocument]:
    conn = get_connection(db_path)

    try:
        rows = conn.execute(
            """
            SELECT
                vd.document_id,
                vd.revision_id,
                vd.content_hash,
                vd.pipeline_hash,
                dr.revision_number,
                dr.content
            FROM version_documents AS vd
            JOIN document_revisions AS dr
                ON dr.id = vd.revision_id
            WHERE vd.version_id = ?
            ORDER BY vd.document_id
            """,
            (version_id,)
        ).fetchall()

        return [
            VersionDocument(
                document_id=row["document_id"],
                revision_id=row["revision_id"],
                content_hash=row["content_hash"],
                pipeline_hash=row["pipeline_hash"],
                revision_number=row["revision_number"],
                content=row["content"],
            )
            for row in rows
        ]

    finally:
        conn.close()
