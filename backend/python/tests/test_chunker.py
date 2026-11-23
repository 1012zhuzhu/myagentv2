
from app.rag.chunker import split_text



def test_split_text():
    text = "A" * 200

    chunks = split_text(
        text,
        chunk_size=80,
        overlap=20,
    )

    assert len(chunks) == 4

    assert len(chunks[0].content) == 80
    assert len(chunks[1].content) == 80
    assert len(chunks[2].content) == 80
    assert len(chunks[3].content) == 20

    # 验证相邻 Chunk 的重叠部分
    assert chunks[0].content[60:] == chunks[1].content[:20]
    assert chunks[1].content[60:] == chunks[2].content[:20]
    assert chunks[2].content[60:] == chunks[3].content[:20]

    # 验证 Chunk 索引连续
    assert [chunk.index for chunk in chunks] == [0, 1, 2, 3]

    print("Chunk 测试通过")


def test_empty_text():
    assert split_text("") == []
    print("空文本测试通过")


def test_invalid_config():
    try:
        split_text("hello", chunk_size=80, overlap=80)
    except ValueError:
        print("参数校验测试通过")
    else:
        raise AssertionError("应该抛出 ValueError")


if __name__ == "__main__":
    test_split_text()
    test_empty_text()
    test_invalid_config()
