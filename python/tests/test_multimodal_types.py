"""Unit tests for multimodal type system (Image, Embedding, Tensor)."""

from __future__ import annotations

import pytest

from starrocks.types import Embedding, Image, Tensor
from starrocks.column import col


class TestImage:
    def test_default_mode(self):
        img = Image()
        assert img.mode == "url"

    def test_bytes_mode(self):
        img = Image(mode="bytes")
        assert img.mode == "bytes"

    def test_repr(self):
        assert "url" in repr(Image())

    def test_frozen(self):
        img = Image()
        with pytest.raises(AttributeError):
            img.mode = "bytes"


class TestEmbedding:
    def test_default_dim(self):
        emb = Embedding()
        assert emb.dim == 0

    def test_with_dim(self):
        emb = Embedding(dim=384)
        assert emb.dim == 384

    def test_repr(self):
        assert "384" in repr(Embedding(dim=384))


class TestTensor:
    def test_default(self):
        t = Tensor()
        assert t.shape is None
        assert t.dtype == "float32"

    def test_with_shape(self):
        t = Tensor(shape=(3, 224, 224), dtype="float16")
        assert t.shape == (3, 224, 224)
        assert t.dtype == "float16"

    def test_repr(self):
        r = repr(Tensor(shape=(10,), dtype="int64"))
        assert "10" in r
        assert "int64" in r


class TestColumnCast:
    def test_cast_image(self):
        c = col("url").cast(Image())
        assert c._multimodal_type == Image()

    def test_cast_embedding(self):
        c = col("vec").cast(Embedding(dim=768))
        assert c._multimodal_type == Embedding(dim=768)

    def test_cast_tensor(self):
        c = col("data").cast(Tensor(shape=(3, 224, 224)))
        assert c._multimodal_type == Tensor(shape=(3, 224, 224))

    def test_cast_preserves_expr(self):
        original = col("url")
        casted = original.cast(Image())
        assert casted.expr == original.expr

    def test_cast_invalid_type(self):
        with pytest.raises(TypeError, match="cast.*expects"):
            col("x").cast("VARCHAR")

    def test_cast_does_not_mutate_original(self):
        c = col("url")
        casted = c.cast(Image())
        assert not hasattr(c, "_multimodal_type")


class TestSchemaDisplay:
    """Test that multimodal type annotations appear in schema."""

    def test_schema_with_multimodal_annotations(self):
        """Verify that types are constructable and usable in a schema context."""
        schema = [
            ("image_url", "VARCHAR", Image()),
            ("embedding", "ARRAY", Embedding(dim=384)),
            ("tensor_data", "ARRAY", Tensor(shape=(10,))),
        ]
        assert len(schema) == 3
        assert schema[0][2].mode == "url"
        assert schema[1][2].dim == 384
        assert schema[2][2].shape == (10,)
