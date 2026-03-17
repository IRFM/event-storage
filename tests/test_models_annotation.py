"""Tests for Annotation model factory classmethods and to_dict serialization."""

import numpy as np
import pytest

from event_storage.models.annotation import Annotation2D, Annotation3D


class TestAnnotation2DFromBbox:
    def test_coordinates(self):
        ann = Annotation2D.from_bbox(x=10, y=20, w=30, h=40)
        assert ann.bbox_x1 == pytest.approx(10.0)
        assert ann.bbox_y1 == pytest.approx(20.0)
        assert ann.bbox_x2 == pytest.approx(40.0)
        assert ann.bbox_y2 == pytest.approx(60.0)

    def test_dimensions(self):
        ann = Annotation2D.from_bbox(x=0, y=0, w=5, h=8)
        assert ann.width == pytest.approx(5.0)
        assert ann.height == pytest.approx(8.0)

    def test_area(self):
        ann = Annotation2D.from_bbox(x=0, y=0, w=4, h=6)
        assert ann.area == pytest.approx(24.0)

    def test_centroid(self):
        ann = Annotation2D.from_bbox(x=0, y=0, w=10, h=20)
        assert ann.centroid_x == pytest.approx(5.0)
        assert ann.centroid_y == pytest.approx(10.0)

    def test_annotation_subtype(self):
        ann = Annotation2D.from_bbox(x=0, y=0, w=1, h=1)
        assert ann.annotation_subtype == "2d"

    def test_float_coercion(self):
        ann = Annotation2D.from_bbox(x=1, y=2, w=3, h=4)
        assert isinstance(ann.bbox_x1, float)


class TestAnnotation2DFromPolygon:
    def _square(self):
        return np.array([[0, 0], [4, 0], [4, 4], [0, 4]], dtype=float)

    def test_bounding_box(self):
        ann = Annotation2D.from_polygon(self._square())
        assert ann.bbox_x1 == pytest.approx(0.0)
        assert ann.bbox_y1 == pytest.approx(0.0)
        assert ann.bbox_x2 == pytest.approx(4.0)
        assert ann.bbox_y2 == pytest.approx(4.0)

    def test_area_shoelace(self):
        ann = Annotation2D.from_polygon(self._square())
        assert ann.area == pytest.approx(16.0)

    def test_centroid(self):
        ann = Annotation2D.from_polygon(self._square())
        assert ann.centroid_x == pytest.approx(2.0)
        assert ann.centroid_y == pytest.approx(2.0)

    def test_polygon_roundtrip(self):
        poly = self._square()
        ann = Annotation2D.from_polygon(poly)
        recovered = ann.get_polygon()
        assert recovered.shape == (4, 2)
        np.testing.assert_allclose(recovered, poly.astype(np.float32), atol=1e-5)

    def test_wrong_shape_raises(self):
        with pytest.raises(ValueError, match="Nx2"):
            Annotation2D.from_polygon(np.zeros((5, 3)))

    def test_triangle_area(self):
        # Right triangle with legs 3 and 4 — area = 6
        tri = np.array([[0, 0], [3, 0], [0, 4]], dtype=float)
        ann = Annotation2D.from_polygon(tri)
        assert ann.area == pytest.approx(6.0)


class TestAnnotation2DFromMask:
    def _rect_mask(self, row_start=2, row_end=5, col_start=3, col_end=7):
        mask = np.zeros((10, 10), dtype=np.uint8)
        mask[row_start:row_end, col_start:col_end] = 1
        return mask

    def test_bounding_box(self):
        ann = Annotation2D.from_mask(self._rect_mask())
        assert ann.bbox_x1 == pytest.approx(3.0)
        assert ann.bbox_y1 == pytest.approx(2.0)
        assert ann.bbox_x2 == pytest.approx(6.0)
        assert ann.bbox_y2 == pytest.approx(4.0)

    def test_area_equals_pixel_count(self):
        ann = Annotation2D.from_mask(self._rect_mask(0, 3, 0, 4))
        assert ann.area == pytest.approx(12.0)

    def test_centroid(self):
        mask = np.zeros((10, 10), dtype=np.uint8)
        mask[4:6, 4:6] = 1  # 2×2 block centred on (4.5, 4.5)
        ann = Annotation2D.from_mask(mask)
        assert ann.centroid_x == pytest.approx(4.5)
        assert ann.centroid_y == pytest.approx(4.5)

    def test_mask_roundtrip(self):
        mask = self._rect_mask()
        ann = Annotation2D.from_mask(mask)
        recovered = ann.get_mask(height=10, width=10)
        np.testing.assert_array_equal(recovered, mask)

    def test_empty_mask_raises(self):
        with pytest.raises(ValueError, match="empty"):
            Annotation2D.from_mask(np.zeros((5, 5), dtype=np.uint8))

    def test_non_2d_raises(self):
        with pytest.raises(ValueError, match="2D"):
            Annotation2D.from_mask(np.ones((3, 3, 3), dtype=np.uint8))

    def test_boolean_mask_accepted(self):
        mask = np.zeros((5, 5), dtype=bool)
        mask[1:3, 1:3] = True
        ann = Annotation2D.from_mask(mask)
        assert ann.area == pytest.approx(4.0)


class TestAnnotation2DGetters:
    def test_get_polygon_none_when_no_data(self):
        ann = Annotation2D(bbox_x1=0, bbox_y1=0, bbox_x2=1, bbox_y2=1)
        assert ann.get_polygon() is None

    def test_get_mask_none_when_no_data(self):
        ann = Annotation2D(bbox_x1=0, bbox_y1=0, bbox_x2=1, bbox_y2=1)
        assert ann.get_mask(10, 10) is None


class TestAnnotation2DToDict:
    def test_basic_fields(self):
        ann = Annotation2D(
            annotation_id=1,
            bbox_x1=1.0,
            bbox_y1=2.0,
            bbox_x2=3.0,
            bbox_y2=4.0,
            width=2.0,
            height=2.0,
            area=4.0,
            centroid_x=2.0,
            centroid_y=3.0,
        )
        d = ann.to_dict()
        assert d["bbox_x1"] == pytest.approx(1.0)
        assert d["area"] == pytest.approx(4.0)
        assert "polygon" not in d

    def test_polygon_included_when_present(self):
        poly = np.array([[0, 0], [2, 0], [2, 2], [0, 2]], dtype=float)
        ann = Annotation2D.from_polygon(poly)
        d = ann.to_dict()
        assert "polygon" in d
        assert len(d["polygon"]) == 4

    def test_annotation_type_name_when_loaded(self):
        from event_storage.models.reference import AnnotationType

        ann = Annotation2D(
            annotation_id=1,
            bbox_x1=0,
            bbox_y1=0,
            bbox_x2=1,
            bbox_y2=1,
            annotation_type=AnnotationType(name="2D"),
        )
        d = ann.to_dict()
        assert d["type"] == "2D"


class TestAnnotation3DFromBbox:
    def test_coordinates(self):
        ann = Annotation3D.from_bbox(x=1, y=2, z=3, w=4, h=5, d=6)
        assert ann.bbox_x1 == pytest.approx(1.0)
        assert ann.bbox_z2 == pytest.approx(9.0)

    def test_dimensions(self):
        ann = Annotation3D.from_bbox(x=0, y=0, z=0, w=2, h=3, d=4)
        assert ann.width == pytest.approx(2.0)
        assert ann.height == pytest.approx(3.0)
        assert ann.depth == pytest.approx(4.0)

    def test_volume(self):
        ann = Annotation3D.from_bbox(x=0, y=0, z=0, w=2, h=3, d=4)
        assert ann.volume == pytest.approx(24.0)

    def test_area(self):
        ann = Annotation3D.from_bbox(x=0, y=0, z=0, w=2, h=3, d=4)
        assert ann.area == pytest.approx(6.0)

    def test_centroid(self):
        ann = Annotation3D.from_bbox(x=0, y=0, z=0, w=4, h=6, d=8)
        assert ann.centroid_x == pytest.approx(2.0)
        assert ann.centroid_y == pytest.approx(3.0)
        assert ann.centroid_z == pytest.approx(4.0)

    def test_annotation_subtype(self):
        ann = Annotation3D.from_bbox(x=0, y=0, z=0, w=1, h=1, d=1)
        assert ann.annotation_subtype == "3d"


class TestAnnotation3DToDict:
    def test_all_fields_present(self):
        ann = Annotation3D.from_bbox(x=0, y=0, z=0, w=2, h=3, d=4)
        d = ann.to_dict()
        for key in (
            "bbox_x1",
            "bbox_y1",
            "bbox_z1",
            "bbox_x2",
            "bbox_y2",
            "bbox_z2",
            "width",
            "height",
            "depth",
            "area",
            "volume",
            "centroid_x",
            "centroid_y",
            "centroid_z",
        ):
            assert key in d

    def test_volume_value(self):
        ann = Annotation3D.from_bbox(x=0, y=0, z=0, w=2, h=3, d=4)
        assert ann.to_dict()["volume"] == pytest.approx(24.0)
