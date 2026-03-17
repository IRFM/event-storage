"""CRUD tests for Annotation2D and Annotation3D."""

import pytest
from conftest import make_event, make_method, make_signal

from event_storage import crud


def _make_span(db, path):
    event = make_event(db)
    sig = make_signal(db, path=path)
    method = make_method(db)
    return crud.event.create_event_span(
        db,
        event_id=event.event_id,
        signal_id=sig.signal_id,
        method_id=method.method_id,
        start_time_ns=0,
        end_time_ns=100,
    )


class TestAnnotation2DCrud:
    def test_create_and_get(self, db):
        span = _make_span(db, "/ann2d/1")
        ann = crud.annotation.create_annotation_2d(
            db,
            span_id=span.span_id,
            bbox_x1=1.0,
            bbox_y1=2.0,
            bbox_x2=3.0,
            bbox_y2=4.0,
        )
        assert crud.annotation.get_annotation_2d(
            db, ann.annotation_id
        ).bbox_x1 == pytest.approx(1.0)

    def test_get_base_annotation(self, db):
        span = _make_span(db, "/ann2d/base")
        ann = crud.annotation.create_annotation_2d(
            db,
            span_id=span.span_id,
            bbox_x1=0.0,
            bbox_y1=0.0,
            bbox_x2=1.0,
            bbox_y2=1.0,
        )
        base = crud.annotation.get_annotation(db, ann.annotation_id)
        assert base.annotation_id == ann.annotation_id

    def test_filter_by_span(self, db):
        span = _make_span(db, "/ann2d/filter")
        crud.annotation.create_annotation_2d(
            db,
            span_id=span.span_id,
            bbox_x1=0.0,
            bbox_y1=0.0,
            bbox_x2=1.0,
            bbox_y2=1.0,
        )
        assert len(crud.annotation.get_annotations(db, span_id=span.span_id)) == 1

    def test_update(self, db):
        span = _make_span(db, "/ann2d/upd")
        ann = crud.annotation.create_annotation_2d(
            db,
            span_id=span.span_id,
            bbox_x1=0.0,
            bbox_y1=0.0,
            bbox_x2=1.0,
            bbox_y2=1.0,
        )
        assert crud.annotation.update_annotation_2d(
            db, ann.annotation_id, bbox_x2=5.0
        ).bbox_x2 == pytest.approx(5.0)

    def test_update_not_found(self, db):
        assert crud.annotation.update_annotation_2d(db, 99999, bbox_x1=0.0) is None

    def test_delete(self, db):
        span = _make_span(db, "/ann2d/del")
        ann = crud.annotation.create_annotation_2d(
            db,
            span_id=span.span_id,
            bbox_x1=0.0,
            bbox_y1=0.0,
            bbox_x2=1.0,
            bbox_y2=1.0,
        )
        assert crud.annotation.delete_annotation(db, ann.annotation_id) is True
        assert crud.annotation.get_annotation(db, ann.annotation_id) is None

    def test_delete_not_found(self, db):
        assert crud.annotation.delete_annotation(db, 99999) is False

    def test_pagination(self, db):
        span = _make_span(db, "/ann2d/page")
        for i in range(5):
            crud.annotation.create_annotation_2d(
                db,
                span_id=span.span_id,
                bbox_x1=float(i),
                bbox_y1=0.0,
                bbox_x2=float(i + 1),
                bbox_y2=1.0,
            )
        assert (
            len(
                crud.annotation.get_annotations_2d(
                    db, span_id=span.span_id, skip=2, limit=2
                )
            )
            == 2
        )


class TestAnnotation3DCrud:
    def test_create_and_get(self, db):
        span = _make_span(db, "/ann3d/1")
        ann = crud.annotation.create_annotation_3d(
            db,
            span_id=span.span_id,
            bbox_x1=0.0,
            bbox_y1=0.0,
            bbox_z1=0.0,
            bbox_x2=1.0,
            bbox_y2=1.0,
            bbox_z2=1.0,
            volume=1.0,
            area=1.0,
        )
        assert crud.annotation.get_annotation_3d(
            db, ann.annotation_id
        ).bbox_z2 == pytest.approx(1.0)

    def test_update(self, db):
        span = _make_span(db, "/ann3d/upd")
        ann = crud.annotation.create_annotation_3d(
            db,
            span_id=span.span_id,
            bbox_x1=0.0,
            bbox_y1=0.0,
            bbox_z1=0.0,
            bbox_x2=1.0,
            bbox_y2=1.0,
            bbox_z2=1.0,
            volume=1.0,
            area=1.0,
        )
        assert crud.annotation.update_annotation_3d(
            db, ann.annotation_id, volume=8.0
        ).volume == pytest.approx(8.0)

    def test_update_not_found(self, db):
        assert crud.annotation.update_annotation_3d(db, 99999, volume=1.0) is None
