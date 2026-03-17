"""CRUD tests for MetadataIR."""

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


def _ir_kwargs(span_id):
    return dict(
        span_id=span_id,
        max_T_K=1200.0,
        max_T_image_position_x=10.0,
        max_T_image_position_y=20.0,
        max_T_world_position_x_m=0.1,
        max_T_world_position_y_m=0.2,
        max_T_world_position_z_m=0.3,
        avg_T_K=800.0,
        min_T_K=400.0,
        min_T_image_position_x=5.0,
        min_T_image_position_y=6.0,
        min_T_world_position_x_m=0.05,
        min_T_world_position_y_m=0.06,
        min_T_world_position_z_m=0.07,
    )


class TestMetadataIRCrud:
    def test_create_and_get(self, db):
        span = _make_span(db, "/ir/1")
        meta = crud.metadata.create_metadata_ir(
            db, **_ir_kwargs(span.span_id)
        )
        assert crud.metadata.get_metadata_ir(
            db, meta.metadata_id
        ).max_T_K == pytest.approx(1200.0)

    def test_get_base_metadata(self, db):
        span = _make_span(db, "/ir/base")
        meta = crud.metadata.create_metadata_ir(
            db, **_ir_kwargs(span.span_id)
        )
        assert (
            crud.metadata.get_metadata(db, meta.metadata_id).metadata_id
            == meta.metadata_id
        )

    def test_filter_by_span(self, db):
        span = _make_span(db, "/ir/filter")
        crud.metadata.create_metadata_ir(
            db, **_ir_kwargs(span.span_id)
        )
        assert len(crud.metadata.get_metadata_list(db, span_id=span.span_id)) == 1

    def test_update(self, db):
        span = _make_span(db, "/ir/upd")
        meta = crud.metadata.create_metadata_ir(
            db, **_ir_kwargs(span.span_id)
        )
        updated = crud.metadata.update_metadata_ir(db, meta.metadata_id, max_T_K=1500.0)
        assert updated.max_T_K == pytest.approx(1500.0)

    def test_update_not_found(self, db):
        assert crud.metadata.update_metadata_ir(db, 99999, max_T_K=0.0) is None

    def test_delete(self, db):
        span = _make_span(db, "/ir/del")
        meta = crud.metadata.create_metadata_ir(
            db, **_ir_kwargs(span.span_id)
        )
        assert crud.metadata.delete_metadata(db, meta.metadata_id) is True
        assert crud.metadata.get_metadata(db, meta.metadata_id) is None

    def test_delete_not_found(self, db):
        assert crud.metadata.delete_metadata(db, 99999) is False

    def test_pagination(self, db):
        span = _make_span(db, "/ir/page")
        for _ in range(4):
            crud.metadata.create_metadata_ir(
                db, **_ir_kwargs(span.span_id)
            )
        assert (
            len(
                crud.metadata.get_metadata_ir_list(
                    db, span_id=span.span_id, skip=1, limit=2
                )
            )
            == 2
        )
