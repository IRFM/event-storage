"""CRUD tests for Event, EventSpan, and push_event."""

import pytest
from conftest import (
    make_event,
    make_full_event_in_memory,
    make_method,
    make_signal,
    make_span,
)

from event_storage import crud


class TestEventCrud:
    def test_create_and_get(self, db):
        event = make_event(db, user_id="u1")
        fetched = crud.event.get_event(db, event.event_id)
        assert fetched.user_id == "u1"

    def test_get_not_found(self, db):
        assert crud.event.get_event(db, 99999) is None

    def test_filter_by_user(self, db):
        make_event(db, user_id="alice")
        make_event(db, user_id="bob")
        assert all(
            e.user_id == "alice" for e in crud.event.get_events(db, user_id="alice")
        )

    def test_filter_by_criticality(self, db):
        cid = crud.reference.get_or_create_criticality(db, "High").criticality_id
        low_cid = crud.reference.get_or_create_criticality(db, "Low").criticality_id
        catid = crud.reference.get_or_create_category(db, "hotspot").category_id
        sid = crud.reference.get_or_create_status(db, "New").status_id
        crud.event.create_event(
            db, user_id="u", criticality_id=cid, category_id=catid, status_id=sid
        )
        crud.event.create_event(
            db, user_id="u", criticality_id=low_cid, category_id=catid, status_id=sid
        )
        results = crud.event.get_events(db, criticality_id=cid)
        assert all(e.criticality_id == cid for e in results)

    def test_update_description(self, db):
        event = make_event(db)
        updated = crud.event.update_event(db, event.event_id, description="new")
        assert updated.description == "new"

    def test_update_not_found(self, db):
        assert crud.event.update_event(db, 99999, description="x") is None

    def test_delete(self, db):
        event = make_event(db)
        assert crud.event.delete_event(db, event.event_id) is True
        assert crud.event.get_event(db, event.event_id) is None

    def test_delete_not_found(self, db):
        assert crud.event.delete_event(db, 99999) is False

    def test_filter_by_method_id(self, db):
        """Events can be filtered by method_id via their spans."""
        method_a = make_method(db, name="MethodA")
        method_b = make_method(db, name="MethodB")

        event_a = make_event(db)
        event_b = make_event(db)

        make_span(db, event_a, path="/method_filter/a", method=method_a)
        make_span(db, event_b, path="/method_filter/b", method=method_b)

        results = crud.event.get_events(db, method_id=method_a.method_id)
        ids = {e.event_id for e in results}
        assert event_a.event_id in ids
        assert event_b.event_id not in ids


class TestPushEvent:
    def test_creates_all_rows(self, db):
        persisted = crud.event.push_event(db, make_full_event_in_memory())
        assert persisted.event_id is not None
        span = persisted.event_spans[0]
        assert span.span_id is not None
        assert span.signal_id is not None
        assert span.annotation.annotation_id is not None

    def test_reuses_existing_references(self, db):
        crud.event.push_event(
            db, make_full_event_in_memory(signal_path="/push/1", experiment_id=1)
        )
        crud.event.push_event(
            db, make_full_event_in_memory(signal_path="/push/2", experiment_id=2)
        )
        crits = crud.reference.get_criticalities(db)
        assert sum(1 for c in crits if c.level == "High") == 1

    def test_denormalized_fields(self, db):
        persisted = crud.event.push_event(
            db, make_full_event_in_memory(experiment_id=42)
        )
        span = persisted.event_spans[0]
        assert span.experiment_id == 42
        assert span.diagnostic == "DIAG"
        assert span.method_name == "Manual"
        assert span.category_id == persisted.category_id


class TestEventSpanCrud:
    def test_create_and_get(self, db):
        event = make_event(db)
        sig = make_signal(db, path="/span/1")
        method = make_method(db)
        span = crud.event.create_event_span(
            db,
            event_id=event.event_id,
            signal_id=sig.signal_id,
            method_id=method.method_id,
            start_time_ns=0,
            end_time_ns=500,
        )
        assert crud.event.get_event_span(db, span.span_id).start_time_ns == 0

    def test_invalid_time_raises(self, db):
        event = make_event(db)
        sig = make_signal(db, path="/span/inv")
        method = make_method(db)
        with pytest.raises(ValueError, match="start_time_ns"):
            crud.event.create_event_span(
                db,
                event_id=event.event_id,
                signal_id=sig.signal_id,
                method_id=method.method_id,
                start_time_ns=1000,
                end_time_ns=0,
            )

    def test_equal_start_end_allowed(self, db):
        event = make_event(db)
        sig = make_signal(db, path="/span/instant")
        method = make_method(db)
        span = crud.event.create_event_span(
            db,
            event_id=event.event_id,
            signal_id=sig.signal_id,
            method_id=method.method_id,
            start_time_ns=100,
            end_time_ns=100,
        )
        assert span.span_id is not None

    def test_overlap_filter(self, db):
        event = make_event(db)
        sig = make_signal(db, path="/span/overlap")
        method = make_method(db)
        crud.event.create_event_span(
            db,
            event_id=event.event_id,
            signal_id=sig.signal_id,
            method_id=method.method_id,
            start_time_ns=0,
            end_time_ns=100,
        )
        crud.event.create_event_span(
            db,
            event_id=event.event_id,
            signal_id=sig.signal_id,
            method_id=method.method_id,
            start_time_ns=200,
            end_time_ns=300,
        )
        assert (
            len(crud.event.get_event_spans(db, start_time_ns=50, end_time_ns=250)) == 2
        )

    def test_update_confidence(self, db):
        event = make_event(db)
        sig = make_signal(db, path="/span/upd")
        method = make_method(db)
        span = crud.event.create_event_span(
            db,
            event_id=event.event_id,
            signal_id=sig.signal_id,
            method_id=method.method_id,
            start_time_ns=0,
            end_time_ns=100,
        )
        assert crud.event.update_event_span(
            db, span.span_id, confidence=0.75
        ).confidence == pytest.approx(0.75)

    def test_update_invalid_time_raises(self, db):
        event = make_event(db)
        sig = make_signal(db, path="/span/inv_upd")
        method = make_method(db)
        span = crud.event.create_event_span(
            db,
            event_id=event.event_id,
            signal_id=sig.signal_id,
            method_id=method.method_id,
            start_time_ns=0,
            end_time_ns=100,
        )
        with pytest.raises(ValueError):
            crud.event.update_event_span(db, span.span_id, start_time_ns=200)

    def test_delete(self, db):
        event = make_event(db)
        sig = make_signal(db, path="/span/del")
        method = make_method(db)
        span = crud.event.create_event_span(
            db,
            event_id=event.event_id,
            signal_id=sig.signal_id,
            method_id=method.method_id,
            start_time_ns=0,
            end_time_ns=50,
        )
        assert crud.event.delete_event_span(db, span.span_id) is True
        assert crud.event.get_event_span(db, span.span_id) is None

    def test_delete_not_found(self, db):
        assert crud.event.delete_event_span(db, 99999) is False
