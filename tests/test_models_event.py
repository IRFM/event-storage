"""Tests for Event and EventSpan model methods and DB-level constraints."""

import json

import pytest
from conftest import make_event, make_full_event_in_memory, make_method, make_signal
from sqlalchemy.exc import IntegrityError

from event_storage import crud
from event_storage.models.reference import Method, Signal


class TestEventToDict:
    def test_basic_fields_present(self, full_event):
        d = full_event.to_dict(include_spans=False)
        assert d["event_id"] == full_event.event_id
        assert d["user_id"] == "user1"
        assert d["criticality"]["level"] == "High"
        assert d["category"]["name"] == "hotspot"
        assert d["status"]["name"] == "New"
        assert "spans" not in d

    def test_include_spans_true(self, full_event):
        d = full_event.to_dict(include_spans=True)
        assert "spans" in d
        assert d["span_count"] == 1

    def test_span_timestamps(self, full_event):
        d = full_event.to_dict()
        assert d["initial_timestamp_ns"] == 0
        assert d["final_timestamp_ns"] == 1000
        assert d["total_duration_ns"] == 1000

    def test_multi_span_timestamps(self):
        e = make_full_event_in_memory(
            signal_path="/multi/1", experiment_id=101, start_ns=100, end_ns=200
        )
        # Add a second span
        from event_storage.models.event import EventSpan

        second_signal = Signal(
            machine="WEST",
            experiment_id=101,
            diagnostic="D2",
            type="IR",
            path="/multi/2",
        )
        second_method = Method(name="Manual", is_manual=True, configuration=None)
        second_span = EventSpan(
            start_time_ns=500,
            end_time_ns=900,
            signal=second_signal,
            method=second_method,
            span_metadata=[],
        )
        e.event_spans.append(second_span)
        d = e.to_dict()
        assert d["initial_timestamp_ns"] == 100
        assert d["final_timestamp_ns"] == 900
        assert d["total_duration_ns"] == 800
        assert d["span_count"] == 2

    def test_created_at_isoformat(self, full_event):
        d = full_event.to_dict(include_spans=False)
        # Should be parseable as ISO 8601
        from datetime import datetime

        datetime.fromisoformat(d["created_at"])

    def test_no_spans_omits_span_fields(self, db):
        event = make_event(db)
        d = event.to_dict(include_spans=True)
        # include_spans=True but no spans exist — span fields must be absent
        assert "spans" not in d
        assert "span_count" not in d


class TestEventToJson:
    def test_to_json_roundtrip(self, full_event, tmp_path):
        path = str(tmp_path / "event.json")
        full_event.to_json(path)
        with open(path) as f:
            data = json.load(f)
        assert data["event_id"] == full_event.event_id
        assert "spans" in data

    def test_to_json_exclude_spans(self, full_event, tmp_path):
        path = str(tmp_path / "event_no_spans.json")
        full_event.to_json(path, include_spans=False)
        with open(path) as f:
            data = json.load(f)
        assert "spans" not in data

    def test_write_events_to_json(self, db, tmp_path):
        from event_storage.models.event import Event

        e1 = make_full_event_in_memory(signal_path="/json/1", experiment_id=201)
        e2 = make_full_event_in_memory(signal_path="/json/2", experiment_id=202)
        p1 = crud.event.push_event(db, e1)
        p2 = crud.event.push_event(db, e2)
        path = str(tmp_path / "events.json")
        Event.write_events_to_json(path, [p1, p2])
        with open(path) as f:
            data = json.load(f)
        assert len(data) == 2
        assert data["0"]["event_id"] == p1.event_id
        assert data["1"]["event_id"] == p2.event_id


class TestEventSpanToDict:
    def test_all_fields_present(self, full_event):
        span = full_event.event_spans[0]
        d = span.to_dict()
        assert d["span_id"] == span.span_id
        assert d["start_time_ns"] == 0
        assert d["end_time_ns"] == 1000
        assert d["duration_ns"] == 1000
        assert d["confidence"] == pytest.approx(0.9)
        assert d["experiment_id"] == 42
        assert d["diagnostic"] == "DIAG"
        assert d["method_name"] == "Manual"

    def test_signal_nested(self, full_event):
        d = full_event.event_spans[0].to_dict()
        assert d["signal"]["machine"] == "WEST"

    def test_annotation_nested(self, full_event):
        d = full_event.event_spans[0].to_dict()
        assert d["annotation"] is not None
        assert d["annotation"]["bbox_x1"] == pytest.approx(0.0)

    def test_signal_fallback_when_not_loaded(self, db):
        """When signal relationship is None but signal_id is set, falls back to dict with id."""
        from event_storage.models.event import EventSpan

        span = EventSpan(
            span_id=None,
            signal_id=99,
            signal=None,
            method_id=1,
            method=None,
            start_time_ns=0,
            end_time_ns=100,
            span_metadata=[],
            annotation=None,
        )
        d = span.to_dict()
        assert d["signal"] == {"signal_id": 99}

    def test_signal_none_when_no_id(self):
        from event_storage.models.event import EventSpan

        span = EventSpan(
            signal_id=None,
            signal=None,
            method_id=None,
            method=None,
            start_time_ns=0,
            end_time_ns=100,
            span_metadata=[],
            annotation=None,
        )
        d = span.to_dict()
        assert d["signal"] is None

    def test_method_fallback_when_not_loaded(self):
        from event_storage.models.event import EventSpan

        span = EventSpan(
            signal_id=None,
            signal=None,
            method_id=7,
            method=None,
            start_time_ns=0,
            end_time_ns=100,
            span_metadata=[],
            annotation=None,
        )
        d = span.to_dict()
        assert d["method"] == {"method_id": 7}

    def test_metadata_list_in_dict(self):
        """span_metadata entries appear in to_dict output."""
        from event_storage.models.metadata import MetadataIR

        event = make_full_event_in_memory(signal_path="/meta_dict/1", experiment_id=301)
        _ = MetadataIR(
            metadata_subtype="ir",
            span=event.event_spans[0],
            max_T_K=1000.0,
            avg_T_K=700.0,
            min_T_K=400.0,
            max_T_image_position_x=1.0,
            max_T_image_position_y=2.0,
            max_T_world_position_x_m=0.1,
            max_T_world_position_y_m=0.2,
            max_T_world_position_z_m=0.3,
            min_T_image_position_x=0.5,
            min_T_image_position_y=0.6,
            min_T_world_position_x_m=0.05,
            min_T_world_position_y_m=0.06,
            min_T_world_position_z_m=0.07,
        )
        d = event.event_spans[0].to_dict()
        assert len(d["metadata"]) == 1

    def test_db_rejects_inverted_times(self, db):
        event = make_event(db)
        sig = make_signal(db, path="/constraint_sig")
        method = make_method(db)
        from event_storage.models.event import EventSpan

        span = EventSpan(
            event_id=event.event_id,
            signal_id=sig.signal_id,
            method_id=method.method_id,
            start_time_ns=1000,
            end_time_ns=0,
        )
        db.add(span)
        with pytest.raises(IntegrityError):
            db.commit()


class TestEventSpanCascadeDelete:
    def test_deleting_event_deletes_spans(self, db):
        event = make_full_event_in_memory(signal_path="/cascade/1", experiment_id=401)
        persisted = crud.event.push_event(db, event)
        span_id = persisted.event_spans[0].span_id
        crud.event.delete_event(db, persisted.event_id)
        from event_storage.models.event import EventSpan

        assert db.get(EventSpan, span_id) is None

    def test_deleting_span_deletes_annotation(self, db):
        event = make_full_event_in_memory(signal_path="/cascade/2", experiment_id=402)
        persisted = crud.event.push_event(db, event)
        span = persisted.event_spans[0]
        ann_id = span.annotation.annotation_id
        crud.event.delete_event_span(db, span.span_id)
        from event_storage.models.annotation import Annotation

        assert db.get(Annotation, ann_id) is None
