"""CRUD tests for Dataset."""

from conftest import make_full_event_in_memory

from event_storage import crud


def _persisted_span(db, path, experiment_id):
    event = make_full_event_in_memory(signal_path=path, experiment_id=experiment_id)
    return crud.event.push_event(db, event).event_spans[0]


class TestDatasetCrud:
    def test_create_and_get(self, db):
        ds = crud.dataset.create_dataset(db, name="DS1", description="test")
        assert crud.dataset.get_dataset(db, ds.dataset_id).name == "DS1"

    def test_get_not_found(self, db):
        assert crud.dataset.get_dataset(db, 99999) is None

    def test_get_or_create_idempotent(self, db):
        d1 = crud.dataset.get_or_create_dataset(db, "Idem", "desc")
        d2 = crud.dataset.get_or_create_dataset(db, "Idem", "desc")
        assert d1.dataset_id == d2.dataset_id

    def test_pagination(self, db):
        for i in range(5):
            crud.dataset.create_dataset(db, name=f"PageDS_{i}")
        assert len(crud.dataset.get_datasets(db, skip=1, limit=3)) == 3

    def test_update(self, db):
        ds = crud.dataset.create_dataset(db, name="ToUpdate")
        updated = crud.dataset.update_dataset(
            db, ds.dataset_id, name="Updated", description="new"
        )
        assert updated.name == "Updated"
        assert updated.description == "new"

    def test_update_not_found(self, db):
        assert crud.dataset.update_dataset(db, 99999, name="x") is None

    def test_delete(self, db):
        ds = crud.dataset.create_dataset(db, name="ToDelete")
        assert crud.dataset.delete_dataset(db, ds.dataset_id) is True
        assert crud.dataset.get_dataset(db, ds.dataset_id) is None

    def test_delete_not_found(self, db):
        assert crud.dataset.delete_dataset(db, 99999) is False

    def test_add_and_list_spans(self, db):
        ds = crud.dataset.create_dataset(db, name="SpanDS")
        span = _persisted_span(db, "/ds/add/1", 601)
        assert (
            crud.dataset.add_event_span_to_dataset(db, ds.dataset_id, span.span_id)
            is True
        )
        spans = crud.dataset.get_dataset_event_spans(db, ds.dataset_id)
        assert any(s.span_id == span.span_id for s in spans)

    def test_add_span_dataset_not_found(self, db):
        span = _persisted_span(db, "/ds/add/missing", 602)
        assert crud.dataset.add_event_span_to_dataset(db, 99999, span.span_id) is False

    def test_add_span_span_not_found(self, db):
        ds = crud.dataset.create_dataset(db, name="MissingSpan")
        assert crud.dataset.add_event_span_to_dataset(db, ds.dataset_id, 99999) is False

    def test_remove_span(self, db):
        ds = crud.dataset.create_dataset(db, name="RemoveSpan")
        span = _persisted_span(db, "/ds/rem/1", 603)
        crud.dataset.add_event_span_to_dataset(db, ds.dataset_id, span.span_id)
        assert (
            crud.dataset.remove_event_span_from_dataset(db, ds.dataset_id, span.span_id)
            is True
        )
        assert not any(
            s.span_id == span.span_id
            for s in crud.dataset.get_dataset_event_spans(db, ds.dataset_id)
        )

    def test_remove_span_not_in_dataset(self, db):
        ds = crud.dataset.create_dataset(db, name="NotIn")
        span = _persisted_span(db, "/ds/rem/notIn", 604)
        assert (
            crud.dataset.remove_event_span_from_dataset(db, ds.dataset_id, span.span_id)
            is False
        )

    def test_summary(self, db):
        ds = crud.dataset.create_dataset(db, name="Summary")
        span = _persisted_span(db, "/ds/summary/1", 605)
        crud.dataset.add_event_span_to_dataset(db, ds.dataset_id, span.span_id)
        summary = crud.dataset.get_dataset_summary(db, ds.dataset_id)
        assert summary["event_span_count"] == 1
        assert summary["name"] == "Summary"

    def test_summary_not_found(self, db):
        assert crud.dataset.get_dataset_summary(db, 99999) is None

    def test_get_spans_empty_when_not_found(self, db):
        assert crud.dataset.get_dataset_event_spans(db, 99999) == []

    def test_get_event_spans_not_in_dataset(self, db):
        ds = crud.dataset.create_dataset(db, name="NotInFixed")
        span_in = _persisted_span(db, "/ds/notin/in", 701)
        span_out = _persisted_span(db, "/ds/notin/out", 702)
        crud.dataset.add_event_span_to_dataset(db, ds.dataset_id, span_in.span_id)
        results = crud.dataset.get_event_spans_not_in_dataset(db, ds.dataset_id)
        ids = {s.span_id for s in results}
        assert span_out.span_id in ids
        assert span_in.span_id not in ids
