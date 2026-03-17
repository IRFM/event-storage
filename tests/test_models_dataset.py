"""Tests for Dataset model and dataset_span_association cascade behaviour."""

from conftest import make_full_event_in_memory

from event_storage import crud
from event_storage.models import EventSpan
from event_storage.models.associations import dataset_span_association


class TestDatasetSpanAssociationCascade:
    def test_deleting_span_removes_association(self, db):
        """Deleting an EventSpan must clean up its rows in DatasetSpan."""
        event = make_full_event_in_memory(
            signal_path="/ds_cascade/1", experiment_id=501
        )
        persisted = crud.event.push_event(db, event)
        span = persisted.event_spans[0]

        ds = crud.dataset.create_dataset(db, name="CascadeDS")
        crud.dataset.add_event_span_to_dataset(db, ds.dataset_id, span.span_id)

        # Confirm the association row exists
        rows = db.execute(
            dataset_span_association.select().where(
                dataset_span_association.c.span_id == span.span_id
            )
        ).fetchall()
        assert len(rows) == 1

        # Delete the span — association must cascade away
        crud.event.delete_event_span(db, span.span_id)

        rows_after = db.execute(
            dataset_span_association.select().where(
                dataset_span_association.c.span_id == span.span_id
            )
        ).fetchall()
        assert len(rows_after) == 0

    def test_deleting_dataset_does_not_delete_spans(self, db):
        """Deleting a Dataset must NOT cascade to EventSpan rows."""
        event = make_full_event_in_memory(
            signal_path="/ds_cascade/2", experiment_id=502
        )
        persisted = crud.event.push_event(db, event)
        span = persisted.event_spans[0]
        span_id = span.span_id

        ds = crud.dataset.create_dataset(db, name="SafeDeleteDS")
        crud.dataset.add_event_span_to_dataset(db, ds.dataset_id, span.span_id)
        crud.dataset.delete_dataset(db, ds.dataset_id)

        assert (
            db.query(EventSpan).filter(EventSpan.span_id == span_id).first() is not None
        )

    def test_span_can_belong_to_multiple_datasets(self, db):
        event = make_full_event_in_memory(
            signal_path="/ds_cascade/3", experiment_id=503
        )
        persisted = crud.event.push_event(db, event)
        span = persisted.event_spans[0]

        ds1 = crud.dataset.create_dataset(db, name="Multi1")
        ds2 = crud.dataset.create_dataset(db, name="Multi2")
        crud.dataset.add_event_span_to_dataset(db, ds1.dataset_id, span.span_id)
        crud.dataset.add_event_span_to_dataset(db, ds2.dataset_id, span.span_id)

        rows = db.execute(
            dataset_span_association.select().where(
                dataset_span_association.c.span_id == span.span_id
            )
        ).fetchall()
        assert len(rows) == 2
