"""CRUD tests for ProcessingStatus."""

import pytest
from conftest import make_method, make_signal

from event_storage import crud


class TestProcessingStatusCrud:
    def test_create_and_get(self, db):
        sig = make_signal(db, path="/ps/1")
        method = make_method(db)
        crud.processing.create_or_update_processing_status(
            db, signal_id=sig.signal_id, method_id=method.method_id, status="pending"
        )
        ps = crud.processing.get_processing_status(db, sig.signal_id, method.method_id)
        assert ps.status == "pending"

    def test_update_existing_upserts(self, db):
        sig = make_signal(db, path="/ps/upsert")
        method = make_method(db)
        crud.processing.create_or_update_processing_status(
            db, signal_id=sig.signal_id, method_id=method.method_id, status="pending"
        )
        crud.processing.create_or_update_processing_status(
            db, signal_id=sig.signal_id, method_id=method.method_id, status="completed"
        )
        all_ps = crud.processing.get_signal_processing_statuses(db, sig.signal_id)
        assert len(all_ps) == 1
        assert all_ps[0].status == "completed"

    def test_missing_signal_raises(self, db):
        method = make_method(db)
        with pytest.raises(ValueError, match="Signal or method not found"):
            crud.processing.create_or_update_processing_status(
                db, signal_id=99999, method_id=method.method_id
            )

    def test_missing_method_raises(self, db):
        sig = make_signal(db, path="/ps/no_method")
        with pytest.raises(ValueError, match="Signal or method not found"):
            crud.processing.create_or_update_processing_status(
                db, signal_id=sig.signal_id, method_id=99999
            )

    def test_get_not_found(self, db):
        assert crud.processing.get_processing_status(db, 99999, 99999) is None

    def test_filter_by_status(self, db):
        method = make_method(db)
        for i, status in enumerate(["completed", "completed", "failed"]):
            sig = make_signal(db, path=f"/ps/filter/{i}")
            crud.processing.create_or_update_processing_status(
                db, signal_id=sig.signal_id, method_id=method.method_id, status=status
            )
        completed = crud.processing.get_method_processing_statuses(
            db, method.method_id, status="completed"
        )
        assert len(completed) == 2

    def test_get_unprocessed_signals(self, db):
        method = make_method(db)
        processed = make_signal(db, path="/ps/done")
        unprocessed = make_signal(db, path="/ps/not_done")
        crud.processing.create_or_update_processing_status(
            db, signal_id=processed.signal_id, method_id=method.method_id
        )
        ids = {
            s.signal_id
            for s in crud.processing.get_unprocessed_signals(db, method.method_id)
        }
        assert unprocessed.signal_id in ids
        assert processed.signal_id not in ids

    def test_update_by_id(self, db):
        sig = make_signal(db, path="/ps/upd_id")
        method = make_method(db)
        ps = crud.processing.create_or_update_processing_status(
            db, signal_id=sig.signal_id, method_id=method.method_id, status="pending"
        )
        updated = crud.processing.update_processing_status(
            db, ps.status_id, status="processing", processing_details={"step": 1}
        )
        assert updated.status == "processing"
        assert updated.processing_details == {"step": 1}

    def test_update_preserves_details_when_none_passed(self, db):
        sig = make_signal(db, path="/ps/preserve")
        method = make_method(db)
        ps = crud.processing.create_or_update_processing_status(
            db,
            signal_id=sig.signal_id,
            method_id=method.method_id,
            processing_details={"original": True},
        )
        updated = crud.processing.update_processing_status(
            db, ps.status_id, status="completed"
        )
        assert updated.processing_details == {"original": True}

    def test_delete(self, db):
        sig = make_signal(db, path="/ps/del")
        method = make_method(db)
        ps = crud.processing.create_or_update_processing_status(
            db, signal_id=sig.signal_id, method_id=method.method_id
        )
        assert crud.processing.delete_processing_status(db, ps.status_id) is True
        assert (
            crud.processing.get_processing_status(db, sig.signal_id, method.method_id)
            is None
        )

    def test_delete_not_found(self, db):
        assert crud.processing.delete_processing_status(db, 99999) is False
