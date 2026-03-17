from datetime import datetime
from typing import List, Optional

from sqlalchemy import and_, select
from sqlalchemy.orm import Session

from event_storage.crud import reference
from event_storage.models.processing import ProcessingStatus, ProcessingStatusEnum
from event_storage.models.reference import Signal


def create_or_update_processing_status(
    db: Session,
    signal_id: int,
    method_id: int,
    status: str = "pending",
    processing_details: Optional[dict] = None,
) -> ProcessingStatus:
    """Create or update processing status for a signal/method pair."""
    if not reference.get_signal(db, signal_id) or not reference.get_method(
        db, method_id
    ):
        raise ValueError("Signal or method not found")

    status_entry = db.execute(
        select(ProcessingStatus).where(
            and_(
                ProcessingStatus.signal_id == signal_id,
                ProcessingStatus.method_id == method_id,
            )
        )
    ).scalar_one_or_none()

    if status_entry:
        status_entry.status = ProcessingStatusEnum(status)
        status_entry.processed_at = datetime.now()
        if processing_details is not None:
            status_entry.processing_details = processing_details
    else:
        status_entry = ProcessingStatus(
            signal_id=signal_id,
            method_id=method_id,
            status=ProcessingStatusEnum(status),
            processing_details=processing_details,
        )
        db.add(status_entry)

    db.commit()
    db.refresh(status_entry)
    return status_entry


# Alias for backwards compatibility
create_processing_status = create_or_update_processing_status


def get_processing_status(
    db: Session, signal_id: int, method_id: int
) -> Optional[ProcessingStatus]:
    """Get processing status for a specific signal and method."""
    return db.execute(
        select(ProcessingStatus).where(
            and_(
                ProcessingStatus.signal_id == signal_id,
                ProcessingStatus.method_id == method_id,
            )
        )
    ).scalar_one_or_none()


def get_signal_processing_statuses(
    db: Session, signal_id: int
) -> List[ProcessingStatus]:
    """Get all processing statuses for a signal."""
    return (
        db.execute(
            select(ProcessingStatus)
            .where(ProcessingStatus.signal_id == signal_id)
            .order_by(ProcessingStatus.method_id)
        )
        .scalars()
        .all()
    )


def get_method_processing_statuses(
    db: Session, method_id: int, status: Optional[str] = None
) -> List[ProcessingStatus]:
    """Get processing statuses for a specific method."""
    query = select(ProcessingStatus).where(ProcessingStatus.method_id == method_id)
    if status:
        query = query.where(ProcessingStatus.status == ProcessingStatusEnum(status))
    return db.execute(query.order_by(ProcessingStatus.signal_id)).scalars().all()


def get_unprocessed_signals(db: Session, method_id: int) -> List[Signal]:
    """Get all signals that haven't been processed by a specific method."""
    processed_ids = {
        row.signal_id
        for row in db.execute(
            select(ProcessingStatus.signal_id).where(
                ProcessingStatus.method_id == method_id
            )
        ).all()
    }
    return (
        db.execute(select(Signal).where(Signal.signal_id.notin_(processed_ids)))
        .scalars()
        .all()
    )


def update_processing_status(
    db: Session,
    status_id: int,
    status: Optional[str] = None,
    processing_details: Optional[dict] = None,
) -> Optional[ProcessingStatus]:
    """Update processing status by its primary key."""
    status_entry = db.get(ProcessingStatus, status_id)
    if not status_entry:
        return None
    if status is not None:
        status_entry.status = ProcessingStatusEnum(status)
    if processing_details is not None:
        status_entry.processing_details = processing_details
    status_entry.processed_at = datetime.now()
    db.commit()
    db.refresh(status_entry)
    return status_entry


def delete_processing_status(db: Session, status_id: int) -> bool:
    """Delete processing status."""
    status_entry = db.get(ProcessingStatus, status_id)
    if not status_entry:
        return False
    db.delete(status_entry)
    db.commit()
    return True
