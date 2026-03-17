from typing import List, Optional

from sqlalchemy import select
from sqlalchemy.orm import Session

from event_storage import crud
from event_storage.models.event import Event, EventSpan

# ============================================================================
# Event CRUD
# ============================================================================


def create_event(
    db: Session,
    user_id: Optional[str],
    criticality_id: int,
    category_id: int,
    status_id: int,
    description: Optional[str] = "",
) -> Event:
    """Create a new event."""
    event = Event(
        user_id=user_id,
        description=description,
        criticality_id=criticality_id,
        category_id=category_id,
        status_id=status_id,
    )
    db.add(event)
    db.commit()
    db.refresh(event)
    return event


def get_event(db: Session, event_id: int) -> Optional[Event]:
    """Get an event by ID."""
    return db.get(Event, event_id)


def get_events(
    db: Session,
    user_id: Optional[str] = None,
    criticality_id: Optional[int] = None,
    method_id: Optional[int] = None,
    status_id: Optional[int] = None,
    skip: int = 0,
    limit: int = 100,
) -> List[Event]:
    """Get events with optional filters."""
    query = select(Event)

    if user_id is not None:
        query = query.where(Event.user_id == user_id)
    if criticality_id is not None:
        query = query.where(Event.criticality_id == criticality_id)
    if method_id is not None:
        query = (
            query.join(Event.event_spans)
            .where(EventSpan.method_id == method_id)
            .distinct()
        )
    if status_id is not None:
        query = query.where(Event.status_id == status_id)

    return db.execute(query.offset(skip).limit(limit)).scalars().all()


def update_event(
    db: Session,
    event_id: int,
    user_id: Optional[str] = None,
    description: Optional[str] = None,
    criticality_id: Optional[int] = None,
    category_id: Optional[int] = None,
    status_id: Optional[int] = None,
) -> Optional[Event]:
    """Update an event."""
    event = get_event(db, event_id)
    if not event:
        return None
    if user_id is not None:
        event.user_id = user_id
    if description is not None:
        event.description = description
    if criticality_id is not None:
        event.criticality_id = criticality_id
    if category_id is not None:
        event.category_id = category_id
    if status_id is not None:
        event.status_id = status_id
    db.commit()
    db.refresh(event)
    return event


def delete_event(db: Session, event_id: int) -> bool:
    """Delete an event."""
    event = get_event(db, event_id)
    if not event:
        return False
    db.delete(event)
    db.commit()
    return True


def push_event(db: Session, event: Event) -> Event:
    """Persist a fully constructed in-memory Event object graph to the DB.

    Resolves or creates all reference objects (signal, method, etc.)
    by name rather than by ID, so the caller never needs to touch the DB
    before calling this.

    Args:
        db: Database session
        event: Fully constructed Event with event_spans, annotations,
               and metadata attached via relationships (not FK ids)

    Returns:
        The persisted Event with all IDs assigned
    """
    # Step 1 — extract all values from the in-memory graph BEFORE any
    # DB interaction. Once any get_or_create_* call commits, SQLAlchemy
    # expires every tracked object in the session, making relationship
    # attributes inaccessible on objects that have not been persisted.
    event_criticality_level = event.criticality.level
    event_category_name = event.category.name
    event_status_name = event.status.name

    span_data = []
    for span in event.event_spans:
        annotation_data = None
        if span.annotation is not None:
            ann = span.annotation
            annotation_data = {
                "obj": ann,
                "type_name": ann.annotation_type.name,
            }

        metadata_data = [
            {"obj": m, "type_name": m.metadata_type.name} for m in span.span_metadata
        ]

        span_data.append(
            {
                "obj": span,
                "signal": {
                    "machine": span.signal.machine,
                    "experiment_id": span.signal.experiment_id,
                    "diagnostic": span.signal.diagnostic,
                    "type": span.signal.type,
                    "path": span.signal.path,
                },
                "method": {
                    "name": span.method.name,
                    "description": getattr(span.method, "description", None),
                    "configuration": span.method.configuration,
                },
                "annotation": annotation_data,
                "metadata": metadata_data,
            }
        )

    # Step 2 — resolve or create all reference rows, then wire up FKs.
    with db.no_autoflush:
        criticality = crud.reference.get_or_create_criticality(
            db, level=event_criticality_level
        )
        category = crud.reference.get_or_create_category(db, name=event_category_name)
        status = crud.reference.get_or_create_status(db, name=event_status_name)

        event.criticality_id = criticality.criticality_id
        event.category_id = category.category_id
        event.status_id = status.status_id
        event.criticality = criticality
        event.category = category
        event.status = status

        for sd in span_data:
            span = sd["obj"]

            signal = crud.reference.get_or_create_signal(db, **sd["signal"])
            method = crud.reference.get_or_create_method(db, **sd["method"])

            span.signal_id = signal.signal_id
            span.method_id = method.method_id
            span.signal = signal
            span.method = method
            span.experiment_id = signal.experiment_id
            span.diagnostic = signal.diagnostic
            span.category_id = category.category_id
            span.method_name = method.name

            if sd["annotation"] is not None:
                ann = sd["annotation"]["obj"]
                ann_type = crud.reference.get_or_create_annotation_type(
                    db, name=sd["annotation"]["type_name"]
                )
                ann.type = ann_type.type_id
                ann.annotation_type = ann_type

            for md in sd["metadata"]:
                meta = md["obj"]
                meta_type = crud.reference.get_or_create_metadata_type(
                    db, name=md["type_name"]
                )
                meta.type = meta_type.metadata_type_id
                meta.metadata_type = meta_type

    db.add(event)
    db.commit()
    db.refresh(event)
    return event


# ============================================================================
# EventSpan CRUD
# ============================================================================


def create_event_span(
    db: Session,
    event_id: int,
    signal_id: int,
    method_id: int,
    start_time_ns: int,
    end_time_ns: int,
    confidence: Optional[float] = None,
) -> EventSpan:
    """Create a new event span."""
    if start_time_ns > end_time_ns:
        raise ValueError("start_time_ns must be less than or equal to end_time_ns")
    event_span = EventSpan(
        event_id=event_id,
        signal_id=signal_id,
        method_id=method_id,
        start_time_ns=start_time_ns,
        end_time_ns=end_time_ns,
        confidence=confidence,
    )
    db.add(event_span)
    db.commit()
    db.refresh(event_span)
    return event_span


def get_event_span(db: Session, span_id: int) -> Optional[EventSpan]:
    """Get an event span by ID."""
    return db.get(EventSpan, span_id)


def get_event_spans(
    db: Session,
    event_id: Optional[int] = None,
    signal_id: Optional[int] = None,
    method_id: Optional[int] = None,
    start_time_ns: Optional[int] = None,
    end_time_ns: Optional[int] = None,
) -> List[EventSpan]:
    """Get event spans with optional filters."""
    query = select(EventSpan)

    if event_id is not None:
        query = query.where(EventSpan.event_id == event_id)
    if signal_id is not None:
        query = query.where(EventSpan.signal_id == signal_id)
    if method_id is not None:
        query = query.where(EventSpan.method_id == method_id)
    if start_time_ns is not None:
        query = query.where(EventSpan.end_time_ns >= start_time_ns)
    if end_time_ns is not None:
        query = query.where(EventSpan.start_time_ns <= end_time_ns)

    return db.execute(query).scalars().all()


def update_event_span(
    db: Session,
    span_id: int,
    start_time_ns: Optional[int] = None,
    end_time_ns: Optional[int] = None,
    confidence: Optional[float] = None,
) -> Optional[EventSpan]:
    """Update an event span."""
    event_span = get_event_span(db, span_id)
    if not event_span:
        return None
    if start_time_ns is not None:
        event_span.start_time_ns = start_time_ns
    if end_time_ns is not None:
        event_span.end_time_ns = end_time_ns
    if confidence is not None:
        event_span.confidence = confidence
    if event_span.start_time_ns > event_span.end_time_ns:
        raise ValueError("start_time_ns must be less than or equal to end_time_ns")
    db.commit()
    db.refresh(event_span)
    return event_span


def delete_event_span(db: Session, span_id: int) -> bool:
    """Delete an event span."""
    event_span = get_event_span(db, span_id)
    if not event_span:
        return False
    db.delete(event_span)
    db.commit()
    return True
