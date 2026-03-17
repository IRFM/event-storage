# tests/conftest.py
import sys
from pathlib import Path
from tempfile import mkdtemp

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

sys._called_from_test = True

from event_storage import Base  # noqa: E402
from event_storage import crud  # noqa: E402
from event_storage.models.annotation import Annotation2D  # noqa: E402
from event_storage.models.event import Event, EventSpan  # noqa: E402
from event_storage.models.reference import (  # noqa: E402
    Category,
    Criticality,
    Method,
    Signal,
    Status,
)


@pytest.fixture(scope="session")
def engine():
    tmp = Path(mkdtemp()) / "test.sqlite"
    _engine = create_engine("sqlite:///" + str(tmp))
    Base.metadata.create_all(_engine)
    yield _engine
    Base.metadata.drop_all(_engine)


@pytest.fixture
def db(engine):
    """Isolated, rolled-back session per test."""
    connection = engine.connect()
    transaction = connection.begin()
    session = Session(connection, future=True)

    try:
        yield session
    finally:
        session.close()
        if transaction.is_active:
            transaction.rollback()
        connection.close()


def make_full_event_in_memory(
    user_id: str = "user1",
    signal_path: str = "/sig/1",
    experiment_id: int = 42,
    start_ns: int = 0,
    end_ns: int = 1000,
) -> Event:
    """Build a complete detached Event graph. Must be called before any session is opened."""
    return Event(
        user_id=user_id,
        description="test event",
        criticality=Criticality(level="High"),
        category=Category(name="hotspot"),
        status=Status(name="New"),
        event_spans=[
            EventSpan(
                start_time_ns=start_ns,
                end_time_ns=end_ns,
                confidence=0.9,
                signal=Signal(
                    machine="WEST",
                    experiment_id=experiment_id,
                    diagnostic="DIAG",
                    type="IR",
                    path=signal_path,
                ),
                method=Method(name="Manual", is_manual=True, configuration=None),
                annotation=Annotation2D.from_bbox(0.0, 0.0, 10.0, 10.0),
                span_metadata=[],
            )
        ],
    )


@pytest.fixture
def full_event(db) -> Event:
    """A fully persisted Event with one span and one 2D annotation."""
    event = make_full_event_in_memory()
    return crud.event.push_event(db, event)


def make_signal(
    db,
    machine: str = "WEST",
    experiment_id: int = None,
    diagnostic: str = "DIAG",
    type_: str = "IR",
    path: str = "/default",
) -> Signal:
    if experiment_id is None:
        experiment_id = abs(hash(path)) % 10_000_000
    return crud.reference.create_signal(
        db,
        machine=machine,
        experiment_id=experiment_id,
        diagnostic=diagnostic,
        type=type_,
        path=path,
    )


def make_method(db, name: str = "Manual", is_manual: bool = True) -> Method:
    return crud.reference.create_method(db, name=name, is_manual=is_manual)


def make_event(db, user_id: str = "user1") -> Event:
    cid = crud.reference.get_or_create_criticality(db, "High").criticality_id
    catid = crud.reference.get_or_create_category(db, "hotspot").category_id
    sid = crud.reference.get_or_create_status(db, "New").status_id
    return crud.event.create_event(
        db,
        user_id=user_id,
        criticality_id=cid,
        category_id=catid,
        status_id=sid,
    )


def make_span(
    db,
    event: Event,
    path: str = "/span_sig",
    start: int = 0,
    end: int = 100,
    method: Method = None,
) -> EventSpan:
    sig = make_signal(db, path=path)
    if method is None:
        method = make_method(db)
    return crud.event.create_event_span(
        db,
        event_id=event.event_id,
        signal_id=sig.signal_id,
        method_id=method.method_id,
        start_time_ns=start,
        end_time_ns=end,
    )
