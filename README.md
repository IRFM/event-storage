# event_storage: Fusion Diagnostics Event Storage System

[![pytest](https://github.com/IRFM/event-storage/actions/workflows/python-package.yml/badge.svg)](https://github.com/IRFM/event-storage/actions/workflows/python-package.yml)

A SQLAlchemy-based storage library for managing events detected or annotated on experimental signals, designed for fusion/tokamak diagnostics.

## Overview

`event_storage` stores **events** — occurrences detected on diagnostic signals — along with their time spans, spatial annotations, and physical metadata. It supports both SQLite (development/testing) and MySQL (production).

## Data Model
```
Signal ──< EventSpan >── Event
               │
               ├── Annotation (2D bbox/polygon/mask, 3D bbox)
               ├── Metadata   (IR temperatures, Pulse state, StrikeLine)
               └── Dataset    (M2M, for ML training sets)
```

## Installation
```bash
pip install -e .
```

## Configuration

Create a `.env` file:
```bash
# SQLite
SQLITE=true
SQLITE_DATABASE_FILE=database.db

# MySQL
SQLITE=false
MYSQL_HOST=localhost
MYSQL_DATABASE=event_storage
MYSQL_USER=user
MYSQL_PASSWORD=password
```

Settings are loaded from `~/.env.default` then `.env`, with `.env` taking precedence.

## Usage

### Initialize the database
```bash
python scripts/init_db.py
```

### Push a fully constructed event

The primary write path. Build the object graph in pure Python, then persist it in one call. `push_event` resolves all reference objects by name, so you never need to look up IDs beforehand.
```python
from event_storage.models.event import Event, EventSpan
from event_storage.models.annotation import Annotation2D
from event_storage.models.reference import Signal, Method, Criticality, Category, Status
from event_storage.crud.event import push_event
from event_storage.database import SessionLocal

event = Event(
    user_id="alice",
    description="Hotspot detected on inner wall",
    criticality=Criticality(level="High"),
    category=Category(name="hotspot"),
    status=Status(name="New"),
    event_spans=[
        EventSpan(
            start_time_ns=1_000_000,
            end_time_ns=2_000_000,
            confidence=0.92,
            signal=Signal(
                machine="WEST",
                experiment_id=55042,
                diagnostic="FLIR",
                type="IR",
                path="/data/west/55042/flir",
            ),
            method=Method(name="AutoDetect", configuration={"threshold": 0.5}),
            annotation=Annotation2D.from_bbox(x=120, y=80, w=30, h=20),
            span_metadata=[],
        )
    ],
)

with SessionLocal() as db:
    persisted = push_event(db, event)
```

### Read events
```python
from event_storage.crud.event import get_events, get_event
from event_storage.database import SessionLocal

with SessionLocal() as db:
    # All high-criticality events
    events = get_events(db, criticality_id=3)

    # Single event with full span/annotation data
    event = get_event(db, event_id=42)
    print(event.to_dict())
```

### Annotations
```python
from event_storage.models.annotation import Annotation2D
import numpy as np

# From bounding box (x, y, w, h)
ann = Annotation2D.from_bbox(x=10, y=20, w=50, h=30)

# From polygon (Nx2 array)
ann = Annotation2D.from_polygon(np.array([[0,0],[10,0],[10,10],[0,10]]))

# From binary mask (HxW array)
ann = Annotation2D.from_mask(mask)
polygon = ann.get_polygon()
mask = ann.get_mask(height=480, width=640)
```

### Datasets
```python
from event_storage.crud.dataset import (
    create_dataset, add_event_span_to_dataset, get_dataset_summary
)
from event_storage.database import SessionLocal

with SessionLocal() as db:
    ds = create_dataset(db, name="training_v1", description="Initial training set")
    add_event_span_to_dataset(db, ds.dataset_id, span_id=17)
    print(get_dataset_summary(db, ds.dataset_id))
```

### Processing status
```python
from event_storage.crud.processing import (
    create_or_update_processing_status, get_unprocessed_signals
)
from event_storage.database import SessionLocal

with SessionLocal() as db:
    # Mark a signal as processed
    create_or_update_processing_status(
        db, signal_id=5, method_id=2,
        status="completed", processing_details={"duration_s": 12.4}
    )

    # Find signals not yet processed by a method
    pending = get_unprocessed_signals(db, method_id=2)
```

### Export to JSON
```python
# Single event
event.to_json("event_42.json")

# Multiple events
Event.write_events_to_json("events.json", events)
```

## Testing
```bash
pytest tests/ -v
```

Tests use a temporary SQLite database via a session-scoped engine and per-test rollback, so no cleanup is needed between runs. The test database is activated automatically by the presence of `sys._called_from_test`.

## Notes

- Binary data (masks, polygons, segmented points) stored as BLOB/LargeBinary
- Foreign key constraints are enforced
- Time validation ensures `start_time_ns <= end_time_ns`
