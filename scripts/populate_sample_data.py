"""Populate database with sample data for testing."""

from getpass import getuser

from event_storage.crud import dataset, reference
from event_storage.crud.event import push_event
from event_storage.database import SessionLocal
from event_storage.models.annotation import Annotation2D, Annotation3D
from event_storage.models.event import Event, EventSpan
from event_storage.models.metadata import MetadataIR
from event_storage.models.reference import (
    Category,
    Criticality,
    MetadataType,
    Method,
    Signal,
    Status,
)


def populate_sample_data():
    """Add sample signals, events, and annotations."""
    with SessionLocal() as db:
        print("Initializing reference data...")
        reference.initialize_reference_data(db)

        print("Creating sample signals...")

        _ = reference.get_or_create_signal(
            db,
            machine="WEST",
            experiment_id=60222,
            diagnostic="WAQ5B",
            type="ir_video",
            path="60222_WAQ5B",
        )
        _ = reference.get_or_create_signal(
            db,
            machine="ITER",
            experiment_id=10003,
            diagnostic="WAVS_IR_EPP_12",
            type="ir_video",
            path="10003_WAVS_IR_EPP_12",
        )

        print("Creating sample events with annotations...")

        # ----------------------------------------------------------------
        # Event 1 — UFO, manual, 2D annotation + IR metadata (offline pattern)
        # ----------------------------------------------------------------
        span1 = EventSpan(
            signal=Signal(
                machine="WEST",
                experiment_id=60222,
                diagnostic="WAQ5B",
                type="ir_video",
                path="60222_WAQ5B",
            ),
            method=Method(name="Manual", is_manual=True),
            start_time_ns=0,
            end_time_ns=10,
            confidence=0.95,
            annotation=Annotation2D.from_bbox(x=10.5, y=20.3, w=20.2, h=19.9),
            span_metadata=[
                MetadataIR(
                    metadata_subtype="ir",
                    metadata_type=MetadataType(name="IR"),
                    max_T_K=1200.5,
                    max_T_image_position_x=15.2,
                    max_T_image_position_y=25.8,
                    max_T_world_position_x_m=15.5,
                    max_T_world_position_y_m=25.5,
                    max_T_world_position_z_m=0.0,
                    avg_T_K=800.3,
                    min_T_K=600.0,
                    min_T_image_position_x=10.0,
                    min_T_image_position_y=20.0,
                    min_T_world_position_x_m=10.5,
                    min_T_world_position_y_m=20.5,
                    min_T_world_position_z_m=0.0,
                )
            ],
        )

        event1 = Event(
            user_id=getuser(),
            description="First test event",
            criticality=Criticality(level="Medium"),
            status=Status(name="Validated"),
            category=Category(name="ufo"),
            event_spans=[span1],
        )

        push_event(db, event1)

        # ----------------------------------------------------------------
        # Event 2 — strike line, automatic, 3D annotation + IR metadata
        # ----------------------------------------------------------------
        span2 = EventSpan(
            signal=Signal(
                machine="ITER",
                experiment_id=10003,
                diagnostic="WAVS_IR_EPP_12",
                type="ir_video",
                path="10003_WAVS_IR_EPP_12",
            ),
            method=Method(name="Automatic", is_manual=False),
            start_time_ns=0,
            end_time_ns=10,
            confidence=0.87,
            annotation=Annotation3D.from_bbox(
                x=10.5,
                y=20.3,
                z=10.9,
                w=20.2,
                h=19.9,
                d=9.7,
            ),
            span_metadata=[
                MetadataIR(
                    metadata_subtype="ir",
                    metadata_type=MetadataType(name="IR"),
                    max_T_K=1300.2,
                    max_T_image_position_x=12.5,
                    max_T_image_position_y=22.7,
                    max_T_world_position_x_m=12.8,
                    max_T_world_position_y_m=22.5,
                    max_T_world_position_z_m=0.0,
                    avg_T_K=900.1,
                    min_T_K=700.0,
                    min_T_image_position_x=8.0,
                    min_T_image_position_y=18.0,
                    min_T_world_position_x_m=8.5,
                    min_T_world_position_y_m=18.5,
                    min_T_world_position_z_m=0.0,
                )
            ],
        )

        event2 = Event(
            user_id=getuser(),
            description="Second test event",
            criticality=Criticality(level="High"),
            status=Status(name="Validated"),
            category=Category(name="strike_line"),
            event_spans=[span2],
        )

        push_event(db, event2)

        print("\nSample data created successfully!")
        print("- Created 2 signals")
        print("- Created 2 events")
        print(f"- Event 1 (id={event1.event_id}): ufo, WAQ5B, 2D annotation")
        print(
            f"- Event 2 (id={event2.event_id}): strike_line, WAVS_IR_EPP_12, 3D annotation"
        )

        print("\nCreating sample datasets...")

        dataset1 = dataset.create_dataset(
            db, name="Training Data Set 1", description="First training dataset"
        )
        dataset2 = dataset.create_dataset(
            db, name="Validation Data Set", description="Validation dataset"
        )

        dataset.add_event_span_to_dataset(
            db,
            dataset_id=dataset1.dataset_id,
            event_span_id=event1.event_spans[0].span_id,
        )
        dataset.add_event_span_to_dataset(
            db,
            dataset_id=dataset2.dataset_id,
            event_span_id=event2.event_spans[0].span_id,
        )

        print("- Created 2 datasets")
        print(f"- Dataset 1 '{dataset1.name}': {len(dataset1.spans)} span(s)")
        print(f"- Dataset 2 '{dataset2.name}': {len(dataset2.spans)} span(s)")


if __name__ == "__main__":
    populate_sample_data()
