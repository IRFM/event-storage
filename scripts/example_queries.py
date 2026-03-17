"""Example queries demonstrating database usage."""

from event_storage.database import SessionLocal
from event_storage.crud import event, annotation, metadata, reference, dataset
from event_storage.models.annotation import Annotation2D, Annotation3D
from event_storage.models.metadata import MetadataIR


def run_example_queries():
    """Run example queries to demonstrate the database."""
    with SessionLocal() as db:
        print("=" * 80)
        print("EXAMPLE DATABASE QUERIES")
        print("=" * 80)

        # Get all events
        events = event.get_events(db)
        print(f"\nTotal events in database: {len(events)}\n")

        for evt in events:
            print(f"Event {evt.event_id}: {evt.description}")
            print(f"  User: {evt.user_id}")
            print(f"  Criticality: {evt.criticality.level}")

            print(f"  Status: {evt.status.name}")
            print(f"  Created: {evt.created_at}")

            # Get event spans
            spans = event.get_event_spans(db, event_id=evt.event_id)

            for span in spans:
                print(f"\n  Span {span.span_id}:")
                print(f"    Signal: {span.signal.path} ({span.signal.type})")
                print(f"    Method: {span.method.name}")
                print(f"    Time: {span.start_time_ns:.2f}s - {span.end_time_ns:.2f}s")
                print(f"    Confidence: {span.confidence}")

                # Display annotation details
                ann = span.annotation
                if isinstance(ann, Annotation2D):
                    print("    Annotation: 2D Bounding Box")
                    print(
                        f"      BBox: ({ann.bbox_x1:.1f}, {ann.bbox_y1:.1f}) to "
                        f"({ann.bbox_x2:.1f}, {ann.bbox_y2:.1f})"
                    )
                    if ann.width is not None and ann.height is not None:
                        print(f"      Dimensions: {ann.width:.1f} x {ann.height:.1f}")
                    if ann.area is not None:
                        print(f"      Area: {ann.area:.2f}")
                elif isinstance(ann, Annotation3D):
                    print("    Annotation: 3D Volume")
                    print(f"      Volume: {ann.volume:.2f}")
                    if ann.area is not None:
                        print(f"      Surface Area: {ann.area:.2f}")
                    print(
                        f"      BBox: ({ann.bbox_x1:.1f}, {ann.bbox_y1:.1f}, {ann.bbox_z1:.1f}) to "
                        f"({ann.bbox_x2:.1f}, {ann.bbox_y2:.1f}, {ann.bbox_z2:.1f})"
                    )
                    if (
                        ann.width is not None
                        and ann.height is not None
                        and ann.depth is not None
                    ):
                        print(
                            f"      Dimensions: {ann.width:.1f} x {ann.height:.1f} x {ann.depth:.1f}"
                        )

                # Display metadata
                meta_list = span.span_metadata
                if meta_list:
                    print(f"    Metadata ({len(meta_list)} entries):")
                    for meta in meta_list:
                        if isinstance(meta, MetadataIR):
                            print(
                                f"        Max T: {meta.max_T_K:.1f}K at "
                                f"({meta.max_T_image_position_x:.1f}, {meta.max_T_image_position_y:.1f})"
                            )
                            print(
                                f"        Min T: {meta.min_T_K:.1f}K at "
                                f"({meta.min_T_image_position_x:.1f}, {meta.min_T_image_position_y:.1f})"
                            )
                            print(f"        Avg T: {meta.avg_T_K:.1f}K")

            print()

        print("=" * 80)

        # Additional query examples
        print("\nADDITIONAL QUERY EXAMPLES:\n")

        # Query by criticality
        high_crit = reference.get_criticality_by_level(db, "High")
        if high_crit:
            high_priority = event.get_events(
                db, criticality_id=high_crit.criticality_id
            )
            print(f"High priority events: {len(high_priority)}")

        # Query 2D annotations
        annotations_2d = annotation.get_annotations_2d(db)
        print(f"2D annotations: {len(annotations_2d)}")

        # Query 3D annotations
        annotations_3d = annotation.get_annotations_3d(db)
        print(f"3D annotations: {len(annotations_3d)}")

        # Query metadata by type
        ir_metadata = metadata.get_metadata_ir_list(db)
        print(f"IR metadata entries: {len(ir_metadata)}")

        # Dataset summary section
        print("\nDATASET SUMMARY:")
        print("-" * 50)

        datasets = dataset.get_datasets(db)
        print(f"Total datasets: {len(datasets)}")

        for ds in datasets:
            print(f"\nDataset: {ds.name}")
            print(f"  Description: {ds.description or 'No description'}")
            print(f"  Created: {ds.created_at}")

            # Get dataset summary
            summary = dataset.get_dataset_summary(db, ds.dataset_id)
            if summary:
                print(f"  Event spans: {summary['event_span_count']}")

        print("\n" + "=" * 80)


if __name__ == "__main__":
    run_example_queries()
