from typing import List, Optional

from sqlalchemy import select
from sqlalchemy.orm import Session

from event_storage import crud
from event_storage.models.annotation import Annotation, Annotation2D, Annotation3D


# ============================================================================
# Annotation Base CRUD
# ============================================================================


def get_annotation(db: Session, annotation_id: int) -> Optional[Annotation]:
    """Get an annotation by ID (returns base or polymorphic type)."""
    return db.get(Annotation, annotation_id)


def get_annotations(
    db: Session,
    span_id: Optional[int] = None,
    type_id: Optional[int] = None,
    skip: int = 0,
    limit: int = 100,
) -> List[Annotation]:
    """Get annotations with optional filters."""
    query = select(Annotation)
    if span_id is not None:
        query = query.where(Annotation.span_id == span_id)
    if type_id is not None:
        query = query.where(Annotation.type == type_id)
    return db.execute(query.offset(skip).limit(limit)).scalars().all()


def delete_annotation(db: Session, annotation_id: int) -> bool:
    """Delete an annotation (works for all annotation types)."""
    annotation = get_annotation(db, annotation_id)
    if not annotation:
        return False
    db.delete(annotation)
    db.commit()
    return True


# ============================================================================
# Annotation2D CRUD
# ============================================================================


def create_annotation_2d(
    db: Session,
    span_id: int,
    bbox_x1: float,
    bbox_y1: float,
    bbox_x2: float,
    bbox_y2: float,
    polygon_coordinates: Optional[bytes] = None,
    mask_data: Optional[bytes] = None,
) -> Annotation2D:
    """Create a new 2D annotation."""
    ann_type = crud.reference.get_or_create_annotation_type(
        db, name=Annotation2D.ANNOTATION_TYPE_NAME
    )
    annotation = Annotation2D(
        annotation_subtype="2d",
        type=ann_type.type_id,
        annotation_type=ann_type,
        span_id=span_id,
        bbox_x1=bbox_x1,
        bbox_y1=bbox_y1,
        bbox_x2=bbox_x2,
        bbox_y2=bbox_y2,
        polygon_coordinates=polygon_coordinates,
        mask_data=mask_data,
    )
    db.add(annotation)
    db.commit()
    db.refresh(annotation)
    return annotation


def get_annotation_2d(db: Session, annotation_id: int) -> Optional[Annotation2D]:
    """Get a 2D annotation by ID."""
    return db.get(Annotation2D, annotation_id)


def get_annotations_2d(
    db: Session,
    span_id: Optional[int] = None,
    skip: int = 0,
    limit: int = 100,
) -> List[Annotation2D]:
    """Get all 2D annotations with optional span filter."""
    query = select(Annotation2D)
    if span_id is not None:
        query = query.where(Annotation2D.span_id == span_id)
    return db.execute(query.offset(skip).limit(limit)).scalars().all()


def update_annotation_2d(
    db: Session,
    annotation_id: int,
    bbox_x1: Optional[float] = None,
    bbox_y1: Optional[float] = None,
    bbox_x2: Optional[float] = None,
    bbox_y2: Optional[float] = None,
    polygon_coordinates: Optional[bytes] = None,
    mask_data: Optional[bytes] = None,
) -> Optional[Annotation2D]:
    """Update a 2D annotation."""
    annotation = get_annotation_2d(db, annotation_id)
    if not annotation:
        return None
    if bbox_x1 is not None:
        annotation.bbox_x1 = bbox_x1
    if bbox_y1 is not None:
        annotation.bbox_y1 = bbox_y1
    if bbox_x2 is not None:
        annotation.bbox_x2 = bbox_x2
    if bbox_y2 is not None:
        annotation.bbox_y2 = bbox_y2
    if polygon_coordinates is not None:
        annotation.polygon_coordinates = polygon_coordinates
    if mask_data is not None:
        annotation.mask_data = mask_data
    db.commit()
    db.refresh(annotation)
    return annotation


# ============================================================================
# Annotation3D CRUD
# ============================================================================


def create_annotation_3d(
    db: Session,
    span_id: int,
    bbox_x1: float,
    bbox_y1: float,
    bbox_z1: float,
    bbox_x2: float,
    bbox_y2: float,
    bbox_z2: float,
    volume: float,
    area: float,
) -> Annotation3D:
    """Create a new 3D annotation."""
    ann_type = crud.reference.get_or_create_annotation_type(
        db, name=Annotation3D.ANNOTATION_TYPE_NAME
    )
    annotation = Annotation3D(
        annotation_subtype="3d",
        type=ann_type.type_id,
        annotation_type=ann_type,
        span_id=span_id,
        bbox_x1=bbox_x1,
        bbox_y1=bbox_y1,
        bbox_z1=bbox_z1,
        bbox_x2=bbox_x2,
        bbox_y2=bbox_y2,
        bbox_z2=bbox_z2,
        volume=volume,
        area=area,
    )
    db.add(annotation)
    db.commit()
    db.refresh(annotation)
    return annotation


def get_annotation_3d(db: Session, annotation_id: int) -> Optional[Annotation3D]:
    """Get a 3D annotation by ID."""
    return db.get(Annotation3D, annotation_id)


def get_annotations_3d(
    db: Session,
    span_id: Optional[int] = None,
    skip: int = 0,
    limit: int = 100,
) -> List[Annotation3D]:
    """Get all 3D annotations with optional span filter."""
    query = select(Annotation3D)
    if span_id is not None:
        query = query.where(Annotation3D.span_id == span_id)
    return db.execute(query.offset(skip).limit(limit)).scalars().all()


def update_annotation_3d(
    db: Session,
    annotation_id: int,
    bbox_x1: Optional[float] = None,
    bbox_y1: Optional[float] = None,
    bbox_z1: Optional[float] = None,
    bbox_x2: Optional[float] = None,
    bbox_y2: Optional[float] = None,
    bbox_z2: Optional[float] = None,
    volume: Optional[float] = None,
    area: Optional[float] = None,
) -> Optional[Annotation3D]:
    """Update a 3D annotation."""
    annotation = get_annotation_3d(db, annotation_id)
    if not annotation:
        return None
    if bbox_x1 is not None:
        annotation.bbox_x1 = bbox_x1
    if bbox_y1 is not None:
        annotation.bbox_y1 = bbox_y1
    if bbox_z1 is not None:
        annotation.bbox_z1 = bbox_z1
    if bbox_x2 is not None:
        annotation.bbox_x2 = bbox_x2
    if bbox_y2 is not None:
        annotation.bbox_y2 = bbox_y2
    if bbox_z2 is not None:
        annotation.bbox_z2 = bbox_z2
    if volume is not None:
        annotation.volume = volume
    if area is not None:
        annotation.area = area
    db.commit()
    db.refresh(annotation)
    return annotation
