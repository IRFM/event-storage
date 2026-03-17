from typing import List, Optional

from sqlalchemy import select
from sqlalchemy.orm import Session

from event_storage import crud
from event_storage.models.metadata import (
    Metadata,
    MetadataIR,
    MetadataPulse,
    MetadataStrikeLine,
)


# ============================================================================
# Metadata Base CRUD
# ============================================================================


def get_metadata(db: Session, metadata_id: int) -> Optional[Metadata]:
    """Get metadata by ID (returns base or polymorphic type)."""
    return db.get(Metadata, metadata_id)


def get_metadata_list(
    db: Session,
    span_id: Optional[int] = None,
    type_id: Optional[int] = None,
    skip: int = 0,
    limit: int = 100,
) -> List[Metadata]:
    """Get metadata with optional filters."""
    query = select(Metadata)
    if span_id is not None:
        query = query.where(Metadata.span_id == span_id)
    if type_id is not None:
        query = query.where(Metadata.type == type_id)
    return db.execute(query.offset(skip).limit(limit)).scalars().all()


def delete_metadata(db: Session, metadata_id: int) -> bool:
    """Delete metadata (works for all metadata types)."""
    metadata = get_metadata(db, metadata_id)
    if not metadata:
        return False
    db.delete(metadata)
    db.commit()
    return True


# ============================================================================
# MetadataIR CRUD
# ============================================================================


def create_metadata_ir(
    db: Session,
    span_id: int,
    max_T_K: float,
    avg_T_K: float,
    min_T_K: float,
    max_T_image_position_x: Optional[float] = None,
    max_T_image_position_y: Optional[float] = None,
    max_T_world_position_x_m: Optional[float] = None,
    max_T_world_position_y_m: Optional[float] = None,
    max_T_world_position_z_m: Optional[float] = None,
    min_T_image_position_x: Optional[float] = None,
    min_T_image_position_y: Optional[float] = None,
    min_T_world_position_x_m: Optional[float] = None,
    min_T_world_position_y_m: Optional[float] = None,
    min_T_world_position_z_m: Optional[float] = None,
) -> MetadataIR:
    """Create new IR metadata."""
    meta_type = crud.reference.get_or_create_metadata_type(
        db, name=MetadataIR.METADATA_TYPE_NAME
    )
    metadata = MetadataIR(
        metadata_subtype="ir",
        type=meta_type.metadata_type_id,
        metadata_type=meta_type,
        span_id=span_id,
        max_T_K=max_T_K,
        avg_T_K=avg_T_K,
        min_T_K=min_T_K,
        max_T_image_position_x=max_T_image_position_x,
        max_T_image_position_y=max_T_image_position_y,
        max_T_world_position_x_m=max_T_world_position_x_m,
        max_T_world_position_y_m=max_T_world_position_y_m,
        max_T_world_position_z_m=max_T_world_position_z_m,
        min_T_image_position_x=min_T_image_position_x,
        min_T_image_position_y=min_T_image_position_y,
        min_T_world_position_x_m=min_T_world_position_x_m,
        min_T_world_position_y_m=min_T_world_position_y_m,
        min_T_world_position_z_m=min_T_world_position_z_m,
    )
    db.add(metadata)
    db.commit()
    db.refresh(metadata)
    return metadata


def get_metadata_ir(db: Session, metadata_id: int) -> Optional[MetadataIR]:
    """Get IR metadata by ID."""
    return db.get(MetadataIR, metadata_id)


def get_metadata_ir_list(
    db: Session,
    span_id: Optional[int] = None,
    skip: int = 0,
    limit: int = 100,
) -> List[MetadataIR]:
    """Get all IR metadata with optional span filter."""
    query = select(MetadataIR)
    if span_id is not None:
        query = query.where(MetadataIR.span_id == span_id)
    return db.execute(query.offset(skip).limit(limit)).scalars().all()


def update_metadata_ir(
    db: Session,
    metadata_id: int,
    max_T_K: Optional[float] = None,
    avg_T_K: Optional[float] = None,
    min_T_K: Optional[float] = None,
    max_T_image_position_x: Optional[float] = None,
    max_T_image_position_y: Optional[float] = None,
    max_T_world_position_x_m: Optional[float] = None,
    max_T_world_position_y_m: Optional[float] = None,
    max_T_world_position_z_m: Optional[float] = None,
    min_T_image_position_x: Optional[float] = None,
    min_T_image_position_y: Optional[float] = None,
    min_T_world_position_x_m: Optional[float] = None,
    min_T_world_position_y_m: Optional[float] = None,
    min_T_world_position_z_m: Optional[float] = None,
) -> Optional[MetadataIR]:
    """Update IR metadata."""
    metadata = get_metadata_ir(db, metadata_id)
    if not metadata:
        return None
    if max_T_K is not None:
        metadata.max_T_K = max_T_K
    if avg_T_K is not None:
        metadata.avg_T_K = avg_T_K
    if min_T_K is not None:
        metadata.min_T_K = min_T_K
    if max_T_image_position_x is not None:
        metadata.max_T_image_position_x = max_T_image_position_x
    if max_T_image_position_y is not None:
        metadata.max_T_image_position_y = max_T_image_position_y
    if max_T_world_position_x_m is not None:
        metadata.max_T_world_position_x_m = max_T_world_position_x_m
    if max_T_world_position_y_m is not None:
        metadata.max_T_world_position_y_m = max_T_world_position_y_m
    if max_T_world_position_z_m is not None:
        metadata.max_T_world_position_z_m = max_T_world_position_z_m
    if min_T_image_position_x is not None:
        metadata.min_T_image_position_x = min_T_image_position_x
    if min_T_image_position_y is not None:
        metadata.min_T_image_position_y = min_T_image_position_y
    if min_T_world_position_x_m is not None:
        metadata.min_T_world_position_x_m = min_T_world_position_x_m
    if min_T_world_position_y_m is not None:
        metadata.min_T_world_position_y_m = min_T_world_position_y_m
    if min_T_world_position_z_m is not None:
        metadata.min_T_world_position_z_m = min_T_world_position_z_m
    db.commit()
    db.refresh(metadata)
    return metadata


# ============================================================================
# MetadataPulse CRUD
# ============================================================================


def create_metadata_pulse(
    db: Session,
    span_id: int,
    max_IP_MA: Optional[float] = None,
    max_P_LH_MW: Optional[float] = None,
    max_P_ICRH_MW: Optional[float] = None,
    max_P_ECRH_MW: Optional[float] = None,
    max_P_NBI_MW: Optional[float] = None,
    max_density: Optional[float] = None,
) -> MetadataPulse:
    """Create new pulse metadata."""
    meta_type = crud.reference.get_or_create_metadata_type(
        db, name=MetadataPulse.METADATA_TYPE_NAME
    )
    metadata = MetadataPulse(
        metadata_subtype="pulse",
        type=meta_type.metadata_type_id,
        metadata_type=meta_type,
        span_id=span_id,
        max_IP_MA=max_IP_MA,
        max_P_LH_MW=max_P_LH_MW,
        max_P_ICRH_MW=max_P_ICRH_MW,
        max_P_ECRH_MW=max_P_ECRH_MW,
        max_P_NBI_MW=max_P_NBI_MW,
        max_density=max_density,
    )
    db.add(metadata)
    db.commit()
    db.refresh(metadata)
    return metadata


def get_metadata_pulse(db: Session, metadata_id: int) -> Optional[MetadataPulse]:
    """Get pulse metadata by ID."""
    return db.get(MetadataPulse, metadata_id)


def get_metadata_pulse_list(
    db: Session,
    span_id: Optional[int] = None,
    skip: int = 0,
    limit: int = 100,
) -> List[MetadataPulse]:
    """Get all pulse metadata with optional span filter."""
    query = select(MetadataPulse)
    if span_id is not None:
        query = query.where(MetadataPulse.span_id == span_id)
    return db.execute(query.offset(skip).limit(limit)).scalars().all()


def update_metadata_pulse(
    db: Session,
    metadata_id: int,
    max_IP_MA: Optional[float] = None,
    max_P_LH_MW: Optional[float] = None,
    max_P_ICRH_MW: Optional[float] = None,
    max_P_ECRH_MW: Optional[float] = None,
    max_P_NBI_MW: Optional[float] = None,
    max_density: Optional[float] = None,
) -> Optional[MetadataPulse]:
    """Update pulse metadata."""
    metadata = get_metadata_pulse(db, metadata_id)
    if not metadata:
        return None
    if max_IP_MA is not None:
        metadata.max_IP_MA = max_IP_MA
    if max_P_LH_MW is not None:
        metadata.max_P_LH_MW = max_P_LH_MW
    if max_P_ICRH_MW is not None:
        metadata.max_P_ICRH_MW = max_P_ICRH_MW
    if max_P_ECRH_MW is not None:
        metadata.max_P_ECRH_MW = max_P_ECRH_MW
    if max_P_NBI_MW is not None:
        metadata.max_P_NBI_MW = max_P_NBI_MW
    if max_density is not None:
        metadata.max_density = max_density
    db.commit()
    db.refresh(metadata)
    return metadata


# ============================================================================
# MetadataStrikeLine CRUD
# ============================================================================


def create_metadata_strike_line(
    db: Session,
    span_id: int,
    angle: Optional[float] = None,
    curvature: Optional[float] = None,
    comment: Optional[str] = None,
    segmented_points: Optional[bytes] = None,
) -> MetadataStrikeLine:
    """Create new strike line metadata."""
    meta_type = crud.reference.get_or_create_metadata_type(
        db, name=MetadataStrikeLine.METADATA_TYPE_NAME
    )
    metadata = MetadataStrikeLine(
        metadata_subtype="strike_line",
        type=meta_type.metadata_type_id,
        metadata_type=meta_type,
        span_id=span_id,
        angle=angle,
        curvature=curvature,
        comment=comment,
        segmented_points=segmented_points,
    )
    db.add(metadata)
    db.commit()
    db.refresh(metadata)
    return metadata


def get_metadata_strike_line(
    db: Session, metadata_id: int
) -> Optional[MetadataStrikeLine]:
    """Get strike line metadata by ID."""
    return db.get(MetadataStrikeLine, metadata_id)


def get_metadata_strike_line_list(
    db: Session,
    span_id: Optional[int] = None,
    skip: int = 0,
    limit: int = 100,
) -> List[MetadataStrikeLine]:
    """Get all strike line metadata with optional span filter."""
    query = select(MetadataStrikeLine)
    if span_id is not None:
        query = query.where(MetadataStrikeLine.span_id == span_id)
    return db.execute(query.offset(skip).limit(limit)).scalars().all()


def update_metadata_strike_line(
    db: Session,
    metadata_id: int,
    angle: Optional[float] = None,
    curvature: Optional[float] = None,
    comment: Optional[str] = None,
    segmented_points: Optional[bytes] = None,
) -> Optional[MetadataStrikeLine]:
    """Update strike line metadata."""
    metadata = get_metadata_strike_line(db, metadata_id)
    if not metadata:
        return None
    if angle is not None:
        metadata.angle = angle
    if curvature is not None:
        metadata.curvature = curvature
    if comment is not None:
        metadata.comment = comment
    if segmented_points is not None:
        metadata.segmented_points = segmented_points
    db.commit()
    db.refresh(metadata)
    return metadata
