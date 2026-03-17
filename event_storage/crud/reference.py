from typing import List, Optional

from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from event_storage.models.reference import (
    AnnotationType,
    Category,
    Criticality,
    MetadataType,
    Method,
    Signal,
    Status,
)


def _get_or_create(db: Session, get_fn, create_fn, **kwargs):
    """
    Generic race-condition-safe get-or-create helper.
    Attempts a SELECT, then an INSERT. If the INSERT fails with an
    IntegrityError (concurrent writer beat us to it), rolls back the
    failed insert and re-fetches.
    """
    obj = get_fn(db, **kwargs)
    if obj:
        return obj
    try:
        obj = create_fn(db, **kwargs)
        return obj
    except IntegrityError:
        db.rollback()
        return get_fn(db, **kwargs)


# ============================================================================
# Signal CRUD
# ============================================================================


def create_signal(
    db: Session,
    machine: str,
    experiment_id: int,
    diagnostic: str,
    type: str,
    path: str,
) -> Signal:
    """Create a new signal."""
    signal = Signal(
        machine=machine,
        experiment_id=experiment_id,
        diagnostic=diagnostic,
        type=type,
        path=path,
    )
    db.add(signal)
    db.commit()
    db.refresh(signal)
    return signal


def get_signal(db: Session, signal_id: int) -> Optional[Signal]:
    """Get a signal by ID."""
    return db.get(Signal, signal_id)


def get_signals_by_experiment(
    db: Session,
    machine: str,
    experiment_id: int,
) -> List[Signal]:
    """Get all signals for a given machine and experiment."""
    return (
        db.execute(
            select(Signal).where(
                Signal.machine == machine, Signal.experiment_id == experiment_id
            )
        )
        .scalars()
        .all()
    )


def get_signal_by_experiment_and_diagnostic(
    db: Session,
    machine: str,
    experiment_id: int,
    diagnostic: str,
) -> Optional[Signal]:
    """Get a signal by machine, experiment_id and diagnostic."""
    return db.execute(
        select(Signal).where(
            Signal.machine == machine,
            Signal.experiment_id == experiment_id,
            Signal.diagnostic == diagnostic,
        )
    ).scalar_one_or_none()


def update_signal(
    db: Session,
    signal_id: int,
    type: Optional[str] = None,
    path: Optional[str] = None,
) -> Optional[Signal]:
    """Update a signal."""
    signal = get_signal(db, signal_id)
    if not signal:
        return None
    if type is not None:
        signal.type = type
    if path is not None:
        signal.path = path
    db.commit()
    db.refresh(signal)
    return signal


def delete_signal(db: Session, signal_id: int) -> bool:
    """Delete a signal."""
    signal = get_signal(db, signal_id)
    if not signal:
        return False
    db.delete(signal)
    db.commit()
    return True


def get_or_create_signal(
    db: Session,
    machine: str,
    experiment_id: int,
    diagnostic: str,
    type: str,
    path: str,
) -> Signal:
    """
    Get a signal matching (machine, experiment_id, diagnostic) — the actual
    unique constraint — or create it if it does not exist.
    """

    def _get(db, **kw):
        return get_signal_by_experiment_and_diagnostic(
            db, kw["machine"], kw["experiment_id"], kw["diagnostic"]
        )

    def _create(db, **kw):
        return create_signal(db, **kw)

    return _get_or_create(
        db,
        _get,
        _create,
        machine=machine,
        experiment_id=experiment_id,
        diagnostic=diagnostic,
        type=type,
        path=path,
    )


# ============================================================================
# AnnotationType CRUD
# ============================================================================


def create_annotation_type(
    db: Session, name: str, description: Optional[str] = None
) -> AnnotationType:
    annotation_type = AnnotationType(name=name, description=description)
    db.add(annotation_type)
    db.commit()
    db.refresh(annotation_type)
    return annotation_type


def get_annotation_type(db: Session, type_id: int) -> Optional[AnnotationType]:
    return db.get(AnnotationType, type_id)


def get_annotation_type_by_name(db: Session, name: str) -> Optional[AnnotationType]:
    return db.execute(
        select(AnnotationType).where(AnnotationType.name == name)
    ).scalar_one_or_none()


def get_annotation_types(db: Session) -> List[AnnotationType]:
    return db.execute(select(AnnotationType)).scalars().all()


def get_or_create_annotation_type(
    db: Session, name: str, description: Optional[str] = None
) -> AnnotationType:
    return _get_or_create(
        db,
        lambda db, **kw: get_annotation_type_by_name(db, kw["name"]),
        lambda db, **kw: create_annotation_type(db, **kw),
        name=name,
        description=description,
    )


# ============================================================================
# MetadataType CRUD
# ============================================================================


def create_metadata_type(
    db: Session, name: str, description: Optional[str] = None
) -> MetadataType:
    metadata_type = MetadataType(name=name, description=description)
    db.add(metadata_type)
    db.commit()
    db.refresh(metadata_type)
    return metadata_type


def get_metadata_type(db: Session, metadata_type_id: int) -> Optional[MetadataType]:
    return db.get(MetadataType, metadata_type_id)


def get_metadata_type_by_name(db: Session, name: str) -> Optional[MetadataType]:
    return db.execute(
        select(MetadataType).where(MetadataType.name == name)
    ).scalar_one_or_none()


def get_metadata_types(db: Session) -> List[MetadataType]:
    return db.execute(select(MetadataType)).scalars().all()


def get_or_create_metadata_type(
    db: Session, name: str, description: Optional[str] = None
) -> MetadataType:
    return _get_or_create(
        db,
        lambda db, **kw: get_metadata_type_by_name(db, kw["name"]),
        lambda db, **kw: create_metadata_type(db, **kw),
        name=name,
        description=description,
    )


# ============================================================================
# Criticality CRUD
# ============================================================================


def create_criticality(
    db: Session, level: str, description: Optional[str] = None
) -> Criticality:
    criticality = Criticality(level=level, description=description)
    db.add(criticality)
    db.commit()
    db.refresh(criticality)
    return criticality


def get_criticality(db: Session, criticality_id: int) -> Optional[Criticality]:
    return db.get(Criticality, criticality_id)


def get_criticality_by_level(db: Session, level: str) -> Optional[Criticality]:
    return db.execute(
        select(Criticality).where(Criticality.level == level)
    ).scalar_one_or_none()


def get_criticalities(db: Session) -> List[Criticality]:
    return db.execute(select(Criticality)).scalars().all()


def get_or_create_criticality(
    db: Session, level: str, description: Optional[str] = None
) -> Criticality:
    return _get_or_create(
        db,
        lambda db, **kw: get_criticality_by_level(db, kw["level"]),
        lambda db, **kw: create_criticality(db, **kw),
        level=level,
        description=description,
    )


# ============================================================================
# Category CRUD
# ============================================================================


def create_category(
    db: Session, name: str, description: Optional[str] = None
) -> Category:
    category = Category(name=name, description=description)
    db.add(category)
    db.commit()
    db.refresh(category)
    return category


def get_category(db: Session, category_id: int) -> Optional[Category]:
    return db.get(Category, category_id)


def get_category_by_name(db: Session, name: str) -> Optional[Category]:
    return db.execute(
        select(Category).where(Category.name == name)
    ).scalar_one_or_none()


def get_categories(db: Session) -> List[Category]:
    return db.execute(select(Category)).scalars().all()


def get_or_create_category(
    db: Session, name: str, description: Optional[str] = None
) -> Category:
    return _get_or_create(
        db,
        lambda db, **kw: get_category_by_name(db, kw["name"]),
        lambda db, **kw: create_category(db, **kw),
        name=name,
        description=description,
    )


# ============================================================================
# Method CRUD
# ============================================================================


def create_method(
    db: Session,
    name: str,
    is_manual: bool = False,
    description: Optional[str] = None,
    configuration: Optional[dict] = None,
) -> Method:
    method = Method(
        name=name,
        is_manual=is_manual,
        description=description,
        configuration=configuration,
    )
    db.add(method)
    db.commit()
    db.refresh(method)
    return method


def get_method(db: Session, method_id: int) -> Optional[Method]:
    return db.get(Method, method_id)


def get_method_by_name(db: Session, name: str) -> Optional[Method]:
    return db.execute(select(Method).where(Method.name == name)).scalar_one_or_none()


def get_method_by_name_and_config(
    db: Session, name: str, configuration: Optional[dict] = None
) -> Optional[Method]:
    methods = db.execute(select(Method).where(Method.name == name)).scalars().all()
    for method in methods:
        if method.configuration == configuration:
            return method
    return None


def get_methods(db: Session) -> List[Method]:
    return db.execute(select(Method)).scalars().all()


def get_or_create_method(
    db: Session,
    name: str,
    is_manual: bool = False,
    description: Optional[str] = None,
    configuration: Optional[dict] = None,
) -> Method:
    method = get_method_by_name_and_config(db, name=name, configuration=configuration)
    if not method:
        method = create_method(
            db,
            name=name,
            is_manual=is_manual,
            description=description,
            configuration=configuration,
        )
    return method


# ============================================================================
# Status CRUD
# ============================================================================


def create_status(db: Session, name: str, description: Optional[str] = None) -> Status:
    status = Status(name=name, description=description)
    db.add(status)
    db.commit()
    db.refresh(status)
    return status


def get_status(db: Session, status_id: int) -> Optional[Status]:
    return db.get(Status, status_id)


def get_status_by_name(db: Session, name: str) -> Optional[Status]:
    return db.execute(select(Status).where(Status.name == name)).scalar_one_or_none()


def get_statuses(db: Session) -> List[Status]:
    return db.execute(select(Status)).scalars().all()


def get_or_create_status(
    db: Session, name: str, description: Optional[str] = None
) -> Status:
    return _get_or_create(
        db,
        lambda db, **kw: get_status_by_name(db, kw["name"]),
        lambda db, **kw: create_status(db, **kw),
        name=name,
        description=description,
    )


# ============================================================================
# Initialize Reference Data
# ============================================================================


def initialize_reference_data(db: Session) -> None:
    """Initialize all reference tables with default data."""
    if not get_annotation_type_by_name(db, "2D"):
        create_annotation_type(db, "2D", "2D bounding boxes, polygons, and masks")
    if not get_annotation_type_by_name(db, "3D"):
        create_annotation_type(db, "3D", "3D bounding boxes, polygons, and masks")

    if not get_metadata_type_by_name(db, "IR"):
        create_metadata_type(db, "IR", "Infrared data")
    if not get_metadata_type_by_name(db, "Pulse"):
        create_metadata_type(db, "Pulse", "Plasma state data")
    if not get_metadata_type_by_name(db, "StrikeLine"):
        create_metadata_type(db, "StrikeLine", "Strike line description")

    if not get_criticality_by_level(db, "Low"):
        create_criticality(db, "Low", "Low priority event")
    if not get_criticality_by_level(db, "Medium"):
        create_criticality(db, "Medium", "Medium priority event")
    if not get_criticality_by_level(db, "High"):
        create_criticality(db, "High", "High priority event")

    if not get_category_by_name(db, "ufo"):
        create_category(db, "ufo", "Moving particle in the vessel")
    if not get_category_by_name(db, "hotspot"):
        create_category(db, "hotspot", "Localised overheating")
    if not get_category_by_name(db, "arcing"):
        create_category(db, "arcing", "Electrical arcing event")
    if not get_category_by_name(db, "strike line"):
        create_category(db, "strike line", "Plasma strike line")
    if not get_category_by_name(db, "unknown"):
        create_category(db, "unknown", "Uncategorised event")

    if not get_method_by_name(db, "Manual"):
        create_method(db, name="Manual", is_manual=True, description="Human annotation")
    if not get_method_by_name(db, "Automatic"):
        create_method(
            db, name="Automatic", is_manual=False, description="Algorithm detection"
        )
    if not get_method_by_name(db, "Auto-Validated"):
        create_method(
            db,
            name="Auto-Validated",
            is_manual=True,
            description="Algorithm detection with human validation",
        )

    if not get_status_by_name(db, "New"):
        create_status(db, "New", "Newly created event")
    if not get_status_by_name(db, "Reviewed"):
        create_status(db, "Reviewed", "Event has been reviewed")
    if not get_status_by_name(db, "Validated"):
        create_status(db, "Validated", "Event has been validated")

    print("Reference data initialized successfully!")
