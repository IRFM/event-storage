from typing import Dict, List, Optional

from sqlalchemy import select
from sqlalchemy.orm import Session

from event_storage.models.associations import dataset_span_association
from event_storage.models.dataset import Dataset
from event_storage.models.event import EventSpan


def create_dataset(
    db: Session, name: str, description: Optional[str] = None
) -> Dataset:
    dataset = Dataset(name=name, description=description)
    db.add(dataset)
    db.commit()
    db.refresh(dataset)
    return dataset


def get_dataset(db: Session, dataset_id: int) -> Optional[Dataset]:
    return db.get(Dataset, dataset_id)


def get_or_create_dataset(db: Session, name: str, description: str) -> Dataset:
    dataset = db.execute(
        select(Dataset).where(Dataset.name == name)
    ).scalar_one_or_none()
    if not dataset:
        dataset = create_dataset(db, name=name, description=description)
    return dataset


def get_datasets(db: Session, skip: int = 0, limit: int = 100) -> List[Dataset]:
    return db.execute(select(Dataset).offset(skip).limit(limit)).scalars().all()


def update_dataset(
    db: Session,
    dataset_id: int,
    name: Optional[str] = None,
    description: Optional[str] = None,
) -> Optional[Dataset]:
    dataset = get_dataset(db, dataset_id)
    if not dataset:
        return None
    if name is not None:
        dataset.name = name
    if description is not None:
        dataset.description = description
    db.commit()
    db.refresh(dataset)
    return dataset


def delete_dataset(db: Session, dataset_id: int) -> bool:
    dataset = get_dataset(db, dataset_id)
    if not dataset:
        return False
    db.delete(dataset)
    db.commit()
    return True


def add_event_span_to_dataset(db: Session, dataset_id: int, event_span_id: int) -> bool:
    dataset = get_dataset(db, dataset_id)
    span = db.get(EventSpan, event_span_id)
    if not dataset or not span:
        return False
    dataset.spans.append(span)
    db.commit()
    return True


def remove_event_span_from_dataset(
    db: Session, dataset_id: int, event_span_id: int
) -> bool:
    dataset = get_dataset(db, dataset_id)
    span = db.get(EventSpan, event_span_id)
    if not dataset or not span:
        return False
    if span not in dataset.spans:
        return False
    dataset.spans.remove(span)
    db.commit()
    return True


def get_dataset_event_spans(
    db: Session, dataset_id: int, skip: int = 0, limit: int = 100
) -> List[EventSpan]:
    dataset = get_dataset(db, dataset_id)
    if not dataset:
        return []
    return list(dataset.spans)[skip : skip + limit]


def get_event_spans_not_in_dataset(
    db: Session, dataset_id: int, skip: int = 0, limit: int = 100
) -> List[EventSpan]:
    """
    Get event spans not associated with the given dataset.
    Uses a subquery instead of loading all dataset spans into memory (fix #9).
    """
    subq = (
        select(dataset_span_association.c.span_id)
        .where(dataset_span_association.c.dataset_id == dataset_id)
        .scalar_subquery()
    )
    return (
        db.execute(
            select(EventSpan)
            .where(EventSpan.span_id.notin_(subq))
            .offset(skip)
            .limit(limit)
        )
        .scalars()
        .all()
    )


def get_dataset_summary(db: Session, dataset_id: int) -> Optional[Dict]:
    dataset = get_dataset(db, dataset_id)
    if not dataset:
        return None
    return {
        "dataset_id": dataset.dataset_id,
        "name": dataset.name,
        "description": dataset.description,
        "created_at": dataset.created_at,
        "updated_at": dataset.updated_at,
        "event_span_count": len(dataset.spans),
    }
