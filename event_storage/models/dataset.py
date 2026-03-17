from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING, Optional

from sqlalchemy import DateTime, Index, Integer, String, Text
from sqlalchemy.orm import Mapped, mapped_column, relationship
from sqlalchemy.sql import func

from event_storage import Base
from event_storage.models.associations import dataset_span_association

if TYPE_CHECKING:
    from event_storage.models.event import EventSpan


class Dataset(Base):
    """Dataset for training machine learning models."""

    __tablename__ = "Dataset"
    __table_args__ = (
        Index("idx_dataset_name", "name"),
        {"mysql_engine": "InnoDB"},
    )

    dataset_id: Mapped[int] = mapped_column(
        Integer, primary_key=True, autoincrement=True
    )
    name: Mapped[str] = mapped_column(String(100), nullable=False)
    description: Mapped[Optional[str]] = mapped_column(Text)
    created_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime, server_default=func.now()
    )
    updated_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime, onupdate=func.now()
    )

    spans: Mapped[list["EventSpan"]] = relationship(
        secondary=dataset_span_association,
        back_populates="datasets",
    )
