import json
from datetime import datetime
from typing import TYPE_CHECKING, Optional

from sqlalchemy import (
    BigInteger,
    CheckConstraint,
    DateTime,
    Float,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
)
from sqlalchemy.orm import Mapped, mapped_column, relationship
from sqlalchemy.sql import func

from event_storage import Base
from event_storage.models.associations import dataset_span_association

if TYPE_CHECKING:
    from event_storage.models.annotation import Annotation
    from event_storage.models.dataset import Dataset
    from event_storage.models.metadata import Metadata
    from event_storage.models.reference import (
        Category,
        Criticality,
        Method,
        Signal,
        Status,
    )


class Event(Base):
    """Main event table representing detected or annotated events."""

    __tablename__ = "Event"

    event_id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    user_id: Mapped[Optional[str]] = mapped_column(String(255))
    created_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime, server_default=func.now()
    )
    description: Mapped[Optional[str]] = mapped_column(Text)
    category_id: Mapped[Optional[int]] = mapped_column(
        Integer, ForeignKey("Category.category_id")
    )
    criticality_id: Mapped[Optional[int]] = mapped_column(
        Integer, ForeignKey("Criticality.criticality_id")
    )
    status_id: Mapped[Optional[int]] = mapped_column(
        Integer, ForeignKey("Status.status_id")
    )

    criticality: Mapped[Optional["Criticality"]] = relationship(
        back_populates="events", cascade="none"
    )
    category: Mapped[Optional["Category"]] = relationship(
        back_populates="events", cascade="none"
    )
    status: Mapped[Optional["Status"]] = relationship(
        back_populates="events", cascade="none"
    )
    event_spans: Mapped[list["EventSpan"]] = relationship(
        back_populates="event",
        cascade="all, delete-orphan",
        passive_deletes=True,
    )

    __table_args__ = (
        Index("idx_event_criticality", "criticality_id"),
        Index("idx_event_category", "category_id"),
        Index("idx_event_status", "status_id"),
        Index("idx_event_time", "created_at"),
        Index("idx_event_user", "user_id"),
        {"mysql_engine": "InnoDB"},
    )

    def to_dict(self, include_spans: bool = True) -> dict:
        data = {
            "event_id": self.event_id,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "user_id": self.user_id,
            "description": self.description,
            "criticality": self.criticality.to_dict() if self.criticality else None,
            "category": self.category.to_dict() if self.category else None,
            "status": self.status.to_dict() if self.status else None,
        }

        if include_spans and self.event_spans:
            data["spans"] = [span.to_dict() for span in self.event_spans]
            data["span_count"] = len(self.event_spans)
            data["initial_timestamp_ns"] = min(
                s.start_time_ns for s in self.event_spans
            )
            data["final_timestamp_ns"] = max(s.end_time_ns for s in self.event_spans)
            data["total_duration_ns"] = (
                data["final_timestamp_ns"] - data["initial_timestamp_ns"]
            )

        return data

    def to_json(self, filepath: str, **kwargs) -> None:
        with open(filepath, "w") as f:
            json.dump(self.to_dict(**kwargs), f, indent=2)

    @classmethod
    def write_events_to_json(cls, path_to_json: str, events: list, **kwargs) -> None:
        out_dict = {idx: event.to_dict(**kwargs) for idx, event in enumerate(events)}
        with open(path_to_json, "w") as f:
            json.dump(out_dict, f, indent=2)


class EventSpan(Base):
    """Time span of an event on a specific signal with annotation."""

    __tablename__ = "EventSpan"

    span_id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    event_id: Mapped[Optional[int]] = mapped_column(
        Integer, ForeignKey("Event.event_id", ondelete="CASCADE")
    )
    signal_id: Mapped[Optional[int]] = mapped_column(
        Integer, ForeignKey("Signal.signal_id")
    )
    start_time_ns: Mapped[int] = mapped_column(BigInteger, nullable=False)
    end_time_ns: Mapped[int] = mapped_column(BigInteger, nullable=False)
    method_id: Mapped[Optional[int]] = mapped_column(
        Integer, ForeignKey("Method.method_id")
    )
    confidence: Mapped[Optional[float]] = mapped_column(Float)

    # Denormalized fields
    experiment_id: Mapped[Optional[int]] = mapped_column(BigInteger)
    diagnostic: Mapped[Optional[str]] = mapped_column(Text)
    category_id: Mapped[Optional[int]] = mapped_column(
        Integer, ForeignKey("Category.category_id")
    )
    method_name: Mapped[Optional[str]] = mapped_column(String(255))

    event: Mapped[Optional["Event"]] = relationship(
        back_populates="event_spans", passive_deletes=True
    )
    signal: Mapped[Optional["Signal"]] = relationship(
        back_populates="event_spans", cascade="none"
    )
    annotation: Mapped[Optional["Annotation"]] = relationship(
        back_populates="event_span",
        cascade="all, delete-orphan",
        passive_deletes=True,
        uselist=False,
    )
    span_metadata: Mapped[list["Metadata"]] = relationship(
        back_populates="span",
        cascade="all, delete-orphan",
        passive_deletes=True,
    )
    method: Mapped[Optional["Method"]] = relationship(
        back_populates="event_spans", cascade="none"
    )
    datasets: Mapped[list["Dataset"]] = relationship(
        secondary=dataset_span_association,
        back_populates="spans",
    )

    __table_args__ = (
        Index("idx_span_event", "event_id"),
        Index("idx_span_signal", "signal_id"),
        Index("idx_span_method", "method_id"),
        Index("idx_span_experiment", "experiment_id"),
        Index("idx_span_category", "category_id"),
        Index("idx_span_time", "start_time_ns", "end_time_ns"),
        CheckConstraint("start_time_ns <= end_time_ns", name="validate_span_times"),
        {"mysql_engine": "InnoDB"},
    )

    def to_dict(self) -> dict:
        return {
            "span_id": self.span_id,
            "signal_id": self.signal_id,
            "start_time_ns": self.start_time_ns,
            "end_time_ns": self.end_time_ns,
            "duration_ns": self.end_time_ns - self.start_time_ns,
            "confidence": self.confidence,
            "experiment_id": self.experiment_id,
            "diagnostic": self.diagnostic,
            "category_id": self.category_id,
            "method_name": self.method_name,
            "signal": (
                self.signal.to_dict()
                if self.signal is not None
                else ({"signal_id": self.signal_id} if self.signal_id else None)
            ),
            "method": (
                self.method.to_dict()
                if self.method is not None
                else ({"method_id": self.method_id} if self.method_id else None)
            ),
            "annotation": self.annotation.to_dict() if self.annotation else None,
            "metadata": [m.to_dict() for m in self.span_metadata],
        }
