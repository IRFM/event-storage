from typing import TYPE_CHECKING, Optional

from sqlalchemy import (
    JSON,
    BigInteger,
    Boolean,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy.orm import Mapped, mapped_column, relationship

from event_storage import Base

if TYPE_CHECKING:
    from event_storage.models.annotation import Annotation
    from event_storage.models.event import Event, EventSpan
    from event_storage.models.metadata import Metadata


class Signal(Base):
    """Signal/data stream from a data source."""

    __tablename__ = "Signal"

    signal_id: Mapped[int] = mapped_column(
        Integer, primary_key=True, autoincrement=True
    )
    machine: Mapped[str] = mapped_column(String(255), nullable=False)
    experiment_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    diagnostic: Mapped[Optional[str]] = mapped_column(String(255))
    type: Mapped[str] = mapped_column(String(255), nullable=False)
    path: Mapped[str] = mapped_column(Text, nullable=False)

    # Relationships
    event_spans: Mapped[list["EventSpan"]] = relationship(back_populates="signal")

    __table_args__ = (
        Index("idx_signal_experiment", "experiment_id"),
        Index("idx_signal_machine", "machine"),
        Index("idx_signal_diagnostic", "diagnostic"),
        UniqueConstraint(
            "machine", "experiment_id", "diagnostic", name="uq_signal_identity"
        ),
        {"mysql_engine": "InnoDB"},
    )

    def to_dict(self) -> dict:
        return {
            "signal_id": self.signal_id,
            "machine": self.machine,
            "experiment_id": self.experiment_id,
            "diagnostic": self.diagnostic,
            "type": self.type,
            "path": self.path,
        }


class AnnotationType(Base):
    """Reference table for annotation types."""

    __tablename__ = "AnnotationType"

    type_id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String(255), nullable=False, unique=True)
    description: Mapped[Optional[str]] = mapped_column(Text)

    annotations: Mapped[list["Annotation"]] = relationship(
        back_populates="annotation_type"
    )

    def to_dict(self) -> dict:
        return {
            "type_id": self.type_id,
            "name": self.name,
            "description": self.description,
        }


class MetadataType(Base):
    """Reference table for metadata types."""

    __tablename__ = "MetadataType"

    metadata_type_id: Mapped[int] = mapped_column(
        Integer, primary_key=True, autoincrement=True
    )
    name: Mapped[str] = mapped_column(String(255), nullable=False, unique=True)
    description: Mapped[Optional[str]] = mapped_column(Text)

    metadata_entries: Mapped[list["Metadata"]] = relationship(
        back_populates="metadata_type"
    )

    def to_dict(self) -> dict:
        return {
            "metadata_type_id": self.metadata_type_id,
            "name": self.name,
            "description": self.description,
        }


class Criticality(Base):
    """Reference table for event criticality levels."""

    __tablename__ = "Criticality"

    criticality_id: Mapped[int] = mapped_column(
        Integer, primary_key=True, autoincrement=True
    )
    level: Mapped[str] = mapped_column(String(255), nullable=False, unique=True)
    description: Mapped[Optional[str]] = mapped_column(Text)

    events: Mapped[list["Event"]] = relationship(back_populates="criticality")

    def to_dict(self) -> dict:
        return {
            "criticality_id": self.criticality_id,
            "level": self.level,
            "description": self.description,
        }


class Method(Base):
    """Reference table for annotation/detection methods."""

    __tablename__ = "Method"

    method_id: Mapped[int] = mapped_column(
        Integer, primary_key=True, autoincrement=True
    )
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    is_manual: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    description: Mapped[Optional[str]] = mapped_column(Text)
    configuration: Mapped[Optional[dict]] = mapped_column(JSON)

    event_spans: Mapped[list["EventSpan"]] = relationship(back_populates="method")

    def to_dict(self) -> dict:
        return {
            "method_id": self.method_id,
            "name": self.name,
            "description": self.description,
            "configuration": self.configuration,
        }


class Status(Base):
    """Reference table for event status."""

    __tablename__ = "Status"

    status_id: Mapped[int] = mapped_column(
        Integer, primary_key=True, autoincrement=True
    )
    name: Mapped[str] = mapped_column(String(255), nullable=False, unique=True)
    description: Mapped[Optional[str]] = mapped_column(Text)

    events: Mapped[list["Event"]] = relationship(back_populates="status")

    def to_dict(self) -> dict:
        return {
            "status_id": self.status_id,
            "name": self.name,
            "description": self.description,
        }


class Category(Base):
    """Reference table for event categories."""

    __tablename__ = "Category"

    category_id: Mapped[int] = mapped_column(
        Integer, primary_key=True, autoincrement=True
    )
    name: Mapped[str] = mapped_column(String(255), nullable=False, unique=True)
    description: Mapped[Optional[str]] = mapped_column(Text)

    events: Mapped[list["Event"]] = relationship(back_populates="category")

    def to_dict(self) -> dict:
        return {
            "category_id": self.category_id,
            "name": self.name,
            "description": self.description,
        }
