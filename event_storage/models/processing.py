import enum
from datetime import datetime
from typing import TYPE_CHECKING, Optional

from sqlalchemy import (
    JSON,
    DateTime,
    ForeignKey,
    Index,
    Integer,
)
from sqlalchemy import Enum as SQLAlchemyEnum
from sqlalchemy.orm import Mapped, mapped_column, relationship
from sqlalchemy.sql import func

from event_storage import Base
from event_storage.models.reference import Method, Signal

if TYPE_CHECKING:
    from event_storage.models.reference import Method, Signal


class ProcessingStatusEnum(str, enum.Enum):
    pending = "pending"
    processing = "processing"
    completed = "completed"
    failed = "failed"


class ProcessingStatus(Base):
    """Track processing status of signals for different methods."""

    __tablename__ = "ProcessingStatus"
    __table_args__ = (
        Index("idx_processing_signal_method", "signal_id", "method_id", unique=True),
        Index("idx_processing_status", "status"),
        Index("idx_processing_method", "method_id"),
        {"mysql_engine": "InnoDB"},
    )

    status_id: Mapped[int] = mapped_column(
        Integer, primary_key=True, autoincrement=True
    )
    signal_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("Signal.signal_id"), nullable=False
    )
    method_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("Method.method_id"), nullable=False
    )
    processed_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime, server_default=func.now(), onupdate=func.now()
    )
    status: Mapped[ProcessingStatusEnum] = mapped_column(
        SQLAlchemyEnum(ProcessingStatusEnum),
        nullable=False,
        server_default=ProcessingStatusEnum.pending.value,
    )
    processing_details: Mapped[Optional[dict]] = mapped_column(JSON)

    signal: Mapped["Signal"] = relationship(back_populates="processing_statuses")
    method: Mapped["Method"] = relationship(back_populates="processing_statuses")


Signal.processing_statuses = relationship("ProcessingStatus", back_populates="signal")
Method.processing_statuses = relationship("ProcessingStatus", back_populates="method")
