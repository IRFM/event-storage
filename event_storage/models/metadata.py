from typing import TYPE_CHECKING, Optional

from sqlalchemy import Float, ForeignKey, Index, Integer, LargeBinary, String, Text
from sqlalchemy.orm import Mapped, mapped_column, relationship

from event_storage import Base

if TYPE_CHECKING:
    from event_storage.models.event import EventSpan
    from event_storage.models.reference import MetadataType


class Metadata(Base):
    """Base metadata table with polymorphic inheritance."""

    __tablename__ = "Metadata"

    metadata_id: Mapped[int] = mapped_column(
        Integer, primary_key=True, autoincrement=True
    )
    span_id: Mapped[Optional[int]] = mapped_column(
        Integer, ForeignKey("EventSpan.span_id", ondelete="CASCADE")
    )
    type: Mapped[Optional[int]] = mapped_column(
        Integer, ForeignKey("MetadataType.metadata_type_id")
    )
    metadata_subtype: Mapped[Optional[str]] = mapped_column(String(50))

    metadata_type: Mapped[Optional["MetadataType"]] = relationship(
        back_populates="metadata_entries", cascade="none"
    )
    span: Mapped[Optional["EventSpan"]] = relationship(
        back_populates="span_metadata", passive_deletes=True, cascade="none"
    )

    __mapper_args__ = {
        "polymorphic_identity": "metadata",
        "polymorphic_on": "metadata_subtype",
        "passive_deletes": True,
    }

    def to_dict(self) -> dict:
        return {
            "metadata_id": self.metadata_id,
            "type": (
                self.metadata_type.name
                if self.metadata_type is not None
                else ({"type_id": self.type} if self.type else None)
            ),
            "span_id": self.span_id,
        }


Index("idx_metadata_span", Metadata.__table__.c.span_id)
Index("idx_metadata_type", Metadata.__table__.c.type)


class MetadataIR(Metadata):
    """Infrared spectral metadata."""

    __tablename__ = "MetadataIR"

    METADATA_TYPE_NAME = "IR"

    metadata_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("Metadata.metadata_id", ondelete="CASCADE"),
        primary_key=True,
    )
    max_T_K: Mapped[float] = mapped_column(Float, nullable=False)
    max_T_image_position_x: Mapped[Optional[float]] = mapped_column(Float)
    max_T_image_position_y: Mapped[Optional[float]] = mapped_column(Float)
    max_T_world_position_x_m: Mapped[Optional[float]] = mapped_column(Float)
    max_T_world_position_y_m: Mapped[Optional[float]] = mapped_column(Float)
    max_T_world_position_z_m: Mapped[Optional[float]] = mapped_column(Float)
    avg_T_K: Mapped[float] = mapped_column(Float, nullable=False)
    min_T_K: Mapped[float] = mapped_column(Float, nullable=False)
    min_T_image_position_x: Mapped[Optional[float]] = mapped_column(Float)
    min_T_image_position_y: Mapped[Optional[float]] = mapped_column(Float)
    min_T_world_position_x_m: Mapped[Optional[float]] = mapped_column(Float)
    min_T_world_position_y_m: Mapped[Optional[float]] = mapped_column(Float)
    min_T_world_position_z_m: Mapped[Optional[float]] = mapped_column(Float)
    component: Mapped[Optional[str]] = mapped_column(Text)
    emissivity: Mapped[Optional[float]] = mapped_column(Float)

    __mapper_args__ = {"polymorphic_identity": "ir"}

    def to_dict(self) -> dict:
        return {
            "metadata_id": self.metadata_id,
            "type": "IR",
            "max_T_K": self.max_T_K,
            "max_T_image_position_x": self.max_T_image_position_x,
            "max_T_image_position_y": self.max_T_image_position_y,
            "max_T_world_position_x_m": self.max_T_world_position_x_m,
            "max_T_world_position_y_m": self.max_T_world_position_y_m,
            "max_T_world_position_z_m": self.max_T_world_position_z_m,
            "avg_T_K": self.avg_T_K,
            "min_T_K": self.min_T_K,
            "min_T_image_position_x": self.min_T_image_position_x,
            "min_T_image_position_y": self.min_T_image_position_y,
            "min_T_world_position_x_m": self.min_T_world_position_x_m,
            "min_T_world_position_y_m": self.min_T_world_position_y_m,
            "min_T_world_position_z_m": self.min_T_world_position_z_m,
            "component": self.component,
            "emissivity": self.emissivity,
        }


class MetadataPulse(Metadata):
    """Plasma state metadata."""

    __tablename__ = "MetadataPulse"

    METADATA_TYPE_NAME = "Pulse"

    metadata_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("Metadata.metadata_id", ondelete="CASCADE"),
        primary_key=True,
    )
    max_IP_MA: Mapped[Optional[float]] = mapped_column(
        Float, comment="Max plasma current in MA"
    )
    max_P_LH_MW: Mapped[Optional[float]] = mapped_column(
        Float, comment="Max power injected by the LH system in MW"
    )
    max_P_ICRH_MW: Mapped[Optional[float]] = mapped_column(
        Float, comment="Max power injected by the ICRH system in MW"
    )
    max_P_ECRH_MW: Mapped[Optional[float]] = mapped_column(
        Float, comment="Max power injected by the ECRH system in MW"
    )
    max_P_NBI_MW: Mapped[Optional[float]] = mapped_column(
        Float, comment="Max power injected by the NBI system in MW"
    )
    max_density: Mapped[Optional[float]] = mapped_column(
        Float, comment="Max density in 1e9 particles/m3"
    )

    __mapper_args__ = {"polymorphic_identity": "pulse"}

    def to_dict(self) -> dict:
        return {
            "metadata_id": self.metadata_id,
            "type": "Pulse",
            "max_IP_MA": self.max_IP_MA,
            "max_density": self.max_density,
            "max_P_LH_MW": self.max_P_LH_MW,
            "max_P_ICRH_MW": self.max_P_ICRH_MW,
            "max_P_ECRH_MW": self.max_P_ECRH_MW,
            "max_P_NBI_MW": self.max_P_NBI_MW,
        }


class MetadataStrikeLine(Metadata):
    """Strike line metadata."""

    __tablename__ = "MetadataStrikeLine"

    METADATA_TYPE_NAME = "StrikeLine"

    metadata_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("Metadata.metadata_id", ondelete="CASCADE"),
        primary_key=True,
    )
    angle: Mapped[Optional[float]] = mapped_column(
        Float, comment="Angle of the strike line, in degrees"
    )
    curvature: Mapped[Optional[float]] = mapped_column(
        Float, comment="Curvature of the strike line (categorical)"
    )
    comment: Mapped[Optional[str]] = mapped_column(
        Text, comment="Comments describing the strike line"
    )
    segmented_points: Mapped[Optional[bytes]] = mapped_column(
        LargeBinary, comment="Segmented points in the cropped image of the strike line"
    )

    __mapper_args__ = {"polymorphic_identity": "strike_line"}

    def to_dict(self) -> dict:
        return {
            "metadata_id": self.metadata_id,
            "type": "StrikeLine",
            "angle": self.angle,
            "curvature": self.curvature,
            "comment": self.comment,
        }
