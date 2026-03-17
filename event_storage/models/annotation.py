from typing import TYPE_CHECKING, Optional

import numpy as np
from sqlalchemy import Float, ForeignKey, Index, Integer, LargeBinary, String
from sqlalchemy.orm import Mapped, mapped_column, relationship

from event_storage import Base
from event_storage.models.reference import AnnotationType

if TYPE_CHECKING:
    from event_storage.models.event import EventSpan


class Annotation(Base):
    """Base annotation table with polymorphic inheritance."""

    __tablename__ = "Annotation"
    __table_args__ = {"mysql_engine": "InnoDB"}

    annotation_id: Mapped[int] = mapped_column(
        Integer, primary_key=True, autoincrement=True
    )
    type: Mapped[Optional[int]] = mapped_column(
        Integer, ForeignKey("AnnotationType.type_id")
    )
    span_id: Mapped[Optional[int]] = mapped_column(
        Integer, ForeignKey("EventSpan.span_id", ondelete="CASCADE")
    )
    annotation_subtype: Mapped[Optional[str]] = mapped_column(String(50))

    annotation_type: Mapped[Optional["AnnotationType"]] = relationship(
        back_populates="annotations", cascade="none"
    )
    event_span: Mapped[Optional["EventSpan"]] = relationship(
        back_populates="annotation", passive_deletes=True, cascade="none"
    )

    __mapper_args__ = {
        "polymorphic_identity": "annotation",
        "polymorphic_on": "annotation_subtype",
        "with_polymorphic": "*",
        "passive_deletes": True,
    }

    def to_dict(self) -> dict:
        return {
            "annotation_id": self.annotation_id,
            "type": (
                self.annotation_type.name
                if self.annotation_type is not None
                else ({"type_id": self.type} if self.type else None)
            ),
            "span_id": self.span_id,
        }


Index("idx_annotation_type", Annotation.__table__.c.type)
Index("idx_annotation_span", Annotation.__table__.c.span_id)


class Annotation2D(Annotation):
    """2D annotation with bounding box, polygon, and mask support."""

    __tablename__ = "Annotation2D"
    __table_args__ = {"mysql_engine": "InnoDB"}

    ANNOTATION_TYPE_NAME = "2D"

    annotation_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("Annotation.annotation_id", ondelete="CASCADE"),
        primary_key=True,
    )
    bbox_x1: Mapped[float] = mapped_column(Float, nullable=False)
    bbox_y1: Mapped[float] = mapped_column(Float, nullable=False)
    bbox_x2: Mapped[float] = mapped_column(Float, nullable=False)
    bbox_y2: Mapped[float] = mapped_column(Float, nullable=False)
    width: Mapped[Optional[float]] = mapped_column(Float)
    height: Mapped[Optional[float]] = mapped_column(Float)
    area: Mapped[Optional[float]] = mapped_column(Float)
    centroid_x: Mapped[Optional[float]] = mapped_column(Float)
    centroid_y: Mapped[Optional[float]] = mapped_column(Float)
    polygon_coordinates: Mapped[Optional[bytes]] = mapped_column(LargeBinary)
    mask_data: Mapped[Optional[bytes]] = mapped_column(LargeBinary)

    __mapper_args__ = {"polymorphic_identity": "2d"}

    @classmethod
    def from_bbox(cls, x: float, y: float, w: float, h: float) -> "Annotation2D":
        x1, y1 = float(x), float(y)
        x2, y2 = float(x + w), float(y + h)
        width, height = float(w), float(h)
        return cls(
            annotation_subtype="2d",
            annotation_type=AnnotationType(name=cls.ANNOTATION_TYPE_NAME),
            bbox_x1=x1,
            bbox_y1=y1,
            bbox_x2=x2,
            bbox_y2=y2,
            width=width,
            height=height,
            area=width * height,
            centroid_x=x1 + width / 2,
            centroid_y=y1 + height / 2,
        )

    @classmethod
    def from_polygon(cls, polygon: np.ndarray) -> "Annotation2D":
        if polygon.shape[1] != 2:
            raise ValueError("Polygon must be Nx2 array of (x, y) coordinates")
        x1 = float(np.min(polygon[:, 0]))
        y1 = float(np.min(polygon[:, 1]))
        x2 = float(np.max(polygon[:, 0]))
        y2 = float(np.max(polygon[:, 1]))
        width, height = x2 - x1, y2 - y1
        centroid_x = float(np.mean(polygon[:, 0]))
        centroid_y = float(np.mean(polygon[:, 1]))
        x, y = polygon[:, 0], polygon[:, 1]
        area = float(0.5 * np.abs(np.dot(x, np.roll(y, 1)) - np.dot(y, np.roll(x, 1))))
        return cls(
            annotation_subtype="2d",
            annotation_type=AnnotationType(name=cls.ANNOTATION_TYPE_NAME),
            bbox_x1=x1,
            bbox_y1=y1,
            bbox_x2=x2,
            bbox_y2=y2,
            width=width,
            height=height,
            area=area,
            centroid_x=centroid_x,
            centroid_y=centroid_y,
            polygon_coordinates=polygon.astype(np.float32).tobytes(),
        )

    @classmethod
    def from_mask(cls, mask: np.ndarray) -> "Annotation2D":
        if mask.ndim != 2:
            raise ValueError("Mask must be a 2D array")
        mask_bool = mask.astype(bool)
        rows, cols = np.where(mask_bool)
        if len(rows) == 0:
            raise ValueError("Mask is empty (no True/1 pixels)")
        x1, y1 = float(np.min(cols)), float(np.min(rows))
        x2, y2 = float(np.max(cols)), float(np.max(rows))
        width, height = x2 - x1 + 1, y2 - y1 + 1
        return cls(
            annotation_subtype="2d",
            annotation_type=AnnotationType(name=cls.ANNOTATION_TYPE_NAME),
            bbox_x1=x1,
            bbox_y1=y1,
            bbox_x2=x2,
            bbox_y2=y2,
            width=width,
            height=height,
            area=float(np.sum(mask_bool)),
            centroid_x=float(np.mean(cols)),
            centroid_y=float(np.mean(rows)),
            mask_data=mask.astype(np.uint8).tobytes(),
        )

    def get_polygon(self) -> Optional[np.ndarray]:
        if self.polygon_coordinates is None:
            return None
        return np.frombuffer(self.polygon_coordinates, dtype=np.float32).reshape(-1, 2)

    def get_mask(self, height: int, width: int) -> Optional[np.ndarray]:
        if self.mask_data is None:
            return None
        return np.frombuffer(self.mask_data, dtype=np.uint8).reshape(height, width)

    def to_dict(self) -> dict:
        data = {
            "annotation_id": self.annotation_id,
            "type": self.annotation_type.name if self.annotation_type else None,
            "bbox_x1": self.bbox_x1,
            "bbox_y1": self.bbox_y1,
            "bbox_x2": self.bbox_x2,
            "bbox_y2": self.bbox_y2,
            "width": self.width,
            "height": self.height,
            "area": self.area,
            "centroid_x": self.centroid_x,
            "centroid_y": self.centroid_y,
        }
        if self.polygon_coordinates:
            data["polygon"] = (
                np.frombuffer(self.polygon_coordinates, dtype=np.float32)
                .reshape(-1, 2)
                .tolist()
            )
        return data


class Annotation3D(Annotation):
    """3D annotation with volume and mesh support."""

    __tablename__ = "Annotation3D"
    __table_args__ = {"mysql_engine": "InnoDB"}

    ANNOTATION_TYPE_NAME = "3D"

    annotation_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("Annotation.annotation_id", ondelete="CASCADE"),
        primary_key=True,
    )
    bbox_x1: Mapped[float] = mapped_column(Float, nullable=False)
    bbox_y1: Mapped[float] = mapped_column(Float, nullable=False)
    bbox_z1: Mapped[float] = mapped_column(Float, nullable=False)
    bbox_x2: Mapped[float] = mapped_column(Float, nullable=False)
    bbox_y2: Mapped[float] = mapped_column(Float, nullable=False)
    bbox_z2: Mapped[float] = mapped_column(Float, nullable=False)
    width: Mapped[Optional[float]] = mapped_column(Float)
    height: Mapped[Optional[float]] = mapped_column(Float)
    depth: Mapped[Optional[float]] = mapped_column(Float)
    area: Mapped[Optional[float]] = mapped_column(Float)
    volume: Mapped[Optional[float]] = mapped_column(Float)
    centroid_x: Mapped[Optional[float]] = mapped_column(Float)
    centroid_y: Mapped[Optional[float]] = mapped_column(Float)
    centroid_z: Mapped[Optional[float]] = mapped_column(Float)
    polygon_coordinates: Mapped[Optional[bytes]] = mapped_column(LargeBinary)

    __mapper_args__ = {"polymorphic_identity": "3d"}

    @classmethod
    def from_bbox(
        cls, x: float, y: float, z: float, w: float, h: float, d: float
    ) -> "Annotation3D":
        x1, y1, z1 = float(x), float(y), float(z)
        x2, y2, z2 = float(x + w), float(y + h), float(z + d)
        width, height, depth = float(w), float(h), float(d)
        return cls(
            annotation_subtype="3d",
            annotation_type=AnnotationType(name=cls.ANNOTATION_TYPE_NAME),
            bbox_x1=x1,
            bbox_y1=y1,
            bbox_z1=z1,
            bbox_x2=x2,
            bbox_y2=y2,
            bbox_z2=z2,
            width=width,
            height=height,
            depth=depth,
            area=width * height,
            volume=width * height * depth,
            centroid_x=x1 + width / 2,
            centroid_y=y1 + height / 2,
            centroid_z=z1 + depth / 2,
        )

    def to_dict(self) -> dict:
        return {
            "annotation_id": self.annotation_id,
            "type": self.annotation_type.name if self.annotation_type else None,
            "bbox_x1": self.bbox_x1,
            "bbox_y1": self.bbox_y1,
            "bbox_z1": self.bbox_z1,
            "bbox_x2": self.bbox_x2,
            "bbox_y2": self.bbox_y2,
            "bbox_z2": self.bbox_z2,
            "width": self.width,
            "height": self.height,
            "depth": self.depth,
            "area": self.area,
            "volume": self.volume,
            "centroid_x": self.centroid_x,
            "centroid_y": self.centroid_y,
            "centroid_z": self.centroid_z,
        }
