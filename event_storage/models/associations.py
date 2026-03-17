from sqlalchemy import Column, Integer, ForeignKey, Table
from event_storage import Base

dataset_span_association = Table(
    "DatasetSpan",
    Base.metadata,
    Column("dataset_id", Integer, ForeignKey("Dataset.dataset_id"), primary_key=True),
    Column(
        "span_id",
        Integer,
        ForeignKey("EventSpan.span_id", ondelete="CASCADE"),
        primary_key=True,
    ),
)
