from sqlalchemy.orm import DeclarativeBase
from sqlalchemy import MetaData


def keyvalgen(obj):
    """Generate attribute name/value pairs, filtering out SQLAlchemy attributes."""
    excl = ("_sa_adapter", "_sa_instance_state")
    for k, v in vars(obj).items():
        if not k.startswith("_") and not any(hasattr(v, a) for a in excl):
            yield k, v


metadata = MetaData()


class Base(DeclarativeBase):
    metadata = metadata

    def __repr__(self):
        params = ", ".join(f"{k}={v}" for k, v in keyvalgen(self))
        return f"{self.__class__.__name__}({params})"
