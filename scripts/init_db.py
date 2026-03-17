"""Initialize the database with tables and reference data."""

from event_storage.database import engine, SessionLocal
from event_storage.base import Base

# All models must be explicitly imported here so SQLAlchemy
# registers them with the metadata before create_all runs.
# Importing the package __init__ is not sufficient — each module
# must be touched to trigger the class definitions.
import event_storage.models.reference
import event_storage.models.event
import event_storage.models.annotation
import event_storage.models.metadata
import event_storage.models.processing
import event_storage.models.dataset
import event_storage.models.associations  # noqa: F401

from event_storage.crud import reference


def init_database():
    """Create all tables and populate reference data."""
    print("Creating database tables...")
    Base.metadata.create_all(engine)
    print("Tables created successfully!")

    print("\nInitializing reference data...")
    with SessionLocal() as session:
        reference.initialize_reference_data(session)

    print("\nDatabase initialization complete!")


if __name__ == "__main__":
    init_database()
