"""Tests for reference model to_dict and __repr__."""

from event_storage.base import keyvalgen
from event_storage.models.reference import (
    AnnotationType,
    Category,
    Criticality,
    MetadataType,
    Method,
    Signal,
    Status,
)


class TestKeyvalgen:
    def test_filters_sqlalchemy_internals(self):
        sig = Signal(
            machine="WEST", experiment_id=1, diagnostic="D", type="IR", path="/p"
        )
        keys = [k for k, _ in keyvalgen(sig)]
        assert not any(k.startswith("_") for k in keys)

    def test_yields_expected_fields(self):
        sig = Signal(
            machine="WEST", experiment_id=1, diagnostic="D", type="IR", path="/p"
        )
        d = dict(keyvalgen(sig))
        assert d["machine"] == "WEST"
        assert d["path"] == "/p"


class TestCustomBaseRepr:
    def test_repr_contains_class_name(self):
        sig = Signal(
            machine="WEST", experiment_id=1, diagnostic="D", type="IR", path="/p"
        )
        assert "Signal(" in repr(sig)

    def test_repr_contains_field_values(self):
        sig = Signal(
            machine="WEST", experiment_id=1, diagnostic="D", type="IR", path="/p"
        )
        assert "machine=WEST" in repr(sig)
        assert "path=/p" in repr(sig)


class TestSignalToDict:
    def test_all_fields_present(self):
        sig = Signal(
            signal_id=1,
            machine="WEST",
            experiment_id=42,
            diagnostic="DIAG",
            type="IR",
            path="/data/42",
        )
        d = sig.to_dict()
        assert d == {
            "signal_id": 1,
            "machine": "WEST",
            "experiment_id": 42,
            "diagnostic": "DIAG",
            "type": "IR",
            "path": "/data/42",
        }


class TestAnnotationTypeToDict:
    def test_all_fields_present(self):
        at = AnnotationType(type_id=1, name="2D", description="bounding boxes")
        d = at.to_dict()
        assert d == {"type_id": 1, "name": "2D", "description": "bounding boxes"}

    def test_none_description(self):
        at = AnnotationType(type_id=2, name="3D", description=None)
        assert at.to_dict()["description"] is None


class TestMetadataTypeToDict:
    def test_all_fields_present(self):
        mt = MetadataType(metadata_type_id=1, name="IR", description="infrared")
        d = mt.to_dict()
        assert d == {"metadata_type_id": 1, "name": "IR", "description": "infrared"}


class TestCriticalityToDict:
    def test_all_fields_present(self):
        c = Criticality(criticality_id=1, level="High", description="high priority")
        d = c.to_dict()
        assert d == {
            "criticality_id": 1,
            "level": "High",
            "description": "high priority",
        }


class TestMethodToDict:
    def test_with_configuration(self):
        m = Method(
            method_id=1,
            name="AutoDetect",
            description="algo",
            configuration={"threshold": 0.5},
        )
        d = m.to_dict()
        assert d["configuration"] == {"threshold": 0.5}
        assert d["name"] == "AutoDetect"

    def test_none_configuration(self):
        m = Method(method_id=2, name="Manual", description=None, configuration=None)
        assert m.to_dict()["configuration"] is None


class TestStatusToDict:
    def test_all_fields_present(self):
        s = Status(status_id=1, name="New", description="newly created")
        d = s.to_dict()
        assert d == {"status_id": 1, "name": "New", "description": "newly created"}


class TestCategoryToDict:
    def test_all_fields_present(self):
        c = Category(category_id=1, name="hotspot", description="localised overheating")
        d = c.to_dict()
        assert d == {
            "category_id": 1,
            "name": "hotspot",
            "description": "localised overheating",
        }
