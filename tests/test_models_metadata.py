"""Tests for Metadata subtype to_dict serialization."""

import pytest

from event_storage.models.metadata import (
    Metadata,
    MetadataIR,
    MetadataPulse,
    MetadataStrikeLine,
)
from event_storage.models.reference import MetadataType


class TestMetadataBaseToDict:
    def test_with_metadata_type_loaded(self):
        mt = MetadataType(metadata_type_id=1, name="IR")
        m = Metadata(metadata_id=1, span_id=2, metadata_type=mt, type=1)
        d = m.to_dict()
        assert d["type"] == "IR"
        assert d["span_id"] == 2

    def test_fallback_to_type_id_when_type_not_loaded(self):
        m = Metadata(metadata_id=1, span_id=2, metadata_type=None, type=5)
        d = m.to_dict()
        assert d["type"] == {"type_id": 5}

    def test_type_none_when_no_type_at_all(self):
        m = Metadata(metadata_id=1, span_id=2, metadata_type=None, type=None)
        d = m.to_dict()
        assert d["type"] is None


class TestMetadataIRToDict:
    def _ir(self):
        return MetadataIR(
            metadata_id=1,
            max_T_K=1200.0,
            max_T_image_position_x=10.0,
            max_T_image_position_y=20.0,
            max_T_world_position_x_m=0.1,
            max_T_world_position_y_m=0.2,
            max_T_world_position_z_m=0.3,
            avg_T_K=800.0,
            min_T_K=400.0,
            min_T_image_position_x=5.0,
            min_T_image_position_y=6.0,
            min_T_world_position_x_m=0.05,
            min_T_world_position_y_m=0.06,
            min_T_world_position_z_m=0.07,
            emissivity=0.9,
            component="divertor",
        )

    def test_type_field_is_ir(self):
        assert self._ir().to_dict()["type"] == "IR"

    def test_temperature_fields(self):
        d = self._ir().to_dict()
        assert d["max_T_K"] == pytest.approx(1200.0)
        assert d["avg_T_K"] == pytest.approx(800.0)
        assert d["min_T_K"] == pytest.approx(400.0)

    def test_world_position_fields(self):
        d = self._ir().to_dict()
        assert d["max_T_world_position_z_m"] == pytest.approx(0.3)
        assert d["min_T_world_position_x_m"] == pytest.approx(0.05)

    def test_component_and_emissivity(self):
        d = self._ir().to_dict()
        assert d["component"] == "divertor"
        assert d["emissivity"] == pytest.approx(0.9)

    def test_all_keys_present(self):
        d = self._ir().to_dict()
        expected_keys = {
            "metadata_id",
            "type",
            "max_T_K",
            "max_T_image_position_x",
            "max_T_image_position_y",
            "max_T_world_position_x_m",
            "max_T_world_position_y_m",
            "max_T_world_position_z_m",
            "avg_T_K",
            "min_T_K",
            "min_T_image_position_x",
            "min_T_image_position_y",
            "min_T_world_position_x_m",
            "min_T_world_position_y_m",
            "min_T_world_position_z_m",
            "component",
            "emissivity",
        }
        assert expected_keys == set(d.keys())


class TestMetadataPulseToDict:
    def _pulse(self):
        return MetadataPulse(
            metadata_id=2,
            max_IP_MA=15.0,
            max_P_LH_MW=3.5,
            max_P_ICRH_MW=2.0,
            max_P_ECRH_MW=1.0,
            max_P_NBI_MW=5.0,
            max_density=8e19,
        )

    def test_type_field(self):
        assert self._pulse().to_dict()["type"] == "Pulse"

    def test_plasma_current(self):
        assert self._pulse().to_dict()["max_IP_MA"] == pytest.approx(15.0)

    def test_heating_systems(self):
        d = self._pulse().to_dict()
        assert d["max_P_LH_MW"] == pytest.approx(3.5)
        assert d["max_P_ICRH_MW"] == pytest.approx(2.0)
        assert d["max_P_ECRH_MW"] == pytest.approx(1.0)
        assert d["max_P_NBI_MW"] == pytest.approx(5.0)

    def test_density(self):
        assert self._pulse().to_dict()["max_density"] == pytest.approx(8e19)

    def test_none_fields_allowed(self):
        p = MetadataPulse(metadata_id=3)
        d = p.to_dict()
        assert d["max_IP_MA"] is None
        assert d["max_P_LH_MW"] is None

    def test_all_keys_present(self):
        d = self._pulse().to_dict()
        assert set(d.keys()) == {
            "metadata_id",
            "type",
            "max_IP_MA",
            "max_density",
            "max_P_LH_MW",
            "max_P_ICRH_MW",
            "max_P_ECRH_MW",
            "max_P_NBI_MW",
        }


class TestMetadataStrikeLineToDict:
    def _sl(self):
        return MetadataStrikeLine(
            metadata_id=3,
            angle=45.0,
            curvature=1.2,
            comment="inner strike line",
        )

    def test_type_field(self):
        assert self._sl().to_dict()["type"] == "StrikeLine"

    def test_geometry_fields(self):
        d = self._sl().to_dict()
        assert d["angle"] == pytest.approx(45.0)
        assert d["curvature"] == pytest.approx(1.2)

    def test_comment(self):
        assert self._sl().to_dict()["comment"] == "inner strike line"

    def test_none_fields_allowed(self):
        sl = MetadataStrikeLine(metadata_id=4)
        d = sl.to_dict()
        assert d["angle"] is None
        assert d["comment"] is None

    def test_segmented_points_not_in_dict(self):
        # segmented_points is a binary blob; to_dict intentionally omits it
        d = self._sl().to_dict()
        assert "segmented_points" not in d

    def test_all_keys_present(self):
        assert set(self._sl().to_dict().keys()) == {
            "metadata_id",
            "type",
            "angle",
            "curvature",
            "comment",
        }
