"""CRUD tests for all reference tables."""

from conftest import make_signal

from event_storage import crud


class TestSignalCrud:
    def test_create_and_get(self, db):
        sig = make_signal(db, path="/ref/sig1")
        fetched = crud.reference.get_signal(db, sig.signal_id)
        assert fetched.path == "/ref/sig1"

    def test_get_not_found(self, db):
        assert crud.reference.get_signal(db, 99999) is None

    def test_get_signals_by_experiment(self, db):
        make_signal(db, experiment_id=99, path="/exp/99/a", diagnostic="D1")
        make_signal(db, experiment_id=99, path="/exp/99/b", diagnostic="D2")
        results = crud.reference.get_signals_by_experiment(db, "WEST", 99)
        assert len(results) == 2

    def test_get_signal_by_experiment_and_diagnostic(self, db):
        make_signal(db, experiment_id=77, diagnostic="DIAG77", path="/exp/77")
        sig = crud.reference.get_signal_by_experiment_and_diagnostic(
            db, "WEST", 77, "DIAG77"
        )
        assert sig.diagnostic == "DIAG77"

    def test_update_path(self, db):
        sig = make_signal(db, path="/old")
        updated = crud.reference.update_signal(db, sig.signal_id, path="/new")
        assert updated.path == "/new"

    def test_update_not_found(self, db):
        assert crud.reference.update_signal(db, 99999, path="/x") is None

    def test_delete(self, db):
        sig = make_signal(db, path="/del/sig")
        assert crud.reference.delete_signal(db, sig.signal_id) is True
        assert crud.reference.get_signal(db, sig.signal_id) is None

    def test_delete_not_found(self, db):
        assert crud.reference.delete_signal(db, 99999) is False

    def test_get_or_create_idempotent(self, db):
        s1 = crud.reference.get_or_create_signal(
            db, machine="WEST", experiment_id=1, diagnostic="D", type="IR", path="/idem"
        )
        s2 = crud.reference.get_or_create_signal(
            db, machine="WEST", experiment_id=1, diagnostic="D", type="IR", path="/idem"
        )
        assert s1.signal_id == s2.signal_id


class TestAnnotationTypeCrud:
    def test_create_get_by_name(self, db):
        at = crud.reference.create_annotation_type(db, "2D", "bboxes")
        assert (
            crud.reference.get_annotation_type_by_name(db, "2D").type_id == at.type_id
        )

    def test_get_all(self, db):
        crud.reference.create_annotation_type(db, "TypeA")
        crud.reference.create_annotation_type(db, "TypeB")
        names = {t.name for t in crud.reference.get_annotation_types(db)}
        assert {"TypeA", "TypeB"}.issubset(names)

    def test_get_or_create_idempotent(self, db):
        t1 = crud.reference.get_or_create_annotation_type(db, "Unique2D")
        t2 = crud.reference.get_or_create_annotation_type(db, "Unique2D")
        assert t1.type_id == t2.type_id


class TestMetadataTypeCrud:
    def test_create_and_get(self, db):
        mt = crud.reference.create_metadata_type(db, "IR", "infrared")
        assert (
            crud.reference.get_metadata_type_by_name(db, "IR").metadata_type_id
            == mt.metadata_type_id
        )

    def test_get_or_create_idempotent(self, db):
        m1 = crud.reference.get_or_create_metadata_type(db, "Pulse")
        m2 = crud.reference.get_or_create_metadata_type(db, "Pulse")
        assert m1.metadata_type_id == m2.metadata_type_id


class TestCriticalityCrud:
    def test_create_get_by_level(self, db):
        c = crud.reference.create_criticality(db, "High")
        assert (
            crud.reference.get_criticality_by_level(db, "High").criticality_id
            == c.criticality_id
        )

    def test_get_or_create_idempotent(self, db):
        c1 = crud.reference.get_or_create_criticality(db, "Medium")
        c2 = crud.reference.get_or_create_criticality(db, "Medium")
        assert c1.criticality_id == c2.criticality_id

    def test_get_all(self, db):
        crud.reference.create_criticality(db, "Low")
        crud.reference.create_criticality(db, "Critical")
        levels = {c.level for c in crud.reference.get_criticalities(db)}
        assert {"Low", "Critical"}.issubset(levels)


class TestCategoryCrud:
    def test_create_get_by_name(self, db):
        cat = crud.reference.create_category(db, "hotspot")
        assert (
            crud.reference.get_category_by_name(db, "hotspot").category_id
            == cat.category_id
        )

    def test_get_or_create_idempotent(self, db):
        c1 = crud.reference.get_or_create_category(db, "ufo")
        c2 = crud.reference.get_or_create_category(db, "ufo")
        assert c1.category_id == c2.category_id


class TestMethodCrud:
    def test_create_get_by_name(self, db):
        m = crud.reference.create_method(db, "AutoDetect", is_manual=False)
        assert (
            crud.reference.get_method_by_name(db, "AutoDetect").method_id == m.method_id
        )

    def test_get_method_by_name_and_config_match(self, db):
        cfg = {"threshold": 0.5}
        m = crud.reference.create_method(db, "Configured", configuration=cfg)
        assert (
            crud.reference.get_method_by_name_and_config(
                db, "Configured", cfg
            ).method_id
            == m.method_id
        )

    def test_get_method_by_name_and_config_mismatch(self, db):
        crud.reference.create_method(db, "MethodX", configuration={"a": 1})
        assert (
            crud.reference.get_method_by_name_and_config(db, "MethodX", {"a": 2})
            is None
        )

    def test_get_or_create_distinguishes_config(self, db):
        m1 = crud.reference.get_or_create_method(
            db, "SameMethod", configuration={"v": 1}
        )
        m2 = crud.reference.get_or_create_method(
            db, "SameMethod", configuration={"v": 2}
        )
        assert m1.method_id != m2.method_id

    def test_get_all(self, db):
        crud.reference.create_method(db, "M1")
        crud.reference.create_method(db, "M2")
        names = {m.name for m in crud.reference.get_methods(db)}
        assert {"M1", "M2"}.issubset(names)


class TestStatusCrud:
    def test_create_get_by_name(self, db):
        s = crud.reference.create_status(db, "New")
        assert crud.reference.get_status_by_name(db, "New").status_id == s.status_id

    def test_get_or_create_idempotent(self, db):
        s1 = crud.reference.get_or_create_status(db, "Reviewed")
        s2 = crud.reference.get_or_create_status(db, "Reviewed")
        assert s1.status_id == s2.status_id


class TestInitializeReferenceData:
    def test_idempotent(self, db):
        crud.reference.initialize_reference_data(db)
        crud.reference.initialize_reference_data(db)
        assert len(crud.reference.get_criticalities(db)) == 3
        assert len(crud.reference.get_categories(db)) == 5
        assert len(crud.reference.get_methods(db)) == 3
        assert len(crud.reference.get_statuses(db)) == 3
