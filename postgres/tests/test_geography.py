from pathlib import Path

from postgres.gold_loader import effective_governorate, load_governorate_polygons, resolve_governorate

BOUNDARIES = Path(__file__).resolve().parents[2] / "data" / "reference" / "egypt_governorates_capmas.geojson"


def test_capmas_boundaries_contain_all_27_governorates():
    polygons = load_governorate_polygons(BOUNDARIES)
    assert len(polygons) == 27
    assert {item[0] for item in polygons} >= {"Cairo", "Alexandria", "Minya", "North Sinai", "South Sinai"}


def test_point_in_polygon_resolves_verified_openaq_coordinates(monkeypatch):
    import postgres.gold_loader as loader
    monkeypatch.setattr(loader, "_GOVERNORATE_POLYGONS", load_governorate_polygons(BOUNDARIES))
    assert resolve_governorate(30.099407670574287, 31.63850168272893) == "Cairo"
    assert resolve_governorate(30.04147, 31.23289) == "Cairo"
    assert resolve_governorate(31.23378, 30.04102) == "Alexandria"


def test_unknown_is_resolved_only_for_openaq(monkeypatch):
    import postgres.gold_loader as loader
    monkeypatch.setattr(loader, "_GOVERNORATE_POLYGONS", load_governorate_polygons(BOUNDARIES))
    assert effective_governorate({"source": "openaq", "governorate": None, "latitude": 30.0994, "longitude": 31.6385}) == "Cairo"
    assert effective_governorate({"source": "open_meteo", "governorate": None, "latitude": 30.0994, "longitude": 31.6385}) == "Unknown"
    assert effective_governorate({"source": "openaq", "governorate": "Giza", "latitude": 30.0994, "longitude": 31.6385}) == "Giza"


def test_outside_egypt_or_missing_coordinates_remain_unresolved(monkeypatch):
    import postgres.gold_loader as loader
    monkeypatch.setattr(loader, "_GOVERNORATE_POLYGONS", load_governorate_polygons(BOUNDARIES))
    assert resolve_governorate(None, 31.2) is None
    assert resolve_governorate(0, 0) is None
