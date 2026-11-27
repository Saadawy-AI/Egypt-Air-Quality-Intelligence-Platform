import importlib.util, os
from pathlib import Path

os.environ["CONTROL_API_TOKEN"] = "test-token-for-unit-tests"
spec = importlib.util.spec_from_file_location(
    "control_app", Path(__file__).resolve().parents[3] / "control_api" / "app.py"
)
mod = importlib.util.module_from_spec(spec)
spec.loader.exec_module(mod)


def test_allowlist_contains_only_explicitly_managed_services():
    assert mod.ALLOWED == {
        "python_iot": "iot-simulator",
        "wokwi": "wokwi-mqtt-bridge",
        "spark_bronze": "spark-bronze",
        "spark_silver": "spark-silver",
        "spark_gold": "spark-gold",
    }
    assert not {
        "openaq",
        "open_meteo_air_quality",
        "open_meteo_weather",
    } & set(mod.ALLOWED)


def test_unknown_source_rejected():
    import pytest

    with pytest.raises(ValueError):
        mod.control("openaq", "stop")


def test_invalid_action_rejected():
    import pytest

    with pytest.raises(ValueError):
        mod.control("python_iot", "restart")


def _handler(authorization):
    from types import SimpleNamespace

    h = mod.Handler.__new__(mod.Handler)
    h.headers = SimpleNamespace(
        get=lambda name, default=None: authorization if name == "Authorization" else default
    )
    return h


def test_auth_accepts_only_exact_bearer_token():
    assert _handler(f"Bearer {mod.TOKEN}").auth() is True
    assert _handler(f"Bearer {mod.TOKEN}x").auth() is False
    assert _handler("Bearer ").auth() is False
    assert _handler(mod.TOKEN).auth() is False
    assert _handler(None).auth() is False


def test_short_or_empty_token_is_refused_at_startup(monkeypatch):
    import pytest

    for bad in ("", "short-token"):
        monkeypatch.setenv("CONTROL_API_TOKEN", bad)
        spec2 = importlib.util.spec_from_file_location(
            "control_app_bad",
            Path(__file__).resolve().parents[3] / "control_api" / "app.py",
        )
        mod2 = importlib.util.module_from_spec(spec2)
        with pytest.raises(SystemExit):
            spec2.loader.exec_module(mod2)
