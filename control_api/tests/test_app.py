import importlib.util, os
from pathlib import Path
os.environ.setdefault("CONTROL_API_TOKEN","test-token")
spec=importlib.util.spec_from_file_location("control_app",Path(__file__).parents[1]/"app.py")
mod=importlib.util.module_from_spec(spec); spec.loader.exec_module(mod)

def test_allowlist_excludes_always_on_sources():
    assert mod.ALLOWED=={"python_iot":"iot-simulator","wokwi":"wokwi-mqtt-bridge"}
    assert not {"openaq","open_meteo_air_quality","open_meteo_weather"} & set(mod.ALLOWED)

def test_unknown_source_rejected():
    import pytest
    with pytest.raises(ValueError): mod.control("openaq","stop")

def test_invalid_action_rejected():
    import pytest
    with pytest.raises(ValueError): mod.control("python_iot","restart")
