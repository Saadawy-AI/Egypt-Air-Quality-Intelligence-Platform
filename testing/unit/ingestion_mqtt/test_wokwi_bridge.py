import importlib.util, os
from pathlib import Path
os.environ["WOKWI_TOPIC_PREFIX"]="iot-test-random-prefix"
spec=importlib.util.spec_from_file_location("wokwi_bridge",Path(__file__).resolve().parents[3]/"ingestion"/"iot"/"wokwi"/"bridge.py")
mod=importlib.util.module_from_spec(spec); spec.loader.exec_module(mod)

def payload():
    return b'{"schema_version":"1.0","device_id":"wokwi-esp32-01","timestamp":"2026-11-25T00:00:00Z","data":{"pm25":12.5,"temperature":25}}'

def test_valid_message_is_labeled_simulated():
    import json
    topic,raw=mod.validate("iot-test-random-prefix/esp32/telemetry",payload())
    item=json.loads(raw)
    assert topic.endswith("/wokwi-esp32-01")
    assert item["source"]=="wokwi_esp32"
    assert item["metadata"]["simulation"]=="true"
    assert item["metadata"]["simulation_provider"]=="wokwi"
    assert item["metadata"]["particulate_input_model"]=="potentiometer_test_inputs_not_physical_pm_sensor"
    assert item["metadata"]["device_id"]=="wokwi-esp32-01"
    assert item["country_code"]=="EG"
    assert item["governorate"]=="Cairo"
    assert item["latitude"]==30.0444
    assert item["longitude"]==31.2357

def test_rejects_wrong_topic_prefix():
    import pytest
    with pytest.raises(ValueError): mod.validate("other/telemetry",payload())

def test_rejects_invalid_sensor_values():
    import pytest
    bad=payload().replace(b'"pm25":12.5',b'"pm25":99999')
    with pytest.raises(ValueError): mod.validate("iot-test-random-prefix/esp32/telemetry",bad)

def test_rejects_invalid_device_id():
    import pytest
    bad=payload().replace(b'wokwi-esp32-01',b'not-wokwi')
    with pytest.raises(ValueError): mod.validate("iot-test-random-prefix/esp32/telemetry",bad)
