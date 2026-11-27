from datetime import datetime, timezone

import pyarrow.dataset as ds
import pytest

from ingestion.build_bronze import (
    TOPICS,
    _bronze_row,
    write_bronze,
)


def test_write_bronze_writes_three_topic_parquet_layers(tmp_path):
    ingested_at = datetime(2026, 11, 17, tzinfo=timezone.utc)

    rows = []
    payloads = [
        ("open_meteo_air_quality", {"source": "open_meteo_air_quality", "governorate": "Cairo", "data": {"hourly": {"pm2_5": [10]}}}),
        ("open_meteo_weather", {"source": "open_meteo_weather", "governorate": "Cairo", "data": {"hourly": {"temperature_2m": [25]}}}),
        ("openaq", {"source": "openaq", "location": {"id": 1}, "sensor": {"id": 2}, "measurement": {"value": 20}}),
    ]

    for offset, (source, payload) in enumerate(payloads):
        rows.append(
            _bronze_row(
                source,
                TOPICS[source],
                payload,
                offset,
                ingested_at,
            )
        )

    written = write_bronze(rows, tmp_path)

    assert written == 3
    for topic in TOPICS.values():
        topic_dir = tmp_path / topic
        assert topic_dir.exists()
        table = ds.dataset(topic_dir, format="parquet", partitioning="hive").to_table()
        assert table.num_rows == 1


def test_write_bronze_skips_duplicate_raw_values(tmp_path):
    ingested_at = datetime(2026, 11, 17, tzinfo=timezone.utc)
    payload = {
        "source": "open_meteo_weather",
        "governorate": "Cairo",
        "data": {"hourly": {"temperature_2m": [25]}},
    }
    row = _bronze_row(
        "open_meteo_weather",
        TOPICS["open_meteo_weather"],
        payload,
        0,
        ingested_at,
    )

    assert write_bronze([row], tmp_path) == 1
    assert write_bronze([row], tmp_path) == 0


def test_write_bronze_reports_unreadable_existing_dataset(tmp_path, monkeypatch):
    ingested_at = datetime(2026, 11, 17, tzinfo=timezone.utc)
    topic_dir = tmp_path / TOPICS["open_meteo_weather"]
    topic_dir.mkdir()
    (topic_dir / "broken.parquet").touch()
    payload = {
        "source": "open_meteo_weather",
        "governorate": "Cairo",
        "data": {"hourly": {"temperature_2m": [25]}},
    }
    row = _bronze_row(
        "open_meteo_weather",
        TOPICS["open_meteo_weather"],
        payload,
        0,
        ingested_at,
    )

    def fail_to_read(*args, **kwargs):
        raise OSError("corrupt parquet footer")

    monkeypatch.setattr(ds, "dataset", fail_to_read)

    with pytest.raises(RuntimeError) as error:
        write_bronze([row], tmp_path)

    assert str(topic_dir) in str(error.value)
