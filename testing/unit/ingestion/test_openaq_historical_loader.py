from datetime import datetime

import pytest

from ingestion.load_openaq_historical_silver import (
    build_table,
    deduplicate_records,
    read_jsonl,
    utc_naive,
)


def sample_record(value="18.5"):
    return {
        "record_id": "openaq:123:pm25:2026-11-20T10:00:00Z",
        "schema_version": "1.0",
        "source": "openaq",
        "country_code": "EG",
        "governorate": None,
        "governorate_ar": None,
        "latitude": 30.1,
        "longitude": 31.6,
        "timestamp": "2026-11-20T10:00:00Z",
        "data": {"pollutant": "pm25", "value": value, "unit": "µg/m³"},
        "metadata": {"sensor_id": "123", "ingestion_mode": "historical_backfill"},
    }


def test_read_jsonl_reconstructs_stable_openaq_record_id(tmp_path):
    record = sample_record()
    record.pop("record_id")
    path = tmp_path / "history.jsonl"
    path.write_text(__import__("json").dumps(record), encoding="utf-8")

    loaded = read_jsonl(path)

    assert loaded[0]["record_id"] == (
        "openaq:123:pm25:2026-11-20T10:00:00Z"
    )


def test_utc_naive_normalizes_offset_to_utc():
    assert utc_naive("2026-11-20T12:00:00+02:00") == datetime(
        2026, 11, 20, 10, 0
    )


def test_deduplicate_records_removes_existing_and_repeated_ids():
    record = sample_record()
    records = [record, dict(record), sample_record("19.0")]
    with pytest.raises(ValueError, match="Conflicting historical values"):
        deduplicate_records(records, set())

    result = deduplicate_records([record, dict(record)], set())
    assert result == [record]
    assert deduplicate_records([record], {record["record_id"]}) == []


def test_build_table_matches_silver_contract():
    table = build_table([sample_record()], "historical_backfill_test")

    assert table.num_rows == 1
    row = table.to_pylist()[0]
    assert row["source"] == "openaq"
    assert row["event_time"] == datetime(2026, 11, 20, 10, 0)
    assert dict(row["data"])["pollutant"] == "pm25"
    assert dict(row["data"])["value"] == "18.5"
    assert row["batch_id"] == "historical_backfill_test"
    assert row["topic"] == "openaq-measurements"
    assert row["partition"] is None
    assert row["offset"] is None
