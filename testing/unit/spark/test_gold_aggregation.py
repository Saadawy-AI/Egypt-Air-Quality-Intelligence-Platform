"""Unit tests for Gold aggregation helpers that do not need a Spark runtime.

pyspark is replaced by stubs so the module can be imported in the lightweight
CI environment (pyspark is not a project test dependency).
"""
import importlib.util
import sys
from pathlib import Path
from unittest.mock import MagicMock

import pytest

SPARK_DIR = Path(__file__).resolve().parents[3] / "spark"


@pytest.fixture
def gold(monkeypatch):
    for name in ("pyspark", "pyspark.sql", "pyspark.sql.functions", "pyspark.sql.types"):
        monkeypatch.setitem(sys.modules, name, MagicMock())
    monkeypatch.syspath_prepend(str(SPARK_DIR))
    spec = importlib.util.spec_from_file_location("gold_aggregation_under_test", SPARK_DIR / "gold" / "aggregation.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_wait_returns_true_immediately_when_marker_exists(gold, tmp_path):
    (tmp_path / "_SUCCESS").write_text("")
    assert gold.wait_for_success_marker(str(tmp_path), timeout=0) is True


def test_wait_returns_false_after_timeout(gold, tmp_path):
    assert gold.wait_for_success_marker(str(tmp_path), timeout=0) is False


def test_wait_sees_marker_that_appears_later(gold, tmp_path, monkeypatch):
    def create_marker(_seconds):
        (tmp_path / "_SUCCESS").write_text("")

    monkeypatch.setattr(gold.time, "sleep", create_marker)
    assert gold.wait_for_success_marker(str(tmp_path), timeout=60, interval=1) is True


def test_notification_for_unknown_dataset_is_rejected(gold):
    event = {"dataset": "nope", "silver_batch_id": 1, "row_count": 1, "silver_path": "x"}
    with pytest.raises(ValueError):
        gold.process_notification(event, 0, MagicMock())


def test_notification_with_mismatched_path_is_rejected(gold):
    event = {"dataset": "weather", "silver_batch_id": 5, "row_count": 3, "silver_path": "/elsewhere"}
    with pytest.raises(ValueError):
        gold.process_notification(event, 0, MagicMock())


def test_dead_letter_file_is_written_atomically(gold, tmp_path, monkeypatch):
    monkeypatch.setattr(gold, "GOLD_DLQ_PATH", str(tmp_path))
    gold.write_gold_dead_letter({"dataset": "weather", "silver_batch_id": 7}, 3, RuntimeError("boom"))
    files = list(tmp_path.iterdir())
    assert [f.name for f in files] == ["00000000000000000003_weather_7.json"]
