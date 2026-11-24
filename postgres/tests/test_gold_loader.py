from contextlib import nullcontext

from postgres import gold_loader


class ExistingBatchConnection:
    def __init__(self):
        self.queries = []

    def transaction(self):
        return nullcontext()

    def execute(self, query, params):
        self.queries.append((query, params))

        class Result:
            @staticmethod
            def fetchone():
                return (params[0],)

        return Result()


def test_processed_batch_is_skipped_before_parquet_read(monkeypatch, tmp_path):
    connection = ExistingBatchConnection()

    def fail_if_read(*args, **kwargs):
        raise AssertionError("Processed Gold batch should not read Parquet again")

    monkeypatch.setattr(gold_loader.ds, "dataset", fail_if_read)

    gold_loader.load_batch(connection, 6, tmp_path)

    assert len(connection.queries) == 1
    assert "SELECT 1 FROM processed_gold_batches" in connection.queries[0][0]
    assert connection.queries[0][1] == (6,)


def test_discover_batches_supports_flat_and_legacy_nested_layouts(
    monkeypatch, tmp_path
):
    complete = tmp_path / "batch_id=00000000000000000003"
    complete.mkdir()
    (complete / "_SUCCESS").touch()
    (complete / "part-00000.parquet").touch()

    missing_success = tmp_path / "batch_id=00000000000000000004"
    missing_success.mkdir()
    (missing_success / "part-00000.parquet").touch()

    nested_only = tmp_path / "batch_id=00000000000000000005"
    nested_only.mkdir()
    (nested_only / "_SUCCESS").touch()
    (nested_only / "nested").mkdir()
    (nested_only / "nested" / "part-00000.parquet").touch()

    monkeypatch.setattr(gold_loader, "GOLD_DATA_DIR", tmp_path)

    discovered = gold_loader.discover_batches()

    assert [(batch_id, path.name) for batch_id, path in discovered] == [
        (3, "batch_id=00000000000000000003"),
        (5, "batch_id=00000000000000000005"),
    ]


def test_discover_batches_does_not_materialize_large_batch_glob(monkeypatch, tmp_path):
    complete = tmp_path / "batch_id=00000000000000000006"
    complete.mkdir()
    (complete / "_SUCCESS").touch()
    (complete / "part-00000.parquet").touch()

    original_glob = gold_loader.Path.glob

    def reject_batch_glob(self, pattern):
        if self == complete and pattern == "*.parquet":
            raise AssertionError("Batch discovery must use lazy os.scandir")
        return original_glob(self, pattern)

    monkeypatch.setattr(gold_loader.Path, "glob", reject_batch_glob)
    monkeypatch.setattr(gold_loader, "GOLD_DATA_DIR", tmp_path)

    assert gold_loader.discover_batches() == [(6, complete)]


class LoaderConnection:
    def __init__(self, batch_id):
        self.batch_id = batch_id

    def transaction(self):
        return nullcontext()

    def execute(self, query, params=()):
        class Result:
            def __init__(self, one=None, many=None):
                self.one = one
                self.many = many

            def fetchone(self):
                return self.one

            def fetchall(self):
                return self.many or []

        if "SELECT 1 FROM processed_gold_batches" in query:
            return Result(one=None)
        if "INSERT INTO processed_gold_batches" in query:
            return Result(one=(self.batch_id,))
        if "SELECT source_name, source_key FROM dim_source" in query:
            return Result(many=[("openaq", 1)])
        if "SELECT governorate_name, governorate_key FROM dim_governorate" in query:
            return Result(many=[("Cairo", 2)])
        if "SELECT metric_name, unit, metric_key FROM dim_metric" in query:
            return Result(many=[("pm25", "ug/m3", 3)])
        if "SELECT date_value, date_key FROM dim_date" in query:
            from datetime import date
            return Result(many=[(date(2026, 11, 23), 20261123)])
        raise AssertionError(f"Unexpected SQL: {query}")


def test_load_batch_uses_coordinate_counts_and_legacy_fallback(monkeypatch, tmp_path):
    from datetime import date, datetime, timezone

    batch_id = 21
    rows = [
        {
            "window_start": datetime(2026, 11, 23, 12, tzinfo=timezone.utc),
            "window_end": datetime(2026, 11, 23, 13, tzinfo=timezone.utc),
            "window_date": date(2026, 11, 23),
            "source": "openaq",
            "governorate": "Cairo",
            "metric": "pm25",
            "unit": "ug/m3",
            "average_value": 12.0,
            "minimum_value": 10.0,
            "maximum_value": 14.0,
            "measurement_count": 5,
            "latitude": 30.0,
            "longitude": 31.0,
            "latitude_count": 2,
            "longitude_count": 4,
        },
        {
            "window_start": datetime(2026, 11, 23, 13, tzinfo=timezone.utc),
            "window_end": datetime(2026, 11, 23, 14, tzinfo=timezone.utc),
            "window_date": date(2026, 11, 23),
            "source": "openaq",
            "governorate": "Cairo",
            "metric": "pm25",
            "unit": "ug/m3",
            "average_value": 15.0,
            "minimum_value": 15.0,
            "maximum_value": 15.0,
            "measurement_count": 3,
            "latitude": 29.0,
            "longitude": 32.0,
        },
    ]

    class Table:
        @staticmethod
        def to_pylist():
            return rows

    class Dataset:
        @staticmethod
        def to_table():
            return Table()

    monkeypatch.setattr(gold_loader.ds, "dataset", lambda *a, **k: Dataset())
    captured = {}

    def capture_execute_many(conn, query, values):
        captured["query"] = query
        captured["values"] = list(values)

    monkeypatch.setattr(gold_loader, "execute_many", capture_execute_many)

    gold_loader.load_batch(LoaderConnection(batch_id), batch_id, tmp_path)

    assert captured["query"] == gold_loader.FACT_UPSERT
    assert len(captured["values"]) == 2
    # FACT_UPSERT tuple positions 12 and 13 are the coordinate denominators.
    assert captured["values"][0][12:14] == (2, 4)
    assert captured["values"][1][12:14] == (3, 3)
