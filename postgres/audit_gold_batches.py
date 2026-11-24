"""Read-only audit of one incremental Gold batch and PostgreSQL load ledger."""
import json
import os
from pathlib import Path

import pyarrow.dataset as ds
import psycopg

from gold_loader import connection_info


def iter_parquet_files(root):
    """Yield Parquet paths lazily without pathlib.rglob's recursive list growth."""
    for directory, _, filenames in os.walk(root):
        for filename in filenames:
            if filename.endswith(".parquet"):
                yield Path(directory) / filename


def main():
    base = Path("/opt/project-data/gold/hourly_measurements_v4")
    requested_batch = os.getenv("AUDIT_BATCH_ID", "latest")
    if requested_batch == "latest":
        candidates = []
        for path in base.glob("batch_id=*"):
            try:
                candidate_id = int(path.name.split("=", 1)[1])
            except (IndexError, ValueError):
                continue
            if (path / "_SUCCESS").is_file() and next(iter_parquet_files(path), None):
                candidates.append((path.stat().st_mtime_ns, candidate_id))
        if not candidates:
            raise RuntimeError(f"No committed Gold batches found under {base}")
        # Gold IDs encode the source dataset, not chronological order.
        batch_id = max(candidates)[1]
    else:
        batch_id = int(requested_batch)

    batch_path = base / f"batch_id={batch_id:020d}"
    # Support both legacy date-partitioned batches and current flat batches.
    parquet_files = list(iter_parquet_files(batch_path))
    success_marker = (batch_path / "_SUCCESS").is_file()
    if not success_marker or not parquet_files:
        raise RuntimeError(
            f"Gold batch {batch_id} is incomplete: "
            f"success={success_marker}, parquet_files={len(parquet_files)}"
        )

    table = ds.dataset(str(batch_path), format="parquet").to_table(
        columns=["source", "metric", "window_start", "measurement_count"]
    )
    columns = table.to_pydict()
    row_details = [
        {
            "source": row["source"],
            "metric": row["metric"],
            "window_start": str(row["window_start"]),
            "measurement_count": row["measurement_count"],
        }
        for row in table.to_pylist()
    ]
    summary = {
        "batch_id": batch_id,
        "success_marker": success_marker,
        "parquet_files": len(parquet_files),
        "rows": table.num_rows,
        "sources": sorted(set(columns["source"])),
        "metrics": sorted(set(columns["metric"])),
        "first_window": str(min(columns["window_start"])),
        "last_window": str(max(columns["window_start"])),
        "measurement_count": sum(columns["measurement_count"]),
    }

    with psycopg.connect(connection_info()) as conn:
        ledger = conn.execute(
            "SELECT batch_id, row_count, processed_at "
            "FROM processed_gold_batches WHERE batch_id = %s",
            (batch_id,),
        ).fetchone()
        fact = conn.execute(
            "SELECT count(*), coalesce(sum(measurement_count), 0), "
            "max(window_start) FROM fact_hourly_measurements"
        ).fetchone()
        recent_pm25 = conn.execute(
            """
            SELECT count(*), coalesce(sum(f.measurement_count), 0),
                   min(f.window_start), max(f.window_start)
            FROM fact_hourly_measurements f
            JOIN dim_source s ON s.source_key = f.source_key
            JOIN dim_metric m ON m.metric_key = f.metric_key
            WHERE s.source_name = 'openaq'
              AND lower(m.metric_name) IN ('pm25', 'pm2.5')
              AND f.window_start >= now() - interval '7 days'
            """
        ).fetchone()
        source_coverage = conn.execute(
            """
            SELECT s.source_name, count(*), max(f.window_start)
            FROM fact_hourly_measurements f
            JOIN dim_source s ON s.source_key = f.source_key
            GROUP BY s.source_name ORDER BY s.source_name
            """
        ).fetchall()

    print(json.dumps({
        "gold_batch": summary,
        "gold_batch_rows": row_details,
        "postgres_ledger": (
            {"batch_id": ledger[0], "row_count": ledger[1],
             "processed_at": str(ledger[2])} if ledger else None
        ),
        "postgres_fact": {
            "rows": fact[0],
            "measurement_count": fact[1],
            "latest_window": str(fact[2]),
        },
        "openaq_pm25_last_7_days": {
            "fact_rows": recent_pm25[0],
            "measurements": recent_pm25[1],
            "first_window": str(recent_pm25[2]),
            "last_window": str(recent_pm25[3]),
        },
        "source_coverage": [
            {"source": row[0], "fact_rows": row[1], "latest_window": str(row[2])}
            for row in source_coverage
        ],
    }, indent=2, default=str))


if __name__ == "__main__":
    main()
