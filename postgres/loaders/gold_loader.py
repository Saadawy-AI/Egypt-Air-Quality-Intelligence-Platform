import gzip
import json
import logging
import os
import time
from datetime import date, datetime, timezone
from pathlib import Path

import pyarrow.dataset as ds
import psycopg
from psycopg.conninfo import make_conninfo


logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger("GoldPostgresLoader")

GOLD_DATA_DIR = Path(
    os.getenv("GOLD_DATA_DIR", "/opt/project-data/gold/hourly_measurements_v4")
)
SCHEMA_PATH = Path(os.getenv("POSTGRES_SCHEMA_PATH", "/app/schema.sql"))
POLL_SECONDS = max(10, int(os.getenv("POLL_SECONDS", "60")))
GOVERNORATE_GEOJSON = Path(os.getenv(
    "GOVERNORATE_GEOJSON",
    "/opt/project-data/reference/egypt_governorates_capmas.geojson.gz",
))


def _point_in_ring(longitude, latitude, ring):
    inside = False
    for index, (x1, y1) in enumerate(ring):
        x2, y2 = ring[(index + 1) % len(ring)]
        cross = (x2 - x1) * (latitude - y1) - (longitude - x1) * (y2 - y1)
        if abs(cross) < 1e-10 and min(x1, x2) - 1e-10 <= longitude <= max(x1, x2) + 1e-10 and min(y1, y2) - 1e-10 <= latitude <= max(y1, y2) + 1e-10:
            return True
        if (y1 > latitude) != (y2 > latitude):
            intersection = (x2 - x1) * (latitude - y1) / (y2 - y1) + x1
            if longitude < intersection:
                inside = not inside
    return inside


def _point_in_polygon(longitude, latitude, rings):
    return _point_in_ring(longitude, latitude, rings[0]) and not any(
        _point_in_ring(longitude, latitude, hole) for hole in rings[1:]
    )


def load_governorate_polygons(path=GOVERNORATE_GEOJSON):
    if not path.is_file():
        logger.warning("CAPMAS governorate boundaries not found; unknown locations remain Unknown | path=%s", path)
        return []
    opener = gzip.open if path.suffix == ".gz" else open
    with opener(path, "rt", encoding="utf-8") as source:
        payload = json.load(source)
    polygons = []
    aliases = {"North Sinia": "North Sinai", "South Sinia": "South Sinai"}
    for feature in payload.get("features", []):
        name = (feature.get("properties", {}).get("Gov_name") or "").strip()
        name = aliases.get(name, name)
        geometry = feature.get("geometry") or {}
        if geometry.get("type") in ("Polygon", "MultiPolygon") and name:
            polygons.append((name, geometry["type"], geometry["coordinates"]))
    return polygons


_GOVERNORATE_POLYGONS = None


def resolve_governorate(latitude, longitude):
    global _GOVERNORATE_POLYGONS
    if latitude is None or longitude is None:
        logger.debug("Governorate lookup skipped: missing coordinates.")
        return None
    try:
        latitude, longitude = float(latitude), float(longitude)
    except (TypeError, ValueError):
        logger.warning("Governorate lookup received invalid coordinates: lat=%r lon=%r", latitude, longitude)
        return None
    if not (-90 <= latitude <= 90 and -180 <= longitude <= 180):
        logger.warning("Governorate lookup received out-of-range coordinates: lat=%s lon=%s", latitude, longitude)
        return None
    if _GOVERNORATE_POLYGONS is None:
        _GOVERNORATE_POLYGONS = load_governorate_polygons()
    matches = []
    for name, geometry_type, coordinates in _GOVERNORATE_POLYGONS:
        if geometry_type == "Polygon":
            matched = _point_in_polygon(longitude, latitude, coordinates)
        else:
            matched = any(_point_in_polygon(longitude, latitude, polygon) for polygon in coordinates)
        if matched:
            matches.append(name)
    if len(matches) != 1:
        logger.warning(
            "Governorate lookup did not resolve uniquely | lat=%s | lon=%s | matches=%s",
            latitude, longitude, matches,
        )
        return None
    return matches[0]


def effective_governorate(row):
    current = row.get("governorate")
    if current and current != "Unknown":
        return current
    if row.get("source") != "openaq":
        return current or "Unknown"
    return resolve_governorate(row.get("latitude"), row.get("longitude")) or "Unknown"

FACT_UPSERT = """
INSERT INTO fact_hourly_measurements (
    window_start, window_end, date_key, source_key, governorate_key,
    metric_key, average_value, minimum_value, maximum_value,
    measurement_count, latitude, longitude, latitude_count, longitude_count
)
VALUES (
    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
)
ON CONFLICT (window_start, source_key, governorate_key, metric_key)
DO UPDATE SET
    window_end = EXCLUDED.window_end,
    date_key = EXCLUDED.date_key,
    average_value = (
        fact_hourly_measurements.average_value
            * fact_hourly_measurements.measurement_count
        + EXCLUDED.average_value * EXCLUDED.measurement_count
    ) / (
        fact_hourly_measurements.measurement_count + EXCLUDED.measurement_count
    ),
    minimum_value = LEAST(
        fact_hourly_measurements.minimum_value, EXCLUDED.minimum_value
    ),
    maximum_value = GREATEST(
        fact_hourly_measurements.maximum_value, EXCLUDED.maximum_value
    ),
    latitude = CASE
        WHEN fact_hourly_measurements.latitude_count
             + EXCLUDED.latitude_count = 0 THEN NULL
        ELSE (
            COALESCE(fact_hourly_measurements.latitude, 0)
                * fact_hourly_measurements.latitude_count
            + COALESCE(EXCLUDED.latitude, 0) * EXCLUDED.latitude_count
        ) / (
            fact_hourly_measurements.latitude_count + EXCLUDED.latitude_count
        )
    END,
    longitude = CASE
        WHEN fact_hourly_measurements.longitude_count
             + EXCLUDED.longitude_count = 0 THEN NULL
        ELSE (
            COALESCE(fact_hourly_measurements.longitude, 0)
                * fact_hourly_measurements.longitude_count
            + COALESCE(EXCLUDED.longitude, 0) * EXCLUDED.longitude_count
        ) / (
            fact_hourly_measurements.longitude_count + EXCLUDED.longitude_count
        )
    END,
    measurement_count = (
        fact_hourly_measurements.measurement_count + EXCLUDED.measurement_count
    ),
    latitude_count = (
        fact_hourly_measurements.latitude_count + EXCLUDED.latitude_count
    ),
    longitude_count = (
        fact_hourly_measurements.longitude_count + EXCLUDED.longitude_count
    ),
    updated_at = now()
"""


def connection_info():
    required = ("PGHOST", "PGDATABASE", "PGUSER", "PGPASSWORD")
    missing = [key for key in required if not os.getenv(key)]
    if missing:
        raise RuntimeError(
            "Missing required PostgreSQL settings: " + ", ".join(missing)
        )
    return make_conninfo(
        host=os.environ["PGHOST"],
        port=os.getenv("PGPORT", "5432"),
        dbname=os.environ["PGDATABASE"],
        user=os.environ["PGUSER"],
        password=os.environ["PGPASSWORD"],
        connect_timeout=10,
        application_name="egypt-air-quality-gold-loader",
    )


def ensure_schema(conn):
    schema_sql = SCHEMA_PATH.read_text(encoding="utf-8")
    with conn.transaction():
        # Execute the complete parameter-free SQL script; splitting on semicolons
        # is unsafe when literals or procedural blocks contain semicolons.
        conn.execute(schema_sql, prepare=False)
        conn.execute("SET TIME ZONE 'UTC'")
    logger.info("PostgreSQL serving schema is ready.")


def discover_batches():
    if not GOLD_DATA_DIR.is_dir():
        return []

    batches = []
    # Avoid Path.glob()/list(scandir) here: the root contains many batch
    # directories and pathlib may materialize the full directory listing.
    with os.scandir(GOLD_DATA_DIR) as root_entries:
        for root_entry in root_entries:
            if not root_entry.name.startswith("batch_id="):
                continue
            if not root_entry.is_dir(follow_symlinks=False):
                continue
            try:
                batch_id = int(root_entry.name.split("=", 1)[1])
            except (IndexError, ValueError):
                continue

            path = Path(root_entry.path)
            if not (path / "_SUCCESS").is_file():
                continue

            # Traverse partition directories lazily and stop at the first
            # Parquet file; retain only the small set of directory paths.
            pending_directories = [path]
            has_parquet = False
            while pending_directories and not has_parquet:
                current_directory = pending_directories.pop()
                with os.scandir(current_directory) as entries:
                    for entry in entries:
                        if entry.is_file(follow_symlinks=False):
                            if entry.name.endswith(".parquet"):
                                has_parquet = True
                                break
                        elif entry.is_dir(follow_symlinks=False):
                            pending_directories.append(Path(entry.path))
            if has_parquet:
                batches.append((batch_id, path))

    return sorted(batches, key=lambda item: item[0])


def as_utc(value):
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def date_attributes(value):
    if isinstance(value, datetime):
        value = value.date()
    elif isinstance(value, str):
        value = date.fromisoformat(value)

    date_key = value.year * 10000 + value.month * 100 + value.day
    return (
        date_key,
        value,
        value.year,
        (value.month - 1) // 3 + 1,
        value.month,
        value.day,
        value.isoweekday(),
    )


def execute_many(conn, query, values):
    with conn.cursor() as cursor:
        cursor.executemany(query, values)


def load_batch(conn, batch_id, batch_path):
    # Fast path for the common poll loop. The unique ledger insert below remains
    # the authoritative concurrency guard.
    with conn.transaction():
        already_loaded = conn.execute(
            "SELECT 1 FROM processed_gold_batches WHERE batch_id = %s",
            (batch_id,),
        ).fetchone()
    if already_loaded is not None:
        logger.debug("Gold batch already loaded; skipping file read | batch=%s", batch_id)
        return

    dataset = ds.dataset(
        str(batch_path),
        format="parquet",
        partitioning="hive",
    )
    row_count = dataset.count_rows()
    if row_count == 0:
        logger.info("Skipped empty Gold batch | batch=%s", batch_id)
        return

    batch_size = max(256, int(os.getenv("GOLD_LOADER_BATCH_SIZE", "4096")))
    loaded_rows = 0

    # The ledger and every fact upsert share one transaction. If any Arrow
    # chunk fails, both the facts and the ledger claim roll back together.
    with conn.transaction():
        inserted = conn.execute(
            """
            INSERT INTO processed_gold_batches (batch_id, row_count, source_path)
            VALUES (%s, %s, %s)
            ON CONFLICT (batch_id) DO NOTHING
            RETURNING batch_id
            """,
            (batch_id, row_count, str(batch_path)),
        ).fetchone()
        if inserted is None:
            logger.debug("Gold batch already loaded by another loader | batch=%s", batch_id)
            return

        for arrow_batch in dataset.to_batches(batch_size=batch_size):
            rows = arrow_batch.to_pylist()
            if not rows:
                continue

            sources = sorted({row["source"] for row in rows if row.get("source")})
            # Stations repeat heavily inside a batch; memoize the polygon lookup
            # so each distinct (source, governorate, lat, lon) is resolved once.
            governorate_memo = {}
            resolved_governorates = []
            for row in rows:
                memo_key = (row.get("source"), row.get("governorate"), row.get("latitude"), row.get("longitude"))
                if memo_key not in governorate_memo:
                    governorate_memo[memo_key] = effective_governorate(row)
                resolved_governorates.append(governorate_memo[memo_key])
            governorates = sorted(set(resolved_governorates))
            metrics = sorted({
                (row["metric"], row.get("unit") or "")
                for row in rows if row.get("metric")
            })
            date_values = sorted({
                row["window_date"].date() if isinstance(row["window_date"], datetime)
                else date.fromisoformat(row["window_date"]) if isinstance(row["window_date"], str)
                else row["window_date"]
                for row in rows if row.get("window_date") is not None
            })
            date_rows = sorted({
                date_attributes(value) for value in date_values
            })

            execute_many(conn,
                """
                INSERT INTO dim_source (source_name) VALUES (%s)
                ON CONFLICT (source_name) DO NOTHING
                """,
                [(value,) for value in sources],
            )
            execute_many(conn,
                """
                INSERT INTO dim_governorate (governorate_name) VALUES (%s)
                ON CONFLICT (governorate_name) DO NOTHING
                """,
                [(value,) for value in governorates],
            )
            execute_many(conn,
                """
                INSERT INTO dim_metric (metric_name, unit) VALUES (%s, %s)
                ON CONFLICT (metric_name, unit) DO NOTHING
                """,
                metrics,
            )
            execute_many(conn,
                """
                INSERT INTO dim_date (
                    date_key, date_value, year, quarter, month, day, day_of_week
                ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (date_key) DO NOTHING
                """,
                date_rows,
            )

            source_keys = dict(conn.execute(
                "SELECT source_name, source_key FROM dim_source WHERE source_name = ANY(%s)",
                (sources,),
            ).fetchall())
            governorate_keys = dict(conn.execute(
                "SELECT governorate_name, governorate_key FROM dim_governorate WHERE governorate_name = ANY(%s)",
                (governorates,),
            ).fetchall())
            metric_names = sorted({name for name, _ in metrics})
            metric_keys = {
                (name, unit): key
                for name, unit, key in conn.execute(
                    "SELECT metric_name, unit, metric_key FROM dim_metric WHERE metric_name = ANY(%s)",
                    (metric_names,),
                ).fetchall()
            }
            date_keys = dict(conn.execute(
                "SELECT date_value, date_key FROM dim_date WHERE date_value = ANY(%s)",
                (date_values,),
            ).fetchall())

            fact_rows = []
            for row, governorate in zip(rows, resolved_governorates):
                source = row.get("source")
                metric = row.get("metric")
                if not source or not metric:
                    raise ValueError(
                        f"Gold batch {batch_id} contains a row without source/metric."
                    )
                unit = row.get("unit") or ""
                date_value = row["window_date"]
                if isinstance(date_value, datetime):
                    date_value = date_value.date()
                elif isinstance(date_value, str):
                    date_value = date.fromisoformat(date_value)

                measurement_count = int(row["measurement_count"])
                if measurement_count <= 0:
                    raise ValueError(
                        f"Gold batch {batch_id} contains a non-positive measurement_count."
                    )
                latitude = row.get("latitude")
                longitude = row.get("longitude")
                fact_rows.append((
                    as_utc(row["window_start"]),
                    as_utc(row["window_end"]),
                    date_keys[date_value],
                    source_keys[source],
                    governorate_keys[governorate],
                    metric_keys[(metric, unit)],
                    float(row["average_value"]),
                    float(row["minimum_value"]),
                    float(row["maximum_value"]),
                    measurement_count,
                    float(latitude) if latitude is not None else None,
                    float(longitude) if longitude is not None else None,
                    int(row["latitude_count"]) if row.get("latitude_count") is not None else (measurement_count if latitude is not None else 0),
                    int(row["longitude_count"]) if row.get("longitude_count") is not None else (measurement_count if longitude is not None else 0),
                ))

            execute_many(conn, FACT_UPSERT, fact_rows)
            loaded_rows += len(fact_rows)

        if loaded_rows != row_count:
            raise RuntimeError(
                f"Gold batch {batch_id} row-count mismatch: scanned={row_count}, loaded={loaded_rows}."
            )

    logger.info(
        "Loaded Gold batch into PostgreSQL | batch=%s | rows=%s | arrow_batch_size=%s",
        batch_id, loaded_rows, batch_size,
    )


def run():
    conninfo = connection_info()
    while True:
        try:
            with psycopg.connect(conninfo) as conn:
                ensure_schema(conn)
                while True:
                    for batch_id, batch_path in discover_batches():
                        load_batch(conn, batch_id, batch_path)
                    time.sleep(POLL_SECONDS)
        except Exception:
            logger.exception("PostgreSQL Gold loader cycle failed; retrying.")
            time.sleep(15)


if __name__ == "__main__":
    run()
